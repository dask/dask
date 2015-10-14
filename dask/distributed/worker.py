from __future__ import print_function

import uuid
import random
import socket
import sys
import os
import traceback
from threading import Thread, Lock, Event
from multiprocessing.pool import ThreadPool
from contextlib import contextmanager
from datetime import datetime
from time import time
from collections import defaultdict

try:
    import cPickle as pickle
except ImportError:
    import pickle

import zmq

from ..compatibility import Queue, unicode
from .. import core


def pickle_dumps(obj):
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

MAX_DEALERS = 100

with open('log.workers', 'w') as f:  # delete file
    pass


def log(*args):
    with open('log.workers', 'a') as f:
        print('\n', *args, file=f)


log('Hello from worker.py')

@contextmanager
def logerrors():
    try:
        yield
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = ''.join(traceback.format_tb(exc_traceback))
        log('Error!', str(e))
        log('Traceback', str(tb))
        raise


class Worker(object):
    """ Asynchronous worker in a distributed dask computation pool


    Parameters
    ----------

    scheduler: string
        Address of scheduler
    hostname: string
        A visible hostname/IP of this worker to the network
    port_to_workers: int
        Port on which to listen to worker connections
    bind_to_workers: string
        Addresses from which we accept worker connections, defaults to *
    heartbeat: int, bool
        The time between heartbeats in seconds, or False to turn off
        heartbeats, defaults to 5

    State
    -----

    status: string
        Status of worker, either 'run' or 'closed'
    to_workers: zmq.Socket
        Router socket to serve requests from other workers
    to_scheduler: zmq.Socket
        Dealer socket to communicate with scheduler

    See Also
    --------

        dask.distributed.scheduler.Scheduler
    """
    def __init__(self, scheduler, data=None, nthreads=100,
                 hostname=None, port_to_workers=None, bind_to_workers='*',
                 block=False, heartbeat=5):
        if isinstance(scheduler, unicode):
            scheduler = scheduler.encode()
        self.data = data if data is not None else dict()
        self.pool = ThreadPool(nthreads)
        self.scheduler = scheduler
        self.heartbeat = heartbeat
        self.status = 'run'
        self.context = zmq.Context()

        self.hostname = hostname or socket.gethostname()

        self.to_workers = self.context.socket(zmq.ROUTER)
        if port_to_workers is None:
            port_to_workers = self.to_workers.bind_to_random_port('tcp://' + bind_to_workers)
        else:
            self.to_workers.bind('tcp://%s:%d' % (bind_to_workers, port_to_workers))
        self.address = ('tcp://%s:%s' % (self.hostname, port_to_workers)).encode()

        self.dealers = dict()

        self.lock = Lock()

        self.queues = dict()
        self.queues_by_worker = defaultdict(lambda: defaultdict(set))

        self.pid = os.getpid()

        self.to_scheduler = self.context.socket(zmq.DEALER)

        self.to_scheduler.setsockopt(zmq.IDENTITY, self.address)
        self.to_scheduler.connect(scheduler)

        self.immediate_functions = {'close': self.close_from_scheduler}

        self.scheduler_functions = {'status': self.status_to_scheduler,
                                    'compute': self.compute,
                                    'getitem': self.getitem_scheduler,
                                    'delitem': self.delitem,
                                    'setitem': self.setitem,
                                    'worker-death': self.worker_death}

        self.worker_functions = {'getitem': self.getitem_worker,
                                 'getitem-ack': self.getitem_ack,
                                 'status': self.status_to_worker}

        log(self.address, 'Start up', self.scheduler)

        self._listen_scheduler_thread = Thread(target=self.listen_to_scheduler)
        self._listen_scheduler_thread.start()
        self._listen_workers_thread = Thread(target=self.listen_to_workers)
        self._listen_workers_thread.start()
        if self.heartbeat:
            self._heartbeat_thread = Thread(target=self.heart,
                                            kwargs={'pulse': heartbeat})
            self._heartbeat_thread.event = Event()
            self._heartbeat_thread.start()

        if block:
            self.block()

    def status_to_scheduler(self, header, payload):
        out_header = {'jobid': header.get('jobid')}
        log(self.address, 'Status check', header['address'])
        self.send_to_scheduler(out_header, 'OK')

    def status_to_worker(self, header, payload):
        out_header = {'jobid': header.get('jobid')}
        log(self.address, 'Status check', header['address'])
        self.send_to_worker(header['address'], out_header, 'OK')

    def getitem_worker(self, header, payload):
        """ Get data and send to another worker

        See Also
        --------
        Worker.collect
        """
        loads = header.get('loads', pickle.loads)
        payload = loads(payload)
        log(self.address, "Getitem for worker", header, payload)
        header2 = {'function': 'getitem-ack',
                   'jobid': header.get('jobid')}
        try:
            result = self.data[payload['key']]
            header2['status'] = 'OK'
        except KeyError as e:
            result = e
            header2['status'] = 'Bad key'
        payload = {'key': payload['key'],
                   'value': result,
                   'queue': payload['queue']}
        self.send_to_worker(header['address'], header2, payload)

    def getitem_ack(self, header, payload):
        """ Receive data after sending a getitem request

        See Also
        --------
        Worker.getitem_worker
        Worker.collect
        """
        with logerrors():
            loads = header.get('loads', pickle.loads)
            payload = loads(payload)
            log(self.address, 'Getitem ack', payload)
            if header['status'] == 'Bad key':
                msg = {'status': 'failed',
                       'key': payload['key'],
                       'worker': header['address']}

            elif header['status'] == 'OK':
                self.data[payload['key']] = payload['value']
                msg = {'status': 'success',
                       'key': payload['key'],
                       'worker': header['address']}

            self.queues[payload['queue']].put(msg)

    def getitem_scheduler(self, header, payload):
        """ Send local data to scheduler

        See Also
        --------
        Scheduler.gather
        Scheduler.getitem_ack
        """
        loads = header.get('loads', pickle.loads)
        payload = loads(payload)
        log(self.address, 'Get from scheduler', payload)
        key = payload['key']
        header2 = {'jobid': header.get('jobid')}
        try:
            result = self.data[key]
            header2['status'] = 'OK'
        except KeyError as e:
            result = e
            header2['status'] = 'Bad key'
        header2['function'] = 'getitem-ack'
        payload2 = {'key': key, 'value': result, 'queue': payload['queue']}
        self.send_to_scheduler(header2, payload2)

    def setitem(self, header, payload):
        """ Assign incoming data to local dictionary

        See Also
        --------
        Scheduler.scatter
        Scheduler.send_data
            Scheduler.setitem_ack
        """
        loads = header.get('loads', pickle.loads)
        payload = loads(payload)
        log(self.address, 'Setitem', payload['key'])
        key = payload['key']
        value = payload['value']
        self.data[key] = value

        queue = payload.get('queue', False)
        if queue:
            header2 = {'jobid': header.get('jobid'),
                       'function': 'setitem-ack'}
            payload2 = {'key': key, 'queue': queue}
            log(self.address, 'Setitem send ack to scheduler',
                header2, payload2)
            self.send_to_scheduler(header2, payload2)

    def delitem(self, header, payload):
        """ Remove item from local data """
        loads = header.get('loads', pickle.loads)
        payload = loads(payload)
        log(self.address, 'Delitem', payload)
        key = payload['key']
        del self.data[key]

        # TODO: this should be replaced with a delitem-ack call
        if payload.get('reply', False):
            self.send_to_scheduler({'jobid': header.get('jobid')}, 'OK')


    def send_to_scheduler(self, header, payload):
        """ Send data to scheduler """
        log(self.address, 'Send to scheduler', header)
        header['address'] = self.address
        header['timestamp'] = datetime.utcnow()
        dumps = header.get('dumps', pickle_dumps)
        with self.lock:
            self.to_scheduler.send_multipart([pickle_dumps(header),
                                              dumps(payload)])

    def send_to_worker(self, address, header, payload):
        """ Send data to workers

        This is a bit tricky.  We want to have one DEALER socket per worker.
        We cache these in ``self.dealers``.  If the number of worker peers is
        high then we might run into having too many file descriptors open.
        Currently we flush the cache of dealers periodically.  This has yet to
        be tested.
        """
        if address not in self.dealers:
            if len(self.dealers) > MAX_DEALERS:
                for sock in self.dealers.values():
                    sock.close()
                self.dealers.clear()
            sock = self.context.socket(zmq.DEALER)
            sock.connect(address)
            self.dealers[address] = sock

        header['address'] = self.address
        header['timestamp'] = datetime.utcnow()
        log(self.address, 'Send to worker', address, header)
        dumps = header.get('dumps', pickle_dumps)
        with self.lock:
            self.dealers[address].send_multipart([pickle_dumps(header),
                                                  dumps(payload)])

    def listen_to_scheduler(self):
        """
        Event loop listening to commands from scheduler

        Header and Payload should deserialize into dicts of the following form:

            Header
            {'function': name of function to call, see self.functions,
             'jobid': job identifier, defaults to None,
             'address': name of sender, defaults to zmq identity}

            Payload
            --Function specific, for setitem might include the following--
            {'key': 'x',
             'value': 10}

        So the minimal request would be as follows:

        >>> sock = context.socket(zmq.DEALER)  # doctest: +SKIP
        >>> sock.connect('tcp://my-address')   # doctest: +SKIP

        >>> header = {'function': 'status'}
        >>> payload = {}
        >>> sock.send_multipart(dumps(header), dumps(status))  # doctest: +SKIP

        Or a more complex packet might be as follows:

        >>> header = {'function': 'setitem', 'jobid': 1}
        >>> payload = {'key': 'x', 'value': 10}
        >>> sock.send_multipart(dumps(header), dumps(status))  # doctest: +SKIP

        We match the function string against ``self.scheduler_functions`` to
        pull out the actual function.  We then execute this function with the
        provided arguments in another thread from ``self.pool``.  That function
        may then choose to send results back to the sender.

        See Also:
            listen_to_workers
            send_to_scheduler
        """
        while self.status != 'closed':
            # Wait on request
            try:
                if not self.to_scheduler.poll(100):
                    continue
            except zmq.ZMQError:
                break
            with logerrors():
                with self.lock:
                    header, payload = self.to_scheduler.recv_multipart()
                header = pickle.loads(header)
                log(self.address, 'Receive job from scheduler', header)
                if header['function'] in self.immediate_functions:
                    function = self.immediate_functions[header['function']]
                    function(header, payload)
                elif header['function'] in self.scheduler_functions:
                    function = self.scheduler_functions[header['function']]
                    future = self.pool.apply_async(function, args=(header, payload))
                else:
                    log(self.address, 'Unknown function', header)

    def listen_to_workers(self):
        """ Listen to communications from workers

        See ``listen_to_scheduler`` for more in depth docstring
        """
        while self.status != 'closed':
            # Wait on request
            try:
                if not self.to_workers.poll(100):
                    continue
            except zmq.ZMQError:
                break

            with logerrors():
                address, header, payload = self.to_workers.recv_multipart()
                header = pickle.loads(header)
                if 'address' not in header:
                    header['address'] = address
                log(self.address, 'Receive job from worker', address, header)

                try:
                    function = self.worker_functions[header['function']]
                except KeyError:
                    log(self.address, 'Unknown function', header)
                else:
                    future = self.pool.apply_async(function, args=(header, payload))

    def block(self):
        """ Block until listener threads close

        Warning: If some other thread doesn't call `.close()` then, in the
        common case you can not easily escape from this.
        """
        self._listen_workers_thread.join()
        self._listen_scheduler_thread.join()
        if self.heartbeat:
            self._heartbeat_thread.join()
        log(self.address, 'Unblocked')

    def collect(self, locations):
        """ Collect data from peers

        Given a dictionary of desired data and who holds that data

        This fires off getitem reqeusts to one of the hosts for each piece of
        data then blocks on all of the responses, then inserts this data into
        ``self.data``.

        Examples
        --------

        >>> locations = {'x': ['tcp://alice:5000', 'tcp://bob:5000'],
        ...              'y': ['tcp://bob:5000']}
        >>> worker.collect(locations)  # doctest: +SKIP

        Protocol
        --------

        1.  Worker creates unique queue
        2.  For each data this worker chooses a worker at random that holds
            that data and fires off a 'getitem' request
            {'key': ..., 'queue': ...}
        3.  Recipient worker handles the request and fires back a 'getitem-ack'
            with the data
            {'key': ..., 'value': ..., 'queue': ...}
        4.  Local getitem_ack function adds the value to the local dict and
            puts the key in the queue
        5.  Once all keys have run through the queue the collect function wakes
            up again, releases the queue, and returns control
        6?  This is often called from Worker.compute; control often ends there

        See Also
        --------
        Worker.getitem
        Worker.getitem_ack
        Worker.compute
        Scheduler.trigger_task
        """
        socks = []

        qkey = str(uuid.uuid1())
        queue = Queue()
        self.queues[qkey] = queue
        locations = locations.copy()  # don't mutate global locations

        # Send out requests for data
        log(self.address, 'Collect data from peers', locations)
        start = time()
        counter = 0
        with logerrors():
            for key, locs in list(locations.items()):
                if key in self.data:  # already have this locally
                    locations.pop(key)
                    continue
                try:
                    worker = random.choice(tuple(locs))  # randomly select one peer
                except IndexError:
                    raise ValueError("%s could not be collected from any "
                                     "locations." % (key))

                # track keys and where they are comming from
                self.queues_by_worker[worker][qkey].add(key)

                header = {'jobid': key,
                          'function': 'getitem'}
                payload = {'function': 'getitem',
                           'key': key,
                           'queue': qkey}
                self.send_to_worker(worker, header, payload)
                counter += 1

            msgs = [queue.get() for i in range(counter)]
            for m in msgs:
                if m['status'] == 'failed':
                    locations[m['key']].remove(m['worker'])
                    log(self.address, 'Failed to get key: ', m['key'],
                        ' from worker: ', m['worker'])
                else:
                    locations.pop(m['key'])

            del self.queues[qkey]
            if locations != {}:
                log(self.address, 'Retrying collect with keys and locations',
                    locations)
                self.collect(locations)
            log(self.address, 'Collect finishes', time() - start, 'seconds')

    def compute(self, header, payload):
        """ Compute dask task

        Given a key, task, and locations of data

        >>> from operator import add
        >>> payload = {'key': 'z',
        ...            'task': (add, 'x', 'y'),
        ...            'locations': {'x': ['tcp://alice:5000']},
        ...            'queue': 'unique-identifier'}

        Collect necessary data from locations (see ``collect``),
        then compute task and store result into ``self.data``.  Finally report
        back to the scheduler that we're free.
        """
        with logerrors():
            # Unpack payload
            loads = header.get('loads', pickle.loads)
            payload = loads(payload)
            locations = payload['locations']
            key = payload['key']
            task = payload['task']

            # Grab data from peers
            if locations:
                self.collect(locations)

            # Do actual work
            start = time()
            status = "OK"
            log(self.address, "Start computation", key, task)
            try:
                result = core.get(self.data, task)
                end = time()
            except Exception as e:
                status = e
                end = time()
            else:
                self.data[key] = result
            log(self.address, "End computation", key, task, status)

            # Report finished to scheduler
            header2 = {'function': 'finished-task'}
            result = {'key': key,
                      'duration': end - start,
                      'status': status,
                      'dependencies': list(locations),
                      'queue': payload['queue']}
            self.send_to_scheduler(header2, result)

    def close_from_scheduler(self, header, payload):
        log(self.address, 'Close signal from scheduler')
        self.close()

    def close(self):
        with self.lock:
            if self.status != 'closed':
                self.status = 'closed'
                do_close = True
            else:
                do_close = False

        if do_close:
            log(self.address, 'Close')
            self.status = 'closed'
            if self.heartbeat:
                self._heartbeat_thread.event.set()  # stop heartbeat
            for sock in self.dealers.values():
                sock.close(linger=1)
            self.to_workers.close(linger=1)
            self.to_scheduler.close(linger=1)
            self.pool.close()
            self.pool.join()
            log(self.address, 'Close pool')
            self.block()
            self.context.destroy(linger=3)

    def __del__(self):
        self.close()

    def __repr__(self):
        s = '<dask.distributed.Worker address=%s, scheduler=%s>'
        return s % (self.address.decode('utf-8')[6:],
                    self.scheduler.decode('utf-8')[6:])

    def heart(self, pulse=5):
        """Send a message to scheduler at a given interval"""
        while self.status != 'closed':
            header = {'function': 'heartbeat'}
            payload = {'pid': self.pid}
            self.send_to_scheduler(header, payload)
            self._heartbeat_thread.event.wait(pulse)

    def worker_death(self, header, payload):
        """
        A worker died, check no queues are blocking waiting for data from the
        worker.
        """
        with logerrors():
            loads = header.get('loads', pickle.loads)
            payload = loads(payload)
            removed_workers = payload['removed']
            for w in removed_workers:
                for queue, keys in self.queues_by_worker[w].items():
                    for k in keys:
                        msg = {'status': 'failed',
                               'key': k,
                               'worker': w}
                        self.queues[queue].put(msg)


def status():
    return 'OK'

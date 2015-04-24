from __future__ import print_function

import socket
from threading import Thread, Lock
from multiprocessing.pool import ThreadPool
from contextlib import contextmanager
from datetime import datetime
import uuid
import random
import multiprocessing
import zmq
from toolz import partial
from time import time
import sys
from ..compatibility import Queue, unicode
from .. import core
try:
    import cPickle as pickle
except ImportError:
    import pickle
import dill


MAX_DEALERS = 100

context = zmq.Context()

with open('log.workers', 'w') as f:  # delete file
    pass

def log(*args):
    with open('log.workers', 'a') as f:
        print(*args, file=f)


@contextmanager
def logerrors():
    try:
        yield
    except Exception as e:
        log('Error!', str(e))
        raise

class Worker(object):
    """ Asynchronous worker in a distributed dask computation pool

    State
    -----

    scheduler: string
        Address of scheduler
    status: string
        Status of worker, either 'run' or 'closed'
    to_workers: zmq.Socket
        Router socket to serve requests from other workers
    to_scheduler: zmq.Socket
        Dealer socket to communicate with scheduler
    address: string
        The address of my router socket

    See Also
    --------

        dask.distributed.scheduler.Scheduler
    """
    def __init__(self, scheduler, data=None, nthreads=100,
                 address=None, port=None):
        if isinstance(scheduler, unicode):
            scheduler = scheduler.encode()
        self.data = data if data is not None else dict()
        self.pool = ThreadPool(nthreads)
        self.scheduler = scheduler
        self.status = 'run'

        self.to_workers = context.socket(zmq.ROUTER)
        if address is None:
            hostname = socket.gethostname()
            if port:
                self.to_workers.bind('tcp://%s:%d' % (hostname, port))
            else:
                port = self.to_workers.bind_to_random_port('tcp://*')
            address = ('tcp://%s:%s' % (hostname, port)).encode()
        else:
            if isinstance(address, unicode):
                address = address.encode()
            self.to_workers.bind(address)
        self.address = address
        self.dealers = dict()

        self.lock = Lock()

        self.queues = dict()

        self.to_scheduler = context.socket(zmq.DEALER)

        self.to_scheduler.setsockopt(zmq.IDENTITY, address)
        self.to_scheduler.connect(scheduler)
        self.send_to_scheduler({'function': 'register'}, {})

        self.scheduler_functions = {'status': self.status_to_scheduler,
                                    'compute': self.compute,
                                    'getitem': self.getitem_scheduler,
                                    'delitem': self.delitem,
                                    'setitem': self.setitem}

        self.worker_functions = {'getitem': self.getitem_worker,
                                 'getitem-ack': self.getitem_ack,
                                 'status': self.status_to_worker}

        log(self.address, 'Start up', self.scheduler)

        self._listen_scheduler_thread = Thread(target=self.listen_to_scheduler)
        self._listen_scheduler_thread.start()
        self._listen_workers_thread = Thread(target=self.listen_to_workers)
        self._listen_workers_thread.start()

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

        See also:
            Worker.collect
        """
        serializer = header.get('serializer', pickle)
        payload = serializer.loads(payload)
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

        See also:
            Worker.getitem_worker
            Worker.collect
        """
        with logerrors():
            serializer = header.get('serializer', pickle)
            payload = serializer.loads(payload)
            log(self.address, 'Getitem ack', payload)
            assert header['status'] == 'OK'

            self.data[payload['key']] = payload['value']
            self.queues[payload['queue']].put(payload['key'])

    def getitem_scheduler(self, header, payload):
        """ Send local data to scheduler

        See also:
            Scheduler.gather
            Scheduler.getitem_ack
        """
        serializer = header.get('serializer', pickle)
        payload = serializer.loads(payload)
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

        See also:
            Scheduler.scatter
            Scheduler.send_data
            Scheduler.setitem_ack
        """
        serializer = header.get('serializer', pickle)
        payload = serializer.loads(payload)
        log(self.address, 'Setitem', payload)
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
        serializer = header.get('serializer', pickle)
        payload = serializer.loads(payload)
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
        serializer = header.get('serializer', pickle)
        with self.lock:
            self.to_scheduler.send_multipart([dill.dumps(header),
                                              serializer.dumps(payload)])

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
            sock = context.socket(zmq.DEALER)
            sock.connect(address)
            self.dealers[address] = sock

        header['address'] = self.address
        header['timestamp'] = datetime.utcnow()
        log(self.address, 'Send to worker', address, header)
        serializer = header.get('serializer', pickle)
        with self.lock:
            self.dealers[address].send_multipart([dill.dumps(header),
                                                  serializer.dumps(payload)])

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
            if not self.to_scheduler.poll(100):
                continue
            header, payload = self.to_scheduler.recv_multipart()
            header = pickle.loads(header)
            log(self.address, 'Receive job from scheduler', header)
            try:
                function = self.scheduler_functions[header['function']]
            except KeyError:
                log(self.address, 'Unknown function', header)
            else:
                future = self.pool.apply_async(function, args=(header, payload))

    def listen_to_workers(self):
        """ Listen to communications from workers

        See ``listen_to_scheduler`` for more in depth docstring
        """
        while self.status != 'closed':
            # Wait on request
            if not self.to_workers.poll(100):
                continue

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

    def collect(self, locations):
        """ Collect data from peers

        Given a dictionary of desired data and who holds that data

        This fires off getitem reqeusts to one of the hosts for each piece of
        data then blocks on all of the responses, then inserts this data into
        ``self.data``.

        Example
        -------

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

        See also:
            Worker.getitem
            Worker.getitem_ack
            Worker.compute
            Scheduler.trigger_task
        """
        socks = []

        qkey = str(uuid.uuid1())
        queue = Queue()
        self.queues[qkey] = queue

        # Send out requests for data
        log(self.address, 'Collect data from peers', locations)
        counter = 0
        with logerrors():
            for key, locs in locations.items():
                if key in self.data:  # already have this locally
                    continue
                worker = random.choice(tuple(locs))  # randomly select one peer
                header = {'jobid': key,
                          'function': 'getitem'}
                payload = {'function': 'getitem',
                           'key': key,
                           'queue': qkey}
                self.send_to_worker(worker, header, payload)
                counter += 1

            for i in range(counter):
                queue.get()

            del self.queues[qkey]
            log(self.address, 'Collect finishes')

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
        # Unpack payload
        serializer = header.get('serializer', pickle)
        payload = serializer.loads(payload)
        locations = payload['locations']
        key = payload['key']
        task = payload['task']

        # Grab data from peers
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

    def close(self):
        if self.pool._state == multiprocessing.pool.RUN:
            log(self.address, 'Close')
            self.status = 'closed'
            self.pool.close()
            self.pool.join()

    def __del__(self):
        self.close()


def status():
    return 'OK'

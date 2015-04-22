from __future__ import print_function

from zmqompute import ComputeNode
from threading import Thread, Lock
from multiprocessing.pool import ThreadPool
from contextlib import contextmanager
import uuid
import random
import multiprocessing
import zmq
import dask
from toolz import partial, get, curry
from time import time
import sys
try:
    from cPickle import dumps, loads, HIGHEST_PROTOCOL
except ImportError:
    from pickle import dumps, loads, HIGHEST_PROTOCOL


DEBUG = True

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


    See Also
    --------

    """
    def __init__(self, scheduler, data=None, nthreads=100,
                 dumps=partial(dumps, protocol=HIGHEST_PROTOCOL),
                 loads=loads, address=None, port=None):
        self.data = data if data is not None else dict()
        self.pool = ThreadPool(nthreads)
        self.dumps = dumps
        self.loads = loads
        self.scheduler = scheduler
        self.status = 'run'

        if address is None:
            if port is None:
                port = 6464
            address = 'tcp://%s:%d' % (socket.gethostname(), port)
        self.address = address

        self.lock = Lock()

        self.dealer = context.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IDENTITY, address)
        self.dealer.connect(scheduler)
        self.send_to_scheduler({'function': 'register'}, {})

        self.router = context.socket(zmq.ROUTER)
        self.router.bind(self.address)

        self.scheduler_functions = {'status': self.status_to_scheduler,
                                    'compute': self.compute,
                                    'getitem': self.get_scheduler,
                                    'delitem': self.delitem,
                                    'setitem': self.setitem}

        self.worker_functions = {'getitem': self.get_worker,
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

    def get_worker(self, header, payload):
        payload = self.loads(payload)
        header2 = {'jobid': header['jobid']}
        try:
            result = self.data[payload['key']]
        except KeyError as e:
            result = e
            header2['status'] = 'Bad key'
        self.send_to_worker(header['address'], header2, result)

    def get_scheduler(self, header, payload):
        payload = self.loads(payload)
        header2 = {'jobid': header['jobid']}
        try:
            result = self.data[payload['key']]
        except KeyError as e:
            result = e
            header2['status'] = 'Bad key'
        self.send_to_scheduler(header2, result)

    def setitem(self, header, payload):
        payload = self.loads(payload)
        log(self.address, 'Setitem', payload)
        key = payload['key']
        value = payload['value']
        self.data[key] = value

        reply = payload.get('reply', False)
        if reply:
            header2 = {'jobid': header.get('jobid'),
                       'function': 'setitem-ack'}
            payload2 = {'key': key}
            self.send_to_scheduler(header2, payload2)

    def delitem(self, header, payload):
        payload = self.loads(payload)
        log(self.address, 'Delitem', payload)
        key = payload['key']
        del self.data[key]

        if payload.get('reply', False):
            self.send_to_scheduler({'jobid': header.get('jobid')}, 'OK')


    def send_to_scheduler(self, header, payload):
        log(self.address, 'Send to scheduler', header)
        header['address'] = self.address
        with self.lock:
            self.dealer.send_multipart([self.dumps(header),
                                        self.dumps(payload)])

    def send_to_worker(self, address, header, result):
        log(self.address, 'Send to worker', address, header)
        header['address'] = self.address
        with self.lock:
            self.router.send_multipart([address,
                                        self.dumps(header),
                                        self.dumps(result)])

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
            if not self.dealer.poll(100):
                continue
            header, payload = self.dealer.recv_multipart()
            header = self.loads(header)
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
            if not self.router.poll(100):
                continue

            address, header, payload = self.router.recv_multipart()
            header = self.loads(header)
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

        >>> locations = {'x': ['tcp://alice:5000', 'tcp://bob:5000'],
        ...              'y': ['tcp://bob:5000']}

        This fires off getitem reqeusts to one of the hosts for each piece of
        data then blocks on all of the responses, then inserts this data into
        ``self.data``.
        """
        socks = []

        # Send out requests for data
        log(self.address, 'Collect data from peers', locations)
        for key, locs in locations.items():
            if key in self.data:  # already have this locally
                continue
            sock = context.socket(zmq.DEALER)
            sock.connect(random.choice(tuple(locs)))  # randomly select one peer
            header = {'jobid': key,
                      'function': 'getitem'}
            payload = {'function': 'getitem',
                       'key': key}
            sock.send_multipart([self.dumps(header),
                                 self.dumps(payload)])
            socks.append(sock)

        # Wait on replies.  Store results in self.data.
        log(self.address, 'Waiting on data replies')
        for sock in socks:
            header, payload = sock.recv_multipart()
            header = self.loads(header)
            payload = self.loads(payload)
            log(self.address, 'Receive data', header['address'],
                                              header['jobid'])
            self.data[header['jobid']] = payload

    def compute(self, header, payload):
        """ Compute dask task

        Given a key, task, and locations of data

            key -- 'z'
            task -- (add, 'x', 'y')
            locations -- {'x': ['tcp://alice:5000']}

        Collect necessary data from locations, merge into self.data (see
        ``collect``), then compute task and store into ``self.data``.
        """
        # Unpack payload
        payload = self.loads(payload)
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
            result = dask.core.get(self.data, task)
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
                  'dependencies': list(locations)}
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

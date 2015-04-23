from __future__ import print_function

import zmq
import socket
import uuid
from collections import defaultdict
import itertools
from multiprocessing.pool import ThreadPool
import random
from datetime import datetime
from threading import Thread, Lock
from contextlib import contextmanager
from toolz import curry, partial
from ..compatibility import Queue, unicode
try:
    from cPickle import loads, dumps, HIGHEST_PROTOCOL
except ImportError:
    from pickle import loads, dumps, HIGHEST_PROTOCOL
dumps = partial(dumps, protocol=HIGHEST_PROTOCOL)

from ..core import get_dependencies, flatten
from .. import core
from ..async import finish_task, start_state_from_dask as dag_state_from_dask

with open('log.scheduler', 'w') as f:  # delete file
    pass

def log(*args):
    with open('log.scheduler', 'a') as f:
        print(*args, file=f)

@contextmanager
def logerrors():
    try:
        yield
    except Exception as e:
        log('Error!', str(e))
        raise

class Scheduler(object):
    """ Disitributed scheduler for dask computations

    State
    -----

    workers - dict
        Maps worker identities to information about that worker
    who_has - dict
        Maps data keys to sets of workers that own that data
    worker_has - dict
        Maps workers to data that they own
    data - dict
        Maps data keys to metadata about the computation that produced it
    to_workers - zmq.Socket (ROUTER)
        Socket to communicate to workers
    to_clients - zmq.Socket (ROUTER)
        Socket to communicate with users
    address_to_workers - string
        ZMQ address of our connection to workers
    address_to_clients - string
        ZMQ address of our connection to clients
    """
    def __init__(self, address_to_workers=None, address_to_clients=None):
        self.context = zmq.Context()
        hostname = socket.gethostname()

        # Bind routers to addresses (and create addresses if necessary)
        self.to_workers = self.context.socket(zmq.ROUTER)
        if address_to_workers is None:
            port = self.to_workers.bind_to_random_port('tcp://*')
            self.address_to_workers = ('tcp://%s:%d' % (hostname, port)).encode()
        else:
            if isinstance(address_to_workers, unicode):
                address_to_workers = address_to_workers.encode()
            self.address_to_workers = address_to_workers
            self.to_workers.bind(self.address_to_workers)

        self.to_clients = self.context.socket(zmq.ROUTER)
        if address_to_clients is None:
            port = self.to_clients.bind_to_random_port('tcp://*')
            self.address_to_clients = ('tcp://%s:%d' % (hostname, port)).encode()
        else:
            if isinstance(address_to_clients, unicode):
                address_to_clients = address_to_clients.encode()
            self.address_to_clients = address_to_clients
            self.to_clients.bind(self.address_to_clients)

        # State about my workers and computed data
        self.workers = dict()
        self.who_has = defaultdict(set)
        self.worker_has = defaultdict(set)
        self.available_workers = Queue()
        self.data = defaultdict(dict)

        self.pool = ThreadPool(100)
        self.lock = Lock()
        self.status = 'run'
        self.queues = dict()

        self.loads = loads
        self.dumps = dumps

        # RPC functions that workers and clients can trigger
        self.worker_functions = {'register': self.worker_registration,
                                 'status': self.status_to_worker,
                                 'finished-task': self.worker_finished_task,
                                 'setitem-ack': self.setitem_ack,
                                 'getitem-ack': self.getitem_ack}
        self.client_functions = {'status': self.status_to_client,
                                 'schedule': self.schedule_from_client}

        # Away we go!
        log(self.address_to_workers, 'Start')
        self._listen_to_workers_thread = Thread(target=self.listen_to_workers)
        self._listen_to_workers_thread.start()
        self._listen_to_clients_thread = Thread(target=self.listen_to_clients)
        self._listen_to_clients_thread.start()

        self.active_tasks = set()

    def listen_to_workers(self):
        while self.status != 'closed':
            if not self.to_workers.poll(100):
                continue
            address, header, payload = self.to_workers.recv_multipart()

            header = self.loads(header)
            if 'address' not in header:
                header['address'] = address
            log(self.address_to_workers, 'Receive job from worker', header)

            try:
                function = self.worker_functions[header['function']]
            except KeyError:
                log(self.address_to_workers, 'Unknown function', header)
            else:
                future = self.pool.apply_async(function, args=(header, payload))

    def listen_to_clients(self):
        while self.status != 'closed':
            if not self.to_clients.poll(100):
                continue
            address, header, payload = self.to_clients.recv_multipart()
            header = self.loads(header)
            if 'address' not in header:
                header['address'] = address
            log(self.address_to_clients, 'Receive job from client', header)

            try:
                function = self.client_functions[header['function']]
            except KeyError:
                log(self.address_to_clients, 'Unknown function', header)
            else:
                self.pool.apply_async(function, args=(header, payload))

    def worker_registration(self, header, payload):
        payload = self.loads(payload)
        address = header['address']
        self.workers[address] = payload
        self.available_workers.put(address)

    def worker_finished_task(self, header, payload):
        log('Hello')
        with logerrors():
            address = header['address']

            payload = self.loads(payload)
            key = payload['key']
            duration = payload['duration']
            dependencies = payload['dependencies']

            log(self.address_to_workers, 'Finish task', payload)
            self.active_tasks.remove(key)

            self.data[key]['duration'] = duration
            self.who_has[key].add(address)
            self.worker_has[address].add(key)
            for dep in dependencies:
                self.who_has[dep].add(address)
                self.worker_has[address].add(dep)
            self.available_workers.put(address)

            self.queues[payload['queue']].put(payload)

    def status_to_client(self, header, payload):
        with logerrors():
            out_header = {'jobid': header.get('jobid')}
            log(self.address_to_clients, 'Status')
            self.send_to_client(header['address'], out_header, 'OK')

    def status_to_worker(self, header, payload):
        out_header = {'jobid': header.get('jobid')}
        log(self.address_to_workers, 'Status sending')
        self.send_to_worker(header['address'], out_header, 'OK')

    def send_to_worker(self, address, header, payload):
        log(self.address_to_workers, 'Send to worker', address, header)
        header['address'] = self.address_to_workers
        if isinstance(address, unicode):
            address = address.encode()
        header['timestamp'] = datetime.utcnow()
        with self.lock:
            self.to_workers.send_multipart([address,
                                            self.dumps(header),
                                            self.dumps(payload)])

    def send_to_client(self, address, header, result):
        log(self.address_to_clients, 'Send to client', address, header)
        header['address'] = self.address_to_clients
        if isinstance(address, unicode):
            address = address.encode()
        header['timestamp'] = datetime.utcnow()
        with self.lock:
            self.to_clients.send_multipart([address,
                                            self.dumps(header),
                                            self.dumps(result)])

    def trigger_task(self, dsk, key):
        deps = get_dependencies(dsk, key)
        worker = self.available_workers.get()
        locations = dict((dep, self.who_has[dep]) for dep in deps)

        header = {'function': 'compute', 'jobid': key}
        payload = {'key': key, 'task': dsk[key], 'locations': locations}
        self.send_to_worker(worker, header, payload)
        self.active_tasks.add(key)

    def release_key(self, key):
        """ Release data from all workers """
        with logerrors():
            workers = list(self.who_has[key])
            log(self.address_to_workers, 'Release data', key, workers)
            header = {'function': 'delitem', 'jobid': key}
            payload = {'key': key}
            for worker in workers:
                self.send_to_worker(worker, header, payload)
                self.who_has[key].remove(worker)
                self.worker_has[worker].remove(key)

    def send_data(self, key, value, address=None, reply=True):
        """ Send data up to some worker

        If no address is given we select one worker randomly
        """
        if reply:
            queue = Queue()
            qkey = str(uuid.uuid1())
            self.queues[qkey] = queue
        else:
            qkey = None
        if address is None:
            address = random.choice(list(self.workers))
        header = {'function': 'setitem', 'jobid': key}
        payload = {'key': key, 'value': value, 'queue': qkey}
        self.send_to_worker(address, header, payload)

        if reply:
            queue.get()
            del self.queues[qkey]

    def scatter(self, key_value_pairs, block=True):
        """ Scatter data to workers

        Parameters
        ----------

        key_value_pairs: Iterator or dict
            Data to send
        block: bool
            Block on completion or return immediately (defaults to True)

        Example
        -------

        >>> scheduler.scatter({'x': 1, 'y': 2})  # doctest: +SKIP
        """
        workers = list(self.workers)
        log(self.address_to_workers, 'Scatter', workers, key_value_pairs)
        workers = itertools.cycle(workers)

        if isinstance(key_value_pairs, dict):
            key_value_pairs = key_value_pairs.items()
        queue = Queue()
        qkey = str(uuid.uuid1())
        self.queues[qkey] = queue
        counter = 0
        for (k, v), w in zip(key_value_pairs, workers):
            header = {'function': 'setitem', 'jobid': k}
            payload = {'key': k, 'value': v}
            if block:
                payload['queue'] = qkey
            self.send_to_worker(w, header, payload)
            counter += 1

        if block:
            for i in range(counter):
                queue.get()

            del self.queues[qkey]

    def gather(self, keys):
        qkey = str(uuid.uuid1())
        queue = Queue()
        self.queues[qkey] = queue

        # Send of requests
        self._gather_send(qkey, keys)

        # Wait for replies
        cache = dict()
        for i in flatten(keys):
            k, v = queue.get()
            cache[k] = v
        del self.queues[qkey]

        # Reshape to keys
        return core.get(cache, keys)

    def _gather_send(self, qkey, key):
        if isinstance(key, list):
            for k in key:
                self._gather_send(qkey, k)
        else:
            header = {'function': 'getitem', 'jobid': key}
            payload = {'key': key, 'queue': qkey}
            seq = list(self.who_has[key])
            worker = random.choice(seq)
            self.send_to_worker(worker, header, payload)

    def getitem_ack(self, header, payload):
        payload = self.loads(payload)
        log(self.address_to_workers, 'Getitem ack', payload)
        with logerrors():
            assert header['status'] == 'OK'
            self.queues[payload['queue']].put((payload['key'],
                                               payload['value']))

    def setitem_ack(self, header, payload):
        address = header['address']
        payload = self.loads(payload)
        key = payload['key']
        self.who_has[key].add(address)
        self.worker_has[address].add(key)
        queue = payload.get('queue')
        if queue:
            self.queues[queue].put(key)

    def close(self):
        self.status = 'closed'

    def schedule(self, dsk, result, **kwargs):
        if isinstance(result, list):
            result_flat = set(flatten(result))
        else:
            result_flat = set([result])
        results = set(result_flat)

        cache = dict()
        dag_state = dag_state_from_dask(dsk, cache=cache)
        self.scatter(cache.items())  # send data in dask up to workers

        tick = [0]

        if dag_state['waiting'] and not dag_state['ready']:
            raise ValueError("Found no accessible jobs in dask graph")

        event_queue = Queue()
        qkey = str(uuid.uuid1())
        self.queues[qkey] = event_queue

        def fire_task():
            tick[0] += 1  # Update heartbeat

            # Choose a good task to compute
            key = dag_state['ready'].pop()
            dag_state['ready-set'].remove(key)
            dag_state['running'].add(key)

            self.trigger_task(dsk, key, qkey)  # Fire

        # Seed initial tasks
        while dag_state['ready'] and self.available_workers.qsize() > 0:
            fire_task()

        # Main loop, wait on tasks to finish, insert new ones
        while dag_state['waiting'] or dag_state['ready'] or dag_state['running']:
            payload = event_queue.get()

            if isinstance(payload['status'], Exception):
                raise payload['status']

            key = payload['key']
            finish_task(dsk, key, dag_state, results,
                        release_data=self._release_data)

            while dag_state['ready'] and self.available_workers.qsize() > 0:
                fire_task()

        return self.gather(result)

    def schedule_from_client(self, header, payload):
        """

        Input Payload: keys, dask
        Output Payload: keys, result
        Sent to client on 'schedule-ack'
        """
        payload = self.loads(payload)
        address = header['address']
        dsk = payload['dask']
        keys = payload['keys']

        header2 = {'jobid': header.get('jobid'),
                   'function': 'schedule-ack'}
        try:
            result = self.schedule(dsk, keys)
            header2['status'] = 'OK'
        except Exception as e:
            result = e
            header2['status'] = 'Error'

        payload2 = {'keys': keys, 'result': result}
        self.send_to_client(address, header2, payload2)

    def _release_data(self, key, state, delete=True):
        """ Remove data from temporary storage during scheduling run

        See Also
            Scheduler.schedule
            dask.async.finish_task
        """
        if key in state['waiting_data']:
            assert not state['waiting_data'][key]
            del state['waiting_data'][key]

        state['released'].add(key)

        if delete:
            self.release_key(key)

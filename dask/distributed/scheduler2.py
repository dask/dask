from __future__ import print_function

import zmq
from collections import defaultdict
from multiprocessing.pool import ThreadPool
from threading import Thread, Lock
from toolz import curry, partial
try:
    from cPickle import loads, dumps, HIGHEST_PROTOCOL
except ImportError:
    from pickle import loads, dumps, HIGHEST_PROTOCOL
dumps = partial(dumps, protocol=HIGHEST_PROTOCOL)

context = zmq.Context()


with open('log.scheduler', 'w') as f:  # delete file
    pass

def log(*args):
    with open('log.scheduler', 'a') as f:
        print(*args, file=f)


class Scheduler(object):
    """ Disitributed scheduler for dask computations

    State
    -----

    workers - dict
        Maps worker identities to information about that worker
    whohas - dict
        Maps data keys to sets of workers that own that data
    ihave - dict
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
        assert (address_to_workers is None) == (address_to_clients is None)
        if address_to_workers is None:
            address_to_workers = 'tcp://%s:%d' % (socket.gethostname, 6465)
            address_to_clients = 'tcp://%s:%d' % (socket.gethostname, 6466)

        self.address_to_workers = address_to_workers
        self.address_to_clients = address_to_clients

        self.workers = dict()
        self.whohas = defaultdict(set)
        self.ihave = defaultdict(set)
        self.available_workers = list()

        self.data = dict()

        self.pool = ThreadPool(100)
        self.lock = Lock()

        self.to_workers = context.socket(zmq.ROUTER)
        self.to_workers.setsockopt(zmq.IDENTITY, self.address_to_workers)
        self.to_workers.bind(self.address_to_workers)

        self.to_clients = context.socket(zmq.ROUTER)
        self.to_clients.setsockopt(zmq.IDENTITY, self.address_to_clients)
        self.to_clients.bind(self.address_to_clients)

        self.status = 'run'

        self.loads = loads
        self.dumps = dumps

        self.worker_functions = {'register': self.worker_registration,
                                 'status': self.status_to_worker,
                                 'finished': self.worker_finished_task}
        self.client_functions = {'status': self.status_to_client}

        log(self.address_to_workers, 'Start')
        self._listen_to_workers_thread = Thread(target=self.listen_to_workers)
        self._listen_to_workers_thread.start()
        self._listen_to_clients_thread = Thread(target=self.listen_to_clients)
        self._listen_to_clients_thread.start()

    def listen_to_workers(self):
        while self.status != 'closed':
            if not self.to_workers.poll(100):
                continue
            address, header, payload = self.to_workers.recv_multipart()
            header = self.loads(header)
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
        self.available_workers.append(address)

    def worker_finished_task(self, header, payload):
        payload = self.loads(payload)
        key = payload['key']
        duration = payload['duration']
        address = header['address']

        self.data[key]['duration'] = duration
        self.whohas[key].add(address)
        self.ihave[address].add(key)
        self.available_workers.append(address)

    def status_to_client(self, header, payload):
        out_header = {'jobid': header.get('jobid')}
        self.send_to_client(header['address'], out_header, 'OK')

    def status_to_worker(self, header, payload):
        out_header = {'jobid': header.get('jobid')}
        log(self.address_to_workers, 'Status sending')
        self.send_to_worker(header['address'], out_header, 'OK')

    def send_to_worker(self, address, header, payload):
        log(self.address_to_workers, 'Send to worker', address, header)
        header['address'] = self.address_to_workers
        with self.lock:
            self.to_workers.send_multipart([address,
                                            self.dumps(header),
                                            self.dumps(payload)])

    def send_to_client(self, address, header, result):
        log(self.address_to_clients, 'Send to client', address, header)
        header['address'] = self.address_to_clients
        with self.lock:
            self.to_clients.send_multipart([address,
                                            self.dumps(header),
                                            self.dumps(result)])

    def close(self):
        self.status = 'closed'

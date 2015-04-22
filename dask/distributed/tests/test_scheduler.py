from dask.distributed.scheduler2 import Scheduler
from dask.distributed.node import Worker
import itertools
from contextlib import contextmanager
from toolz import take
from time import sleep

import zmq

context = zmq.Context()

server_names = ('ipc://server-%d' % i for i in itertools.count())
worker_names = ('ipc://worker-%d' % i for i in itertools.count())

@contextmanager
def scheduler():
    address = next(server_names)
    address_to_workers = address + '-workers'
    address_to_clients = address + '-clients'
    s = Scheduler(address_to_workers, address_to_clients)
    try:
        yield s
    finally:
        s.close()


def test_status_worker():
    with scheduler() as s:
        sock = context.socket(zmq.DEALER)
        sock.setsockopt(zmq.IDENTITY, 'ipc://worker1')
        sock.connect(s.address_to_workers)

        header = {'address': 'ipc://worker1', 'jobid': 1, 'function': 'status'}
        payload = {'function': 'status'}
        sock.send_multipart([s.dumps(header), s.dumps(payload)])

        header2, payload2 = sock.recv_multipart()
        assert s.loads(header2) == {'address': s.address_to_workers,
                                    'jobid': header.get('jobid')}
        assert s.loads(payload2) == 'OK'


def test_status_client():
    with scheduler() as s:
        sock = context.socket(zmq.DEALER)
        sock.setsockopt(zmq.IDENTITY, 'ipc://client-1')
        sock.connect(s.address_to_clients)

        header = {'address': 'ipc://client-1', 'jobid': 2, 'function': 'status'}
        payload = {'function': 'status'}
        sock.send_multipart([s.dumps(header), s.dumps(payload)])

        header2, payload2 = sock.recv_multipart()
        assert s.loads(header2) == {'address': s.address_to_clients,
                                    'jobid': header.get('jobid')}
        assert s.loads(payload2) == 'OK'


@contextmanager
def scheduler_and_workers(n=2):
    with scheduler() as s:
        workers = [Worker(s.address_to_workers, address=name)
                    for name in take(n, worker_names)]
        try:
            yield s, workers
        finally:
            for w in workers:
                w.close()


def test_cluster():
    with scheduler_and_workers() as (s, (a, b)):
        sleep(0.1)
        assert a.address in s.workers
        assert b.address in s.workers
        assert a.scheduler == s.address_to_workers
        assert b.scheduler == s.address_to_workers

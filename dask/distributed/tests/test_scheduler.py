from dask.distributed.scheduler import Scheduler, get_distributed
from dask.distributed.worker import Worker
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


def inc(x):
    return x + 1

def add(x, y):
    return x + y


def test_compute_cycle():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        sleep(0.1)
        assert s.available_workers.qsize() == 2

        dsk = {'a': (add, 1, 2), 'b': (inc, 'a')}
        s.trigger_task(dsk, 'a')
        sleep(0.1)

        assert 'a' in s.whohas
        assert 'a' in a.data or 'a' in b.data
        assert a.data.get('a') == 3 or b.data.get('a') == 3
        assert a.address in s.ihave or b.address in s.ihave
        assert s.available_workers.qsize() == 2

        s.trigger_task(dsk, 'b')
        sleep(0.1)

        assert 'b' in s.whohas
        assert 'b' in a.data or 'b' in b.data
        assert a.data.get('b') == 4 or b.data.get('b') == 4
        assert s.available_workers.qsize() == 2


def test_send_release_data():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        s.send_data('x', 1, a.address)
        sleep(0.05)
        assert a.data['x'] == 1
        assert a.address in s.whohas['x']
        assert 'x' in s.ihave[a.address]

        s.release_key('x')
        sleep(0.05)
        assert 'x' not in a.data
        assert a.address not in s.whohas['x']
        assert 'x' not in s.ihave[a.address]


def test_get():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        dsk = {'x': (add, 1, 2), 'y': (inc, 'x')}
        get_distributed(s, dsk, ['y'])

        sleep(0.05)
        assert 'y' in s.whohas
        assert a.data.get('y') == 4 or b.data.get('y') == 4
        assert not s.whohas['x']
        assert 'x' not in a.data and 'x' not in b.data

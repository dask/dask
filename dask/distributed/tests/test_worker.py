from dask.distributed.worker import Worker
from contextlib import contextmanager
import multiprocessing
import itertools
import zmq
from time import sleep

context = zmq.Context()

def inc(x):
    return x + 1

def add(x, y):
    return x + y


global_port = [5000]

worker_names = ('ipc://node-%d' % i for i in itertools.count())

@contextmanager
def worker(port=None, data=None, address=None):
    if port is None:
        global_port[0] += 1
        port = global_port[0]
    if data is None:
        data = dict()
    if address is None:
        address = next(worker_names)
    a = Worker('ipc://server', data, address=address)

    try:
        yield a
    finally:
        a.close()


@contextmanager
def worker_and_router(*args, **kwargs):
    with worker(*args, **kwargs) as w:
        router = context.socket(zmq.ROUTER)
        router.bind(w.scheduler)
        handshake = router.recv_multipart()  # burn initial handshake

        yield w, router


def test_status():
    with worker_and_router(data={'x': 10, 'y': 20}, address='ipc://alice') as (w, r):
        header = {'jobid': 3, 'function': 'status', 'address': 'ipc://server'}
        payload = {'function': 'status'}
        r.send_multipart(['ipc://alice', w.dumps(header), w.dumps(payload)])

        address, header, result = r.recv_multipart()
        assert address == w.address
        result = w.loads(result)
        header = w.loads(header)
        assert result == 'OK'
        assert header['address'] == w.address
        assert header['jobid'] == 3


def test_getitem():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 4, 'function': 'getitem', 'address': 'ipc://server'}
        payload = {'function': 'getitem', 'key': 'x'}
        r.send_multipart([w.address, w.dumps(header), w.dumps(payload)])

        address, header, result = r.recv_multipart()
        result = w.loads(result)
        header = w.loads(header)
        assert result == 10


def test_setitem():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 5, 'function': 'setitem', 'address': 'ipc://server'}
        payload = {'function': 'setitem', 'key': 'z', 'value': 30,
                   'reply': True}
        r.send_multipart([w.address, w.dumps(header), w.dumps(payload)])

        r.recv_multipart()
        assert w.data['z'] == 30


def test_delitem():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 5, 'function': 'delitem', 'address': 'ipc://server'}
        payload = {'function': 'delitem', 'key': 'y', 'reply': True}
        r.send_multipart([w.address, w.dumps(header), w.dumps(payload)])

        address, header, result = r.recv_multipart()
        assert 'y' not in w.data


def test_error():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 5, 'function': 'getitem', 'address': 'ipc://server'}
        payload = {'function': 'getitem', 'key': 'does-not-exist'}
        r.send_multipart([w.address, w.dumps(header), w.dumps(payload)])

        address, header, result = r.recv_multipart()
        result = w.loads(result)
        header = w.loads(header)
        assert isinstance(result, KeyError)
        assert header['status'] != 'OK'


def test_close():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        assert w.pool._state == multiprocessing.pool.RUN
        w.close()
        assert w.pool._state == multiprocessing.pool.CLOSE
        w.close()  # idempotent


def test_collect():
    with worker(data={'x': 10, 'y': 20}) as a:
        with worker(data={'a': 1, 'b': 2}) as b:
            with worker(data={'c': 5}) as c:
                router = context.socket(zmq.ROUTER)
                router.bind(c.scheduler)
                handshake = router.recv_multipart()  # burn initial handshake
                handshake = router.recv_multipart()  # burn initial handshake
                handshake = router.recv_multipart()  # burn initial handshake

                c.collect({'x': [a.address],
                           'a': [b.address],
                           'y': [a.address]})

                assert c.data == dict(a=1, c=5, x=10, y=20)


def test_compute():
    with worker(data={'x': 10, 'y': 20}) as a:
        with worker_and_router(data={'a': 1, 'b': 2}) as (b, r):
            r.recv_multipart()  # burn handshake

            header = {'function': 'compute'}
            payload = {'function': 'compute',
                       'key': 'c',
                       'task': (add, 'a', 'x'),
                       'locations': {'x': [a.address]}}
            r.send_multipart([b.address, b.dumps(header), b.dumps(payload)])

            address, header, result = r.recv_multipart()
            result = b.loads(result)
            header = b.loads(header)
            assert header['address'] == b.address
            assert b.data['c'] == 11
            assert 0 < result['duration'] < 1.0
            assert result['key'] == 'c'
            assert result['status'] == 'OK'

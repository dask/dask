import pytest
pytest.importorskip('zmq')
pytest.importorskip('dill')

from dask.utils import raises
from dask.distributed.worker import Worker
from contextlib import contextmanager
import multiprocessing
import itertools
import zmq
from time import sleep
import pickle

from dask.compatibility import Queue

context = zmq.Context()

def inc(x):
    return x + 1

def add(x, y):
    return x + y


@contextmanager
def worker(data=None, scheduler='tcp://127.0.0.1:5555'):
    if data is None:
        data = dict()

    a = Worker(scheduler, data, hostname='127.0.0.1', nthreads=10)

    try:
        yield a
    finally:
        a.close()


@contextmanager
def worker_and_router(*args, **kwargs):
    router = context.socket(zmq.ROUTER)
    router.RCVTIMEO = 100000
    port = kwargs.get('port')
    if port:
        router.bind('tcp://*:%d' % port)
    else:
        port = router.bind_to_random_port('tcp://*')
    kwargs['scheduler'] = 'tcp://localhost:%d' % port
    with worker(*args, **kwargs) as w:
        handshake = router.recv_multipart()  # burn initial handshake
        yield w, router

    router.close(1)


def test_status():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 3, 'function':  'status',
                  'address': w.scheduler}
        payload = {}
        r.send_multipart([w.address, pickle.dumps(header), pickle.dumps(payload)])

        address, header, result = r.recv_multipart()
        assert address == w.address
        result = pickle.loads(result)
        header = pickle.loads(header)
        assert result == 'OK'
        assert header['address'] == w.address
        assert header['jobid'] == 3


def test_getitem():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 4, 'function': 'getitem',
                  'address': w.scheduler}
        payload = {'key': 'x', 'queue': 'some-key'}
        r.send_multipart([w.address, pickle.dumps(header), pickle.dumps(payload)])

        address, header, payload = r.recv_multipart()
        payload = pickle.loads(payload)
        assert payload['value'] == 10
        assert payload['queue'] == 'some-key'
        header = pickle.loads(header)
        assert header['function'] == 'getitem-ack'


def test_setitem():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 5, 'function': 'setitem', 'address': w.scheduler}
        payload = {'function': 'setitem', 'key': 'z', 'value': 30}
        r.send_multipart([w.address, pickle.dumps(header), pickle.dumps(payload)])
        sleep(0.05)
        assert w.data['z'] == 30


def test_delitem():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 5, 'function': 'delitem', 'address': w.scheduler}
        payload = {'function': 'delitem', 'key': 'y', 'reply': True}
        r.send_multipart([w.address, pickle.dumps(header), pickle.dumps(payload)])

        address, header, result = r.recv_multipart()
        assert 'y' not in w.data


def test_error():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        header = {'jobid': 5, 'function': 'getitem', 'address': w.scheduler}
        payload = {'function': 'getitem', 'key': 'does-not-exist', 'queue': ''}
        r.send_multipart([w.address, pickle.dumps(header), pickle.dumps(payload)])

        address, header, result = r.recv_multipart()
        result = pickle.loads(result)
        header = pickle.loads(header)
        assert isinstance(result['value'], KeyError)
        assert header['status'] != 'OK'


def test_close():
    with worker_and_router(data={'x': 10, 'y': 20}) as (w, r):
        assert w.pool._state == multiprocessing.pool.RUN
        w.close()
        assert all(s.closed for s in w.dealers.values())
        assert w.to_scheduler.closed
        assert w.to_workers.closed
        assert w.pool._state == multiprocessing.pool.CLOSE
        w.close()  # idempotent


def test_collect():
    with worker_and_router(data={'x': 10, 'y': 20}) as (a, router):
        with worker(data={'a': 1, 'b': 2}, scheduler=a.scheduler) as b:
            with worker(data={'c': 5}, scheduler=a.scheduler) as c:
                handshake = router.recv_multipart()  # burn initial handshake
                handshake = router.recv_multipart()  # burn initial handshake

                c.collect({'x': [a.address],
                           'a': [b.address],
                           'c': [c.address],
                           'y': [a.address]})

                assert c.data == dict(a=1, c=5, x=10, y=20)

                assert raises(ValueError,
                              lambda: c.collect({'nope': [a.address]}))


def test_compute():
    with worker_and_router(data={'a': 1, 'b': 2}) as (b, r):
        with worker(data={'x': 10, 'y': 20}, scheduler=b.scheduler) as a:
            r.recv_multipart()  # burn handshake

            header = {'function': 'compute'}
            payload = {'key': 'c',
                       'task': (add, 'a', 'x'),
                       'locations': {'x': [a.address]},
                       'queue': 'q-key'}

            r.ROUTER_MANDATORY = 1
            r.send_multipart([b.address, pickle.dumps(header), pickle.dumps(payload)])
            # Worker b does stuff, sends back ack
            address, header, result = r.recv_multipart()
            header = pickle.loads(header)
            result = header.get('loads', pickle.loads)(result)
            assert header['address'] == b.address
            assert b.data['c'] == 11
            assert 0 <= result['duration'] < 1.0
            assert result['key'] == 'c'
            assert result['status'] == 'OK'
            assert result['queue'] == payload['queue']


def test_worker_death():
    with worker_and_router() as (w1, r):
        with worker(scheduler=w1.scheduler) as w2:
            r.recv_multipart()  # burn handshake

            # setup worker
            qkey = 'queue_key'
            dkey = 'data_key'
            w1.queues[qkey] = Queue()
            w1.queues_by_worker[w2.address] = {qkey: [dkey]}

            # mock message
            header = {}
            payload = pickle.dumps({'removed': [w2.address]})

            # worker death
            w1.worker_death(header, payload)

            # assertions
            msg = w1.queues[qkey].get()
            assert msg['status'] == 'failed'
            assert msg['key'] == dkey
            assert msg['worker'] == w2.address

from dask.distributed.node import Worker
from contextlib import contextmanager
import multiprocessing
import zmq

context = zmq.Context()

def inc(x):
    return x + 1

def add(x, y):
    return x + y


global_port = [5000]

@contextmanager
def worker(port=None, data=None):
    if port is None:
        global_port[0] += 1
        port = global_port[0]
    if data is None:
        data = dict()
    a = Worker('127.0.0.1:%d'%port, data)

    try:
        yield a
    finally:
        a.close()


def test_status():
    with worker(data={'x': 10, 'y': 20}) as w:
        socket = context.socket(zmq.REQ)
        socket.connect(w.address)

        payload = dict(function='status', jobid=3)
        socket.send(w.dumps(payload))
        result = socket.recv()
        result2 = w.loads(result)
        assert result2 == {'address': w.address,
                           'jobid': 3,
                           'result': 'OK',
                           'status': 'OK'}


def test_getitem():
    with worker(data={'x': 10, 'y': 20}) as w:
        socket = context.socket(zmq.REQ)
        socket.connect(w.address)

        payload = dict(function='getitem', args=('x',), jobid=4)
        socket.send(w.dumps(payload))
        result = w.loads(socket.recv())
        assert result == {'address': w.address,
                          'jobid': 4,
                          'result': 10,
                          'status': 'OK'}


def test_setitem():
    with worker(data={'x': 10, 'y': 20}) as w:
        socket = context.socket(zmq.REQ)
        socket.connect(w.address)

        payload = dict(function='setitem', args=('z', 30), jobid=4)
        socket.send(w.dumps(payload))
        result = w.loads(socket.recv())
        assert w.data['z'] == 30


def test_delitem():
    with worker(data={'x': 10, 'y': 20}) as w:
        socket = context.socket(zmq.REQ)
        socket.connect(w.address)

        assert 'y' in w.data
        payload = dict(function='delitem', args=('y',))
        socket.send(w.dumps(payload))
        result = w.loads(socket.recv())
        assert 'y' not in w.data


def test_Error():
    with worker(data={'x': 10, 'y': 20}) as w:
        socket = context.socket(zmq.REQ)
        socket.connect(w.address)

        payload = dict(function='getitem', args=('does-not-exist',))
        socket.send(w.dumps(payload))
        result = w.loads(socket.recv())
        assert isinstance(result['result'], KeyError)
        assert result['status'] != 'OK'


def test_close():
    with worker(data={'x': 10, 'y': 20}) as w:
        assert w.pool._state == multiprocessing.pool.RUN
        w.close()
        assert w.pool._state == multiprocessing.pool.CLOSE
        w.close()  # idempotent


def test_collect():
    with worker(data={'x': 10, 'y': 20}) as a:
        with worker(data={'a': 1, 'b': 2}) as b:
            with worker(data={'c': 5}) as c:
                socket = context.socket(zmq.REQ)
                socket.connect(c.address)

                payload = dict(function='collect', args=({'x': [a.address],
                                                          'a': [b.address],
                                                          'y': [a.address]},))
                socket.send(c.dumps(payload))

                result = c.loads(socket.recv())

                assert c.data == dict(a=1, c=5, x=10, y=20)


def test_compute():
    with worker(data={'x': 10, 'y': 20}) as a:
        with worker(data={'a': 1, 'b': 2}) as b:
            socket = context.socket(zmq.REQ)
            socket.connect(b.address)

            payload = dict(function='compute',
                           args=('c', (add, 'a', 'x'), {'x': [a.address]}))
            socket.send(b.dumps(payload))

            result = b.loads(socket.recv())
            assert b.data['c'] == 11
            assert 0 < result['result']['duration'] < 1.0
            assert result['result']['key'] == 'c'
            assert result['address'] == b.address
            assert result['status'] == 'OK'

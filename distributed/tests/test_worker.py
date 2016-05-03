from __future__ import print_function, division, absolute_import

from numbers import Integral
from operator import add
import os
import shutil
import sys
import traceback

import pytest
from toolz import pluck
from tornado import gen
from tornado.ioloop import TimeoutError

from distributed.batched import BatchedStream
from distributed.center import Center
from distributed.core import rpc, dumps, loads, connect, read, write
from distributed.sizeof import sizeof
from distributed.worker import Worker, error_message
from distributed.utils_test import loop, _test_cluster, inc, gen_cluster, slow



def test_worker_ncores():
    from distributed.worker import _ncores
    w = Worker('127.0.0.1', 8019)
    try:
        assert w.executor._max_workers == _ncores
    finally:
        shutil.rmtree(w.local_dir)


def test_identity():
    w = Worker('127.0.0.1', 8019)
    ident = w.identity(None)
    assert ident['type'] == 'Worker'
    assert ident['center'] == ('127.0.0.1', 8019)


def test_health():
    w = Worker('127.0.0.1', 8019)
    d = w.health()
    assert isinstance(d, dict)
    d = w.health()
    assert 'time' in d
    try:
        import psutil
    except ImportError:
        pass
    else:
        assert 'disk-read' in d
        assert 'disk-write' in d
        assert 'network-recv' in d
        assert 'network-send' in d


def test_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        aa = rpc(ip=a.ip, port=a.port)
        bb = rpc(ip=b.ip, port=b.port)

        result = yield aa.identity()
        assert not a.active
        response = yield aa.compute(key='x',
                                    function=dumps(add),
                                    args=dumps([1, 2]),
                                    who_has={},
                                    close=True)
        assert not a.active
        assert response['status'] == 'OK'
        assert a.data['x'] == 3
        assert c.who_has['x'] == {a.address}
        assert isinstance(response['compute_start'], float)
        assert isinstance(response['compute_stop'], float)
        assert isinstance(response['thread'], Integral)

        response = yield bb.compute(key='y',
                                    function=dumps(add),
                                    args=dumps(['x', 10]),
                                    who_has={'x': [a.address]})
        assert response['status'] == 'OK'
        assert b.data['y'] == 13
        assert c.who_has['y'] == {b.address}
        assert response['nbytes'] == sizeof(b.data['y'])
        assert isinstance(response['transfer_start'], float)
        assert isinstance(response['transfer_stop'], float)

        def bad_func():
            1 / 0

        response = yield bb.compute(key='z',
                                    function=dumps(bad_func),
                                    args=dumps(()),
                                    close=True)
        assert not b.active
        assert response['status'] == 'error'
        assert isinstance(loads(response['exception']), ZeroDivisionError)
        if sys.version_info[0] >= 3:
            assert any('1 / 0' in line
                      for line in pluck(3, traceback.extract_tb(
                          loads(response['traceback'])))
                      if line)

        aa.close_streams()
        yield a._close()

        assert a.address not in c.ncores and b.address in c.ncores

        assert list(c.ncores.keys()) == [b.address]

        assert isinstance(b.address, str)
        assert b.ip in b.address
        assert str(b.port) in b.address

        bb.close_streams()
        yield b._close()

    _test_cluster(f)


def test_compute_who_has(loop):
    @gen.coroutine
    def f():
        c = Center(ip='127.0.0.1')
        c.listen(0)
        x = Worker(c.ip, c.port, ip='127.0.0.1')
        y = Worker(c.ip, c.port, ip='127.0.0.1')
        z = Worker(c.ip, c.port, ip='127.0.0.1')
        x.data['a'] = 1
        y.data['a'] = 2
        yield [x._start(), y._start(), z._start()]

        zz = rpc(ip=z.ip, port=z.port)
        yield zz.compute(function=dumps(inc),
                         args=dumps(('a',)),
                         who_has={'a': [x.address]},
                         key='b')
        assert z.data['b'] == 2

        if 'a' in z.data:
            del z.data['a']
        yield zz.compute(function=dumps(inc),
                         args=dumps(('a',)),
                         who_has={'a': [y.address]},
                         key='c')
        assert z.data['c'] == 3

        yield [x._close(), y._close(), z._close()]
        zz.close_streams()

    loop.run_sync(f, timeout=5)


def test_workers_update_center(loop):
    @gen.coroutine
    def f(c, a, b):
        aa = rpc(ip=a.ip, port=a.port)

        response = yield aa.update_data(data={'x': dumps(1), 'y': dumps(2)})
        assert response['status'] == 'OK'
        assert response['nbytes'] == {'x': sizeof(1), 'y': sizeof(2)}

        assert a.data == {'x': 1, 'y': 2}
        assert c.who_has == {'x': {a.address},
                             'y': {a.address}}
        assert c.has_what[a.address] == {'x', 'y'}

        yield aa.delete_data(keys=['x'], close=True)
        assert not c.who_has['x']
        assert all('x' not in s for s in c.has_what.values())

        aa.close_streams()

    _test_cluster(f)


@slow
def dont_test_delete_data_with_missing_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        bad = '127.0.0.1:9001'  # this worker doesn't exist
        c.who_has['z'].add(bad)
        c.who_has['z'].add(a.address)
        c.has_what[bad].add('z')
        c.has_what[a.address].add('z')
        a.data['z'] = 5

        cc = rpc(ip=c.ip, port=c.port)

        yield cc.delete_data(keys=['z'])  # TODO: this hangs for a while
        assert 'z' not in a.data
        assert not c.who_has['z']
        assert not c.has_what[bad]
        assert not c.has_what[a.address]

        cc.close_streams()

    _test_cluster(f)


def test_upload_file(loop):
    @gen.coroutine
    def f(c, a, b):
        assert not os.path.exists(os.path.join(a.local_dir, 'foobar.py'))
        assert not os.path.exists(os.path.join(b.local_dir, 'foobar.py'))
        assert a.local_dir != b.local_dir

        aa = rpc(ip=a.ip, port=a.port)
        bb = rpc(ip=b.ip, port=b.port)
        yield [aa.upload_file(filename='foobar.py', data=b'x = 123'),
               bb.upload_file(filename='foobar.py', data='x = 123')]

        assert os.path.exists(os.path.join(a.local_dir, 'foobar.py'))
        assert os.path.exists(os.path.join(b.local_dir, 'foobar.py'))

        def g():
            import foobar
            return foobar.x

        yield aa.compute(function=dumps(g),
                         key='x')
        result = yield aa.get_data(keys=['x'])
        assert result == {'x': dumps(123)}

        yield a._close()
        yield b._close()
        aa.close_streams()
        bb.close_streams()
        assert not os.path.exists(os.path.join(a.local_dir, 'foobar.py'))

    _test_cluster(f)


def test_upload_egg(loop):
    @gen.coroutine
    def f(c, a, b):
        eggname = 'mytestegg-1.0.0-py3.4.egg'
        local_file = __file__.replace('test_worker.py', eggname)
        assert not os.path.exists(os.path.join(a.local_dir, eggname))
        assert not os.path.exists(os.path.join(b.local_dir, eggname))
        assert a.local_dir != b.local_dir

        aa = rpc(ip=a.ip, port=a.port)
        bb = rpc(ip=b.ip, port=b.port)
        with open(local_file, 'rb') as f:
            payload = f.read()
        yield [aa.upload_file(filename=eggname, data=payload),
               bb.upload_file(filename=eggname, data=payload)]

        assert os.path.exists(os.path.join(a.local_dir, eggname))
        assert os.path.exists(os.path.join(b.local_dir, eggname))

        def g(x):
            import testegg
            return testegg.inc(x)

        yield aa.compute(function=dumps(g), key='x', args=dumps((10,)))
        result = yield aa.get_data(keys=['x'])
        assert result == {'x': dumps(10 + 1)}

        yield a._close()
        yield b._close()
        aa.close_streams()
        bb.close_streams()
        assert not os.path.exists(os.path.join(a.local_dir, eggname))

    _test_cluster(f)


def test_broadcast(loop):
    @gen.coroutine
    def f(c, a, b):
        cc = rpc(ip=c.ip, port=c.port)
        results = yield cc.broadcast(msg={'op': 'ping'})
        assert results == {a.address: b'pong', b.address: b'pong'}

        cc.close_streams()

    _test_cluster(f)


def test_worker_with_port_zero(loop):
    @gen.coroutine
    def f():
        c = Center('127.0.0.1')
        c.listen(8007)
        w = Worker(c.ip, c.port, ip='127.0.0.1')
        yield w._start()
        assert isinstance(w.port, int)
        assert w.port > 1024

    loop.run_sync(f)

@pytest.mark.slow
def test_worker_waits_for_center_to_come_up(loop):
    @gen.coroutine
    def f():
        w = Worker('127.0.0.1', 8007, ip='127.0.0.1')
        yield w._start()

    try:
        loop.run_sync(f, timeout=4)
    except TimeoutError:
        pass


@gen_cluster()
def test_worker_task(s, a, b):
    aa = rpc(ip=a.ip, port=a.port)
    yield aa.compute(task=dumps((inc, 1)), key='x', report=False)

    assert a.data['x'] == 2


@gen_cluster()
def test_worker_task_data(s, a, b):
    aa = rpc(ip=a.ip, port=a.port)
    yield aa.compute(task=dumps(2), key='x', report=False)

    assert a.data['x'] == 2


@gen_cluster()
def test_worker_task_bytes(s, a, b):
    aa = rpc(ip=a.ip, port=a.port)

    yield aa.compute(task=dumps((inc, 1)), key='x', report=False)
    assert a.data['x'] == 2

    yield aa.compute(function=dumps(inc), args=dumps((10,)), key='y',
            report=False)
    assert a.data['y'] == 11


def test_error_message():
    class MyException(Exception):
        def __init__(self, a, b):
            self.args = (a + b,)
        def __str__(self):
            return "MyException(%s)" % self.args

    msg = error_message(MyException('Hello', 'World!'))
    assert 'Hello' in str(msg['exception'])


@gen_cluster()
def test_gather(s, a, b):
    b.data['x'] = 1
    b.data['y'] = 2
    aa = rpc(ip=a.ip, port=a.port)
    resp = yield aa.gather(who_has={'x': [b.address], 'y': [b.address]})
    assert resp['status'] == 'OK'

    assert a.data['x'] == b.data['x']
    assert a.data['y'] == b.data['y']


@gen_cluster()
def test_compute_stream(s, a, b):
    stream = yield connect(a.ip, a.port)
    yield write(stream, {'op': 'compute-stream'})
    msgs = [{'op': 'compute-task', 'function': dumps(inc), 'args': dumps((i,)), 'key': 'x-%d' % i}
            for i in range(10)]

    bstream = BatchedStream(stream, 0)
    for msg in msgs[:5]:
        yield write(stream, msg)

    for i in range(5):
        msg = yield read(bstream)
        assert msg['status'] == 'OK'
        assert msg['key'][0] == 'x'

    for msg in msgs[5:]:
        yield write(stream, msg)

    for i in range(5):
        msg = yield read(bstream)
        assert msg['status'] == 'OK'
        assert msg['key'][0] == 'x'

    yield write(stream, {'op': 'close'})

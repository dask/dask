from operator import add
import os
import shutil
import sys
from time import sleep

from distributed.center import Center
from distributed.core import read, write, rpc, connect
from distributed.sizeof import sizeof
from distributed.utils import ignoring
from distributed.worker import Worker
from distributed.utils_test import loop, _test_cluster

from tornado import gen
from tornado.ioloop import IOLoop


def test_worker_ncores():
    from distributed.worker import _ncores
    w = Worker('127.0.0.1', 8018, '127.0.0.1', 8019)
    try:
        assert w.executor._max_workers == _ncores
    finally:
        shutil.rmtree(w.local_dir)

def test_identity():
    w = Worker('127.0.0.1', 8018, '127.0.0.1', 8019)
    ident = w.identity(None)
    assert ident['type'] == 'Worker'
    assert ident['center'] == ('127.0.0.1', 8019)


def test_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        aa = rpc(ip=a.ip, port=a.port)
        bb = rpc(ip=b.ip, port=b.port)

        response, _ = yield aa.compute(key='x', function=add,
                                       args=[1, 2], needed=[],
                                       close=True)
        assert response == b'OK'
        assert a.data['x'] == 3
        assert c.who_has['x'] == set([(a.ip, a.port)])

        response, info = yield bb.compute(key='y', function=add,
                                          args=['x', 10], needed=['x'])
        assert response == b'OK'
        assert b.data['y'] == 13
        assert c.who_has['y'] == set([(b.ip, b.port)])
        assert info['nbytes'] == sizeof(b.data['y'])

        def bad_func():
            1 / 0

        response, (error, traceback) = yield bb.compute(key='z',
                function=bad_func, args=(), needed=(), close=True)
        assert response == b'error'
        assert isinstance(error, ZeroDivisionError)
        if sys.version_info[0] >= 3:
            assert any('1 / 0' in line for line in traceback)

        aa.close_streams()
        yield a._close()

        assert a.address not in c.ncores and b.address in c.ncores

        assert list(c.ncores.keys()) == [(b.ip, b.port)]

        bb.close_streams()
        yield b._close()

    _test_cluster(f)


def test_workers_update_center(loop):
    @gen.coroutine
    def f(c, a, b):
        aa = rpc(ip=a.ip, port=a.port)

        response, content = yield aa.update_data(data={'x': 1, 'y': 2})
        assert response == b'OK'
        assert content['nbytes'] == {'x': sizeof(1), 'y': sizeof(2)}

        assert a.data == {'x': 1, 'y': 2}
        assert c.who_has == {'x': {(a.ip, a.port)},
                             'y': {(a.ip, a.port)}}
        assert c.has_what[(a.ip, a.port)] == {'x', 'y'}

        yield aa.delete_data(keys=['x'], close=True)
        assert not c.who_has['x']
        assert all('x' not in s for s in c.has_what.values())

        aa.close_streams()

    _test_cluster(f)


def test_delete_data_with_missing_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        bad = ('127.0.0.1', 9001)  # this worker doesn't exist
        c.who_has['z'].add(bad)
        c.who_has['z'].add(a.address)
        c.has_what[bad].add('z')
        c.has_what[a.address].add('z')
        a.data['z'] = 5

        cc = rpc(ip=c.ip, port=c.port)

        yield cc.delete_data(keys=['z'])
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
               bb.upload_file(filename='foobar.py', data=b'x = 123')]

        assert os.path.exists(os.path.join(a.local_dir, 'foobar.py'))
        assert os.path.exists(os.path.join(b.local_dir, 'foobar.py'))

        def g():
            import foobar
            return foobar.x

        yield aa.compute(function=g, key='x')
        result = yield aa.get_data(keys=['x'])
        assert result == {'x': 123}

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

        yield aa.compute(function=g, key='x', args=(10,))
        result = yield aa.get_data(keys=['x'])
        assert result == {'x': 10 + 1}

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
"""

def test_close():
    c = Center('127.0.0.1', 8007, loop=loop)
    a = Worker('127.0.0.1', 8008, c.ip, c.port, loop=loop)

    @asyncio.coroutine
    def f():
        while len(c.ncores) < 1:
            yield from asyncio.sleep(0.01, loop=loop)

        assert a.status == 'running'
        yield from rpc(a.ip, a.port, loop=loop).terminate()
        assert a.status == 'closed'

        assert c.status == 'running'
        yield from rpc(c.ip, c.port, loop=loop).terminate()
        assert c.status == 'closed'

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), f(), loop=loop))
"""

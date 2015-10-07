from operator import add
from time import sleep

from distributed.core import read, write, rpc, connect
from distributed.utils import ignoring
from distributed.center import Center
from distributed.worker import Worker


from tornado import gen
from tornado.ioloop import IOLoop


def _test_cluster(f):
    @gen.coroutine
    def g():
        c = Center('127.0.0.1', 8017)
        c.listen(c.port)
        a = Worker('127.0.0.1', 8018, c.ip, c.port, ncores=1)
        yield a._start()
        b = Worker('127.0.0.1', 8019, c.ip, c.port, ncores=1)
        yield b._start()

        while len(c.ncores) < 2:
            yield gen.sleep(0.01)

        try:
            yield f(c, a, b)
        finally:
            with ignoring(Exception):
                yield a._close()
            with ignoring(Exception):
                yield b._close()
            c.stop()

    IOLoop.current().run_sync(g)


def test_worker():
    @gen.coroutine
    def f(c, a, b):
        aa = rpc(ip=a.ip, port=a.port)
        bb = rpc(ip=b.ip, port=b.port)

        response = yield aa.compute(key='x', function=add,
                                    args=[1, 2], needed=[],
                                    close=True)
        assert response == b'OK'
        assert a.data['x'] == 3
        assert c.who_has['x'] == set([(a.ip, a.port)])

        response = yield bb.compute(key='y', function=add,
                                    args=['x', 10], needed=['x'])
        assert response == b'OK'
        assert b.data['y'] == 13
        assert c.who_has['y'] == set([(b.ip, b.port)])

        def bad_func():
            1 / 0

        response = yield bb.compute(key='z', function=bad_func,
                                    args=(), needed=(), close=True)
        assert response == b'error'
        assert isinstance(b.data['z'], ZeroDivisionError)

        aa.close_streams()
        yield a._close()

        assert a.address not in c.ncores and b.address in c.ncores

        assert list(c.ncores.keys()) == [(b.ip, b.port)]

        bb.close_streams()
        yield b._close()

    _test_cluster(f)


def test_workers_update_center():
    @gen.coroutine
    def f(c, a, b):
        aa = rpc(ip=a.ip, port=a.port)

        yield aa.update_data(data={'x': 1, 'y': 2})

        assert a.data == {'x': 1, 'y': 2}
        assert c.who_has == {'x': {(a.ip, a.port)},
                             'y': {(a.ip, a.port)}}
        assert c.has_what[(a.ip, a.port)] == {'x', 'y'}

        yield aa.delete_data(keys=['x'], close=True)
        assert not c.who_has['x']
        assert all('x' not in s for s in c.has_what.values())

        aa.close_streams()

    _test_cluster(f)

def test_delete_data_with_missing_worker():
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

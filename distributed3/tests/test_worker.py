from operator import add
from time import sleep

from distributed3.core import read, write, rpc, connect
from distributed3.utils import ignoring
from distributed3.center import Center
from distributed3.worker import Worker


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
            with ignoring():
                yield a._close()
            with ignoring():
                yield b._close()
            c.stop()

    IOLoop.current().run_sync(g)


def test_worker():
    @gen.coroutine
    def f(c, a, b):
        a_stream = yield connect(a.ip, a.port)
        b_stream = yield connect(b.ip, b.port)

        response = yield rpc(a_stream).compute(key='x', function=add,
                                               args=[1, 2], needed=[],
                                               close=True)
        assert response == b'success'
        assert a.data['x'] == 3
        assert c.who_has['x'] == set([(a.ip, a.port)])

        response = yield rpc(b_stream).compute(key='y', function=add,
                                               args=['x', 10], needed=['x'])
        assert response == b'success'
        assert b.data['y'] == 13
        assert c.who_has['y'] == set([(b.ip, b.port)])

        def bad_func():
            1 / 0

        response = yield rpc(b_stream).compute(key='z', function=bad_func,
                                               args=(), needed=(), close=True)
        assert response == b'error'
        assert isinstance(b.data['z'], ZeroDivisionError)

        a_stream.close()
        yield a._close()

        assert a.address not in c.ncores and b.address in c.ncores

        assert list(c.ncores.keys()) == [(b.ip, b.port)]

        b_stream.close()
        yield b._close()

    _test_cluster(f)



"""

def test_workers_update_center():
    c = Center('127.0.0.1', 8007, loop=loop)
    a = Worker('127.0.0.1', 8008, c.ip, c.port, loop=loop)

    @asyncio.coroutine
    def f():
        a_reader, a_writer = yield from connect(a.ip, a.port, loop=loop)

        yield from rpc(a_reader, a_writer).update_data(data={'x': 1, 'y': 2})

        assert a.data == {'x': 1, 'y': 2}
        assert c.who_has == {'x': {(a.ip, a.port)},
                             'y': {(a.ip, a.port)}}
        assert c.has_what == {(a.ip, a.port): {'x', 'y'}}

        yield from rpc(a_reader, a_writer).delete_data(keys=['x'], close=True)

        yield from a._close()
        yield from c._close()

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), f(), loop=loop))


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

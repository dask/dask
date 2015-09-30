from contextlib import contextmanager
from multiprocessing import Process
from operator import add, mul
from time import time
from toolz import merge

import dask
from distributed3 import Center, Worker
from distributed3.utils import ignoring
from distributed3.client import gather_from_center
from distributed3.dask import _get

from tornado import gen
from tornado.ioloop import IOLoop


def inc(x):
    return x + 1


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


def test_scheduler():
    dsk = {'x': 1, 'y': (add, 'x', 10), 'z': (add, (inc, 'x'), 20),
           'a': 1, 'b': (mul, 'a', 10), 'c': (mul, 'a', 20),
           'total': (add, 'c', 'z')}
    keys = ['total', 'c', ['z']]

    @gen.coroutine
    def f(c, a, b):
        result = yield _get(c.ip, c.port, dsk, keys)
        result2 = yield gather_from_center((c.ip, c.port), result)

        expected = dask.async.get_sync(dsk, keys)
        assert tuple(result2) == expected
        assert set(a.data) | set(b.data) == {'total', 'c', 'z'}

    _test_cluster(f)


def test_scheduler_errors():
    def mydiv(x, y):
        return x / y
    dsk = {'x': 1, 'y': (mydiv, 'x', 0)}
    keys = 'y'

    @gen.coroutine
    def f(c, a, b):
        try:
            yield _get(c.ip, c.port, dsk, keys)
            assert False
        except ZeroDivisionError as e:
            # assert 'mydiv' in str(e)
            pass

    _test_cluster(f)


def test_gather():
    dsk = {'x': 1, 'y': (inc, 'x')}
    keys = 'y'

    @gen.coroutine
    def f(c, a, b):
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)
        assert result == 2

    _test_cluster(f)

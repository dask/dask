from operator import add, mul
from time import sleep
from threading import Thread

import dask

from distributed import Center, Worker
from distributed.utils import ignoring
from distributed.client import RemoteData
from distributed.dask import _get
from distributed.utils_test import cluster, inc

from tornado import gen
from tornado.ioloop import IOLoop


def _test_cluster(f):
    @gen.coroutine
    def g():
        c = Center('127.0.0.1', 8017)
        c.listen(c.port)
        a = Worker('127.0.0.1', 8018, c.ip, c.port, ncores=2)
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
    dsk = {'x': 1, 'y': (add, 'x', 10), 'z': (add, (inc, 'y'), 20),
           'a': 1, 'b': (mul, 'a', 10), 'c': (mul, 'b', 20),
           'total': (add, 'c', 'z')}
    keys = ['total', 'c', ['z']]

    @gen.coroutine
    def f(c, a, b):
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)

        expected = dask.async.get_sync(dsk, keys)
        assert tuple(result) == expected
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
            result = yield _get(c.ip, c.port, dsk, keys)
            assert False
        except ZeroDivisionError as e:
            # assert 'mydiv' in str(e)
            pass

    _test_cluster(f)


def test_avoid_computations_for_data_in_memory():
    def bad():
        raise Exception()
    dsk = {'x': (bad,), 'y': (inc, 'x'), 'z': (inc, 'y')}
    keys = 'z'

    @gen.coroutine
    def f(c, a, b):
        a.data['y'] = 10                # manually add 'y' to a
        c.who_has['y'].add(a.address)
        c.has_what[a.address].add('y')

        result = yield _get(c.ip, c.port, dsk, keys)
        assert result.key in a.data or result.key in b.data

    _test_cluster(f)


def test_gather():
    dsk = {'x': 1, 'y': (inc, 'x')}
    keys = 'y'

    @gen.coroutine
    def f(c, a, b):
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)
        assert result == 2

    _test_cluster(f)


def slowinc(x):
    from time import sleep
    sleep(0.02)
    return x + 1


def dont_test_failing_worker():
    n = 10
    dsk = {('x', i, j): (slowinc, ('x', i, j - 1)) for i in range(4)
                                                   for j in range(1, n)}
    dsk.update({('x', i, 0): i * 10 for i in range(4)})
    dsk['z'] = (sum, [('x', i, n - 1) for i in range(4)])
    keys = 'z'

    with cluster() as (c, [a, b]):
        def kill_a():
            sleep(0.1)
            a['proc'].terminate()

        @gen.coroutine
        def f():
            result = yield _get('127.0.0.1', c['port'], dsk, keys, gather=True)
            assert result == 96

        thread = Thread(target=kill_a)
        thread.start()
        IOLoop.current().run_sync(f)


def test_repeated_computation():
    def func():
        from random import randint
        return randint(0, 100)

    dsk = {'x': (func,)}

    @gen.coroutine
    def f(c, a, b):
        x = yield _get(c.ip, c.port, dsk, 'x', gather=True)
        y = yield _get(c.ip, c.port, dsk, 'x', gather=True)
        assert x == y

    _test_cluster(f)


def test_RemoteData_interaction():
    @gen.coroutine
    def f(c, a, b):
        a.data['x'] = 10
        c.who_has['x'].add(a.address)
        c.has_what[a.address].add('x')
        x = RemoteData('x', c.ip, c.port)

        dsk = {'y': (inc, x)}

        result = yield _get(c.ip, c.port, dsk, 'y', gather=True)
        assert result == 11
        assert 'x' in a.data  # don't delete input data

    _test_cluster(f)


def test_get_with_overlapping_keys():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    keys = 'y'

    @gen.coroutine
    def f(c, a, b):
        dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
        keys = 'z'
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)
        assert result == dask.get(dsk, keys)

        dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y'),
               'a': (inc, 'z'), 'b': (add, 'a', 'x')}
        keys = 'b'
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)
        assert result == dask.get(dsk, keys)

    _test_cluster(f)

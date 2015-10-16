from operator import add

import pytest
from toolz import isdistinct
from tornado.ioloop import IOLoop
from tornado import gen

from distributed.executor import Executor, Future
from distributed import Center, Worker
from distributed.utils import ignoring
from distributed.utils_test import cluster


def inc(x):
    return x + 1


def div(x, y):
    return x / y


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


def test_submit():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)
        x = e.submit(inc, 10)
        assert not x.done()

        assert isinstance(x, Future)
        assert x.executor is e
        result = yield x._result()
        assert result == 11
        assert x.done()

        y = e.submit(inc, 20)
        z = e.submit(add, x, y)
        result = yield z._result()
        assert result == 11 + 21
        yield e._shutdown()
        assert c.who_has[z.key]

    _test_cluster(f)


def test_map():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)

        L1 = e.map(inc, range(5))
        assert len(L1) == 5
        assert isdistinct(x.key for x in L1)
        assert all(isinstance(x, Future) for x in L1)

        result = yield L1[0]._result()
        assert result == inc(0)
        assert len(e.dask) == 5

        L2 = e.map(inc, L1)

        result = yield L2[1]._result()
        assert result == inc(inc(1))
        assert len(e.dask) == 10
        assert L1[0].key in e.dask[L2[0].key]

        total = e.submit(sum, L2)
        result = yield total._result()
        assert result == sum(map(inc, map(inc, range(5))))

        yield e._shutdown()

    _test_cluster(f)


def test_map_naming():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))

        L1 = e.map(inc, range(5))
        L2 = e.map(inc, range(5))

        assert [x.key for x in L1] == [x.key for x in L2]

        L3 = e.map(inc, [1, 1, 1, 1])
        assert len({x.event for x in L3}) == 1

        L4 = e.map(inc, [1, 1, 1, 1], pure=False)
        assert len({x.event for x in L4}) == 4

    _test_cluster(f)


def test_submit_naming():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)

        assert a.event is b.event

        c = e.submit(inc, 1, pure=False)
        assert c.key != a.key

    _test_cluster(f)


def test_exceptions():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)

        x = e.submit(div, 1, 2)
        result = yield x._result()
        assert result == 1 / 2

        x = e.submit(div, 1, 0)
        with pytest.raises(ZeroDivisionError):
            result = yield x._result()

        x = e.submit(div, 10, 2)  # continues to operate
        result = yield x._result()
        assert result == 10 / 2

        yield e._shutdown()

    _test_cluster(f)


def test_gc():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)
        x = e.submit(inc, 10)
        result = yield x._result()

        assert c.who_has[x.key]

        x.__del__()

        yield e._shutdown()

        assert not c.who_has[x.key]

    _test_cluster(f)


def test_thread():
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            x = e.submit(inc, 1)
            assert x.result() == 2


def test_sync_exceptions():
    with cluster() as (c, [a, b]):
        e = Executor(('127.0.0.1', c['port']), start=True)

        x = e.submit(div, 10, 2)
        assert x.result() == 5

        y = e.submit(div, 10, 0)
        try:
            y.result()
            assert False
        except ZeroDivisionError:
            pass

        z = e.submit(div, 10, 5)
        assert z.result() == 2

        e.shutdown()


def test_stress_1():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)

        n = 2**6

        seq = e.map(inc, range(n))
        while len(seq) > 1:
            yield gen.sleep(0.1)
            seq = [e.submit(add, seq[i], seq[i + 1])
                    for i in range(0, len(seq), 2)]
        result = yield seq[0]._result()
        assert result == sum(map(inc, range(n)))

        yield e._shutdown()

    _test_cluster(f)


def test_gather():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)
        x = e.submit(inc, 10)
        y = e.submit(inc, x)

        result = yield e._gather(x)
        assert result == 11
        result = yield e._gather([x])
        assert result == [11]
        result = yield e._gather({'x': x, 'y': [y]})
        assert result == {'x': 11, 'y': [12]}

        yield e._shutdown()

    _test_cluster(f)


def test_gather_sync():
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            x = e.submit(inc, 1)
            assert e.gather(x) == 2


def test_get():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)
        result = yield e._get({'x': (inc, 1)}, 'x')
        assert result == 2

        result = yield e._get({'x': (inc, 1)}, ['x'])
        assert result == [2]

        yield e._shutdown()

    _test_cluster(f)


def test_get_sync():
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            assert e.get({'x': (inc, 1)}, 'x') == 2

from operator import add

from collections import Iterator
import pytest
import sys
from toolz import isdistinct
from tornado.ioloop import IOLoop
from tornado import gen

from distributed.executor import (Executor, Future, _wait, wait, _as_completed,
        as_completed)
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
        e = Executor((c.ip, c.port), start=False)
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
        e = Executor((c.ip, c.port), start=False)
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

        L3 = e.map(add, L1, L2)
        result = yield L3[1]._result()
        assert result == inc(1) + inc(inc(1))

        L4 = e.map(add, range(3), range(4))
        results = yield e._gather(L4)
        if sys.version_info[0] >= 3:
            assert results == list(map(add, range(3), range(4)))

        yield e._shutdown()

    _test_cluster(f)


def test_future():
    e = Executor('127.0.0.1:8787', start=False)
    x = e.submit(inc, 10)
    assert str(x.key) in repr(x)
    assert str(x.status) in repr(x)


def test_map_naming():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False)

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
        e = Executor((c.ip, c.port), start=False)

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)

        assert a.event is b.event

        c = e.submit(inc, 1, pure=False)
        assert c.key != a.key

    _test_cluster(f)


def test_exceptions():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False)
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
        e = Executor((c.ip, c.port), start=False)
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
        e = Executor((c.ip, c.port), start=False)
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
        e = Executor((c.ip, c.port), start=False)
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
        e = Executor((c.ip, c.port), start=False)
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


def test_submit_errors():
    def f(a, b, c):
        pass

    e = Executor('127.0.0.1:8787', start=False)

    with pytest.raises(TypeError):
        e.submit(1, 2, 3)
    with pytest.raises(TypeError):
        e.map([1, 2, 3])


def test_wait():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False)
        IOLoop.current().spawn_callback(e._go)

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)
        c = e.submit(inc, 2)

        done, not_done = yield _wait([a, b, c])

        assert done == {a, b, c}
        assert not_done == set()
        assert a.status == b.status == 'finished'

        yield e._shutdown()

    _test_cluster(f)


def test__as_completed():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False)
        IOLoop.current().spawn_callback(e._go)

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)
        c = e.submit(inc, 2)

        from distributed.compatibility import Queue
        queue = Queue()
        yield _as_completed([a, b, c], queue)

        assert queue.qsize() == 3
        assert {queue.get(), queue.get(), queue.get()} == {a, b, c}

        yield e._shutdown()

    _test_cluster(f)


def test_as_completed():
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            x = e.submit(inc, 1)
            y = e.submit(inc, 2)
            z = e.submit(inc, 1)

            seq = as_completed([x, y, z])
            assert isinstance(seq, Iterator)
            assert set(seq) == {x, y, z}


def test_wait_sync():
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            x = e.submit(inc, 1)
            y = e.submit(inc, 2)

            done, not_done = wait([x, y])
            assert done == {x, y}
            assert not_done == set()
            assert x.status == y.status == 'finished'


def test_garbage_collection():
    import gc
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False)

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)

        assert e.refcount[a.key] == 2
        a.__del__()
        assert e.refcount[a.key] == 1

        c = e.submit(inc, b)
        b.__del__()

        IOLoop.current().spawn_callback(e._go)

        result = yield c._result()
        assert result == 3

        bkey = b.key
        b.__del__()
        assert bkey not in e.futures

    _test_cluster(f)


def test_recompute_released_key():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), delete_batch_time=0, start=False)
        IOLoop.current().spawn_callback(e._go)

        x = e.submit(inc, 100)
        result1 = yield x._result()
        xkey = x.key
        del x
        import gc; gc.collect()
        assert e.refcount[xkey] == 0

        # 1 second batching needs a second action to trigger
        while xkey in c.who_has or xkey in a.data or xkey in b.data:
            yield gen.sleep(0.1)

        x = e.submit(inc, 100)
        assert x.key in e.futures
        result2 = yield x._result()
        assert result1 == result2

    _test_cluster(f)


def test_stress_gc():
    def slowinc(x):
        from time import sleep
        sleep(0.02)
        return x + 1

    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), delete_batch_time=0.5) as e:
            x = e.submit(slowinc, 1)
            for i in range(10):  # this could be increased
                x = e.submit(slowinc, x)

            assert x.result() == 12


def test_missing_data_heals():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), delete_batch_time=0, start=False)
        IOLoop.current().spawn_callback(e._go)

        x = e.submit(inc, 1)
        y = e.submit(inc, x)
        z = e.submit(inc, y)

        yield _wait([x, y, z])

        # Secretly delete y's key
        if y.key in a.data:
            del a.data[y.key]
        if y.key in b.data:
            del b.data[y.key]

        w = e.submit(add, y, z)

        result = yield w._result()
        assert result == 3 + 4

    _test_cluster(f)

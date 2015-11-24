from operator import add, sub

from collections import Iterator
from concurrent.futures import CancelledError
from time import sleep, time
import sys

import pytest
from toolz import identity, isdistinct, first
from tornado.ioloop import IOLoop
from tornado import gen

from distributed import Center, Worker
from distributed.client import WrappedKey
from distributed.executor import (Executor, Future, _wait, wait, _as_completed,
        as_completed, tokenize, _global_executors, default_executor)
from distributed.sizeof import sizeof
from distributed.utils import ignoring, sync
from distributed.utils_test import cluster, slow, _test_cluster, loop


def inc(x):
    return x + 1


def div(x, y):
    return x / y


def throws(x):
    raise Exception()


def test_submit(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

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

    _test_cluster(f, loop)


def test_map(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

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

        def f(x, y=10):
            return x + y

        L5 = e.map(f, range(5), y=5)
        results = yield e._gather(L5)
        assert results == list(range(5, 10))

        y = e.submit(f, 10)
        L6 = e.map(f, range(5), y=y)
        results = yield e._gather(L6)
        assert results == list(range(20, 25))

        yield e._shutdown()
    _test_cluster(f, loop)


def test_future(loop):
    e = Executor('127.0.0.1:8787', start=False, loop=loop)
    x = e.submit(inc, 10)
    assert str(x.key) in repr(x)
    assert str(x.status) in repr(x)


def test_Future_exception(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(div, 1, 0)
        result = yield x._exception()
        assert isinstance(result, ZeroDivisionError)

        x = e.submit(div, 1, 1)
        result = yield x._exception()
        assert result is None
        yield e._shutdown()
    _test_cluster(f, loop)


def test_Future_exception_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            x = e.submit(div, 1, 0)
            assert isinstance(x.exception(), ZeroDivisionError)

            x = e.submit(div, 1, 1)
            assert x.exception() is None


def test_map_naming(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)

        L1 = e.map(inc, range(5))
        L2 = e.map(inc, range(5))

        assert [x.key for x in L1] == [x.key for x in L2]

        L3 = e.map(inc, [1, 1, 1, 1])
        assert len({x.event for x in L3}) == 1

        L4 = e.map(inc, [1, 1, 1, 1], pure=False)
        assert len({x.event for x in L4}) == 4

    _test_cluster(f, loop)


def test_submit_naming(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)

        assert a.event is b.event

        c = e.submit(inc, 1, pure=False)
        assert c.key != a.key
    _test_cluster(f, loop)


def test_exceptions(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

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
    _test_cluster(f, loop)


def test_gc(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(inc, 10)
        result = yield x._result()

        assert c.who_has[x.key]

        x.__del__()

        yield e._shutdown()

        assert not c.who_has[x.key]
        yield e._shutdown()
    _test_cluster(f, loop)


def test_thread(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            x = e.submit(inc, 1)
            assert x.result() == 2


def test_sync_exceptions(loop):
    with cluster() as (c, [a, b]):
        e = Executor(('127.0.0.1', c['port']), start=True, loop=loop)

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


def test_stress_1(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

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


def test_gather(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(inc, 10)
        y = e.submit(inc, x)

        result = yield e._gather(x)
        assert result == 11
        result = yield e._gather([x])
        assert result == [11]
        result = yield e._gather({'x': x, 'y': [y]})
        assert result == {'x': 11, 'y': [12]}

        yield e._shutdown()

    _test_cluster(f, loop)


def test_gather_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            x = e.submit(inc, 1)
            assert e.gather(x) == 2


def test_get(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        result = yield e._get({'x': (inc, 1)}, 'x')
        assert result == 2

        result = yield e._get({'x': (inc, 1)}, ['x'])
        assert result == [2]

        result = yield e._get({}, [])
        assert result == []

        yield e._shutdown()

    _test_cluster(f, loop)


def test_get_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            assert e.get({'x': (inc, 1)}, 'x') == 2


def test_submit_errors(loop):
    def f(a, b, c):
        pass

    e = Executor('127.0.0.1:8787', start=False, loop=loop)

    with pytest.raises(TypeError):
        e.submit(1, 2, 3)
    with pytest.raises(TypeError):
        e.map([1, 2, 3])


def test_wait(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)
        c = e.submit(inc, 2)

        done, not_done = yield _wait([a, b, c])

        assert done == {a, b, c}
        assert not_done == set()
        assert a.status == b.status == 'finished'

        yield e._shutdown()

    _test_cluster(f, loop)


def test__as_completed(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)
        c = e.submit(inc, 2)

        from distributed.compatibility import Queue
        queue = Queue()
        yield _as_completed([a, b, c], queue)

        assert queue.qsize() == 3
        assert {queue.get(), queue.get(), queue.get()} == {a, b, c}

        yield e._shutdown()

    _test_cluster(f, loop)


def test_as_completed(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            x = e.submit(inc, 1)
            y = e.submit(inc, 2)
            z = e.submit(inc, 1)

            seq = as_completed([x, y, z])
            assert isinstance(seq, Iterator)
            assert set(seq) == {x, y, z}


def test_wait_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            x = e.submit(inc, 1)
            y = e.submit(inc, 2)

            done, not_done = wait([x, y])
            assert done == {x, y}
            assert not_done == set()
            assert x.status == y.status == 'finished'


def test_garbage_collection(loop):
    import gc
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)

        a = e.submit(inc, 1)
        b = e.submit(inc, 1)

        assert e.refcount[a.key] == 2
        a.__del__()
        assert e.refcount[a.key] == 1

        c = e.submit(inc, b)
        b.__del__()

        yield e._start()

        result = yield c._result()
        assert result == 3

        bkey = b.key
        b.__del__()
        assert bkey not in e.futures
        yield e._shutdown()

    _test_cluster(f)


def test_garbage_collection_with_scatter(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), delete_batch_time=0, start=False,
                loop=loop)
        yield e._start()

        [a] = yield e._scatter([1])
        assert a.key in e.futures
        assert a.status == 'finished'
        assert a.event.is_set()

        assert e.refcount[a.key] == 1
        a.__del__()
        assert e.refcount[a.key] == 0

        start = time()
        while True:
            if a.key not in c.who_has:
                break
            else:
                assert time() < start + 3
                yield gen.sleep(0.1)

        yield e._shutdown()
    _test_cluster(f)


def test_recompute_released_key(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), delete_batch_time=0, start=False,
                loop=loop)
        yield e._start()

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
        yield e._shutdown()

    _test_cluster(f, loop)

def slowinc(x):
    from time import sleep
    sleep(0.02)
    return x + 1


def test_stress_gc(loop):
    n = 100
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), delete_batch_time=0.5, loop=loop) as e:
            x = e.submit(slowinc, 1)
            for i in range(n):
                x = e.submit(slowinc, x)

            assert x.result() == n + 2


@slow
def test_long_tasks_dont_trigger_timeout(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), delete_batch_time=0, start=False,
                loop=loop)
        yield e._start()

        from time import sleep
        x = e.submit(sleep, 3)
        yield x._result()

        yield e._shutdown()
    _test_cluster(f, loop)


def test_missing_data_heals(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), delete_batch_time=0, start=False,
                loop=loop)
        yield e._start()

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
        yield e._shutdown()

    _test_cluster(f, loop)


@slow
def test_missing_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        bad = ('bad-host', 8788)
        c.ncores[bad] = 4
        c.who_has['b'] = {bad}
        c.has_what[bad] = {'b'}

        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}

        result = yield e._get(dsk, 'c')
        assert result == 3
        assert bad not in e.ncores

        yield e._shutdown()

    _test_cluster(f, loop)


def test_gather_robust_to_missing_data(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x, y, z = e.map(inc, range(3))
        yield _wait([x, y, z])  # everything computed

        for q in [x, y]:
            if q.key in a.data:
                del a.data[q.key]
            if q.key in b.data:
                del b.data[q.key]

        xx, yy, zz = yield e._gather([x, y, z])
        assert (xx, yy, zz) == (1, 2, 3)

        yield e._shutdown()

    _test_cluster(f, loop)


def test_gather_robust_to_nested_missing_data(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        w = e.submit(inc, 1)
        x = e.submit(inc, w)
        y = e.submit(inc, x)
        z = e.submit(inc, y)

        yield _wait([z])

        for worker in [a, b]:
            for datum in [y, z]:
                if datum.key in worker.data:
                    del worker.data[datum.key]

        result = yield e._gather([z])

        assert result == [inc(inc(inc(inc(1))))]

        yield e._shutdown()
    _test_cluster(f, loop)


def test_tokenize_on_futures(loop):
    e = Executor((None, None), start=False, loop=loop)
    x = e.submit(inc, 1)
    y = e.submit(inc, 1)
    tok = tokenize(x)
    assert tokenize(x) == tokenize(x)
    assert tokenize(x) == tokenize(y)

    e.futures[x.key]['status'] = 'finished'

    assert tok == tokenize(y)


def test_restrictions_submit(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(inc, 1, workers={a.ip})
        y = e.submit(inc, x, workers={b.ip})
        yield _wait([x, y])

        assert e.restrictions[x.key] == {a.ip}
        assert x.key in a.data

        assert e.restrictions[y.key] == {b.ip}
        assert y.key in b.data

        yield e._shutdown()
    _test_cluster(f, loop)


def test_restrictions_map(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        L = e.map(inc, range(5), workers={a.ip})
        yield _wait(L)

        assert set(a.data) == {x.key for x in L}
        assert not b.data
        for x in L:
            assert e.restrictions[x.key] == {a.ip}

        L = e.map(inc, [10, 11, 12], workers=[{a.ip},
                                              {a.ip, b.ip},
                                              {b.ip}])
        yield _wait(L)

        assert e.restrictions[L[0].key] == {a.ip}
        assert e.restrictions[L[1].key] == {a.ip, b.ip}
        assert e.restrictions[L[2].key] == {b.ip}

        with pytest.raises(ValueError):
            e.map(inc, [10, 11, 12], workers=[{a.ip}])

        yield e._shutdown()
    _test_cluster(f, loop)


def test_restrictions_get(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
        restrictions = {'y': {a.ip}, 'z': {b.ip}}

        result = yield e._get(dsk, 'z', restrictions)
        assert result == 3
        assert 'y' in a.data
        assert 'z' in b.data

        yield e._shutdown()
    _test_cluster(f, loop)


def dont_test_bad_restrictions_raise_exception(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        z = e.submit(inc, 2, workers={'bad-address'})
        try:
            yield z._result()
            assert False
        except ValueError as e:
            assert 'bad-address' in str(e)
            assert z.key in str(e)

        yield e._shutdown()
    _test_cluster(f, loop)


def test_submit_after_failed_worker(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            L = e.map(inc, range(10))
            wait(L)
            a['proc'].terminate()
            total = e.submit(sum, L)
            assert total.result() == sum(map(inc, range(10)))


def test_gather_after_failed_worker(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            L = e.map(inc, range(10))
            wait(L)
            a['proc'].terminate()
            result = e.gather(L)
            assert result == list(map(inc, range(10)))


@slow
def test_gather_then_submit_after_failed_workers(loop):
    with cluster(nworkers=4) as (c, [w, x, y, z]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            L = e.map(inc, range(20))
            wait(L)
            w['proc'].terminate()
            total = e.submit(sum, L)
            wait([total])

            (_, port) = first(e.who_has[total.key])
            for d in [x, y, z]:
                if d['port'] == port:
                    d['proc'].terminate()

            result = e.gather([total])
            assert result == [sum(map(inc, range(20)))]


def test_errors_dont_block(loop):
    c = Center('127.0.0.1', 8017)
    w = Worker('127.0.0.2', 8018, c.ip, c.port, ncores=1)
    e = Executor((c.ip, c.port), start=False, loop=loop)
    @gen.coroutine
    def f():
        c.listen(c.port)
        yield w._start()
        yield e._start()

        L = [e.submit(inc, 1),
             e.submit(throws, 1),
             e.submit(inc, 2),
             e.submit(throws, 2)]

        i = 0
        while not (L[0].status == L[2].status == 'finished'):
            i += 1
            if i == 1000:
                assert False
            yield gen.sleep(0.01)
        result = yield e._gather([L[0], L[2]])
        assert result == [2, 3]

        yield w._close()
        c.stop()

    loop.run_sync(f)


def test_submit_quotes(loop):
    def assert_list(x, z=[]):
        return isinstance(x, list) and isinstance(z, list)

    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(assert_list, [1, 2, 3])
        result = yield x._result()
        assert result

        x = e.submit(assert_list, [1, 2, 3], z=[4, 5, 6])
        result = yield x._result()
        assert result

        x = e.submit(inc, 1)
        y = e.submit(inc, 2)
        z = e.submit(assert_list, [x, y])
        result = yield z._result()
        assert result

        yield e._shutdown()
    _test_cluster(f, loop)


def test_map_quotes(loop):
    def assert_list(x, z=[]):
        return isinstance(x, list) and isinstance(z, list)

    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        L = e.map(assert_list, [[1, 2, 3], [4]])
        result = yield e._gather(L)
        assert all(result)

        L = e.map(assert_list, [[1, 2, 3], [4]], z=[10])
        result = yield e._gather(L)
        assert all(result)

        L = e.map(assert_list, [[1, 2, 3], [4]], [[]] * 3)
        result = yield e._gather(L)
        assert all(result)

        yield e._shutdown()
    _test_cluster(f)


def test_two_consecutive_executors_share_results(loop):
    from random import randint
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(randint, 0, 1000, pure=True)
        xx = yield x._result()

        f = Executor((c.ip, c.port), start=False, loop=loop)
        yield f._start()

        y = f.submit(randint, 0, 1000, pure=True)
        yy = yield y._result()

        assert xx == yy

        yield e._shutdown()
        yield f._shutdown()
    _test_cluster(f)


def test_submit_then_get_with_Future(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(slowinc, 1)
        dsk = {'y': (inc, x)}

        result = yield e._get(dsk, 'y')
        assert result == 3

        yield e._shutdown()
    _test_cluster(f)


def test_aliases(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(inc, 1)

        dsk = {'y': x}
        result = yield e._get(dsk, 'y')
        assert result == 2

        yield e._shutdown()
    _test_cluster(f, loop)


def test_executor_has_state_on_initialization(loop):
    e = Executor('127.0.0.1:8787', start=False, loop=loop)
    assert isinstance(e.ncores, dict)


def test__scatter(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        d = yield e._scatter({'y': 20})
        assert isinstance(d['y'], Future)
        assert a.data.get('y') == 20 or b.data.get('y') == 20
        assert a.address in e.who_has['y'] or b.address in e.who_has['y']
        assert c.who_has['y']
        assert e.nbytes == {'y': sizeof(20)}
        yy = yield e._gather([d['y']])
        assert yy == [20]

        [x] = yield e._scatter([10])
        assert isinstance(x, Future)
        assert a.data.get(x.key) == 10 or b.data.get(x.key) == 10
        xx = yield e._gather([x])
        assert c.who_has[x.key]
        assert a.address in e.who_has[x.key] or b.address in e.who_has[x.key]
        assert e.nbytes == {'y': sizeof(20), x.key: sizeof(10)}
        assert xx == [10]

        z = e.submit(add, x, d['y'])  # submit works on RemoteData
        result = yield z._result()
        assert result == 10 + 20
        result = yield e._gather([z, x])
        assert result == [30, 10]

        yield e._shutdown()
    _test_cluster(f, loop)


def test_get_releases_data(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        [x] = yield e._get({'x': (inc, 1)}, ['x'])
        import gc; gc.collect()
        assert e.refcount['x'] == 0

        yield e._shutdown()
    _test_cluster(f, loop)


def test_global_executors(loop):
    assert not _global_executors
    with pytest.raises(ValueError):
        default_executor()
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            assert _global_executors == {e}
            assert default_executor() is e
            with Executor(('127.0.0.1', c['port']), loop=loop) as f:
                with pytest.raises(ValueError):
                    default_executor()
                assert default_executor(e) is e
                assert default_executor(f) is f

    assert not _global_executors


def test_exception_on_exception(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)

        x = e.submit(lambda: 1 / 0)
        y = e.submit(inc, x)

        yield e._start()

        with pytest.raises(ZeroDivisionError):
            out = yield y._result()

        z = e.submit(inc, y)

        with pytest.raises(ZeroDivisionError):
            out = yield z._result()

        yield e._shutdown()
    _test_cluster(f, loop)


def test_nbytes(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        [x] = yield e._scatter([1])
        assert e.nbytes == {x.key: sizeof(1)}

        y = e.submit(inc, x)
        yield y._result()

        assert e.nbytes == {x.key: sizeof(1),
                            y.key: sizeof(2)}

        yield e._shutdown()
    _test_cluster(f, loop)


def test_nbytes_determines_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(identity, 1, workers=[a.address[0]])
        y = e.submit(identity, tuple(range(100)), workers=[b.address[0]])
        yield e._gather([x, y])

        z = e.submit(lambda x, y: None, x, y)
        yield z._result()
        assert e.who_has[z.key] == {b.address}

        yield e._shutdown()
    _test_cluster(f, loop)


def test_pragmatic_move_small_data_to_large_data(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        lists = e.map(lambda n: list(range(n)), [10] * 10, pure=False)
        sums = e.map(sum, lists)
        total = e.submit(sum, sums)

        def f(x, y):
            return None
        results = e.map(f, lists, [total] * 10)

        yield _wait([total])

        yield _wait(results)

        for l, r in zip(lists, results):
            assert e.who_has[l.key] == e.who_has[r.key]

        yield e._shutdown()
    _test_cluster(f, loop)


def test_get_with_non_list_key(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        dsk = {('x', 0): (inc, 1), 5: (inc, 2)}

        x = yield e._get(dsk, ('x', 0))
        y = yield e._get(dsk, 5)
        assert x == 2
        assert y == 3

        yield e._shutdown()
    _test_cluster(f, loop)


def test_get_with_error(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        dsk = {'x': (div, 1, 0), 'y': (inc, 'x')}
        with pytest.raises(ZeroDivisionError):
            y = yield e._get(dsk, 'y')

        yield e._shutdown()
    _test_cluster(f, loop)


def test_get_with_error_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            dsk = {'x': (div, 1, 0), 'y': (inc, 'x')}
            with pytest.raises(ZeroDivisionError):
                y = e.get(dsk, 'y')


def test_directed_scatter(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        yield e._scatter([1, 2, 3], workers=[a.address])
        assert len(a.data) == 3
        assert not b.data

        yield e._shutdown()
    _test_cluster(f, loop)


def test_directed_scatter_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            e.scatter([1, 2, 3], workers=[('127.0.0.1', b['port'])])
            has_what = sync(e.loop, e.center.has_what)
            assert len(has_what[('127.0.0.1', b['port'])]) == 3
            assert len(has_what[('127.0.0.1', a['port'])]) == 0


def test_many_submits_spread_evenly(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        L = [e.submit(inc, i) for i in range(10)]
        yield _wait(L)

        assert a.data and b.data

        yield e._shutdown()
    _test_cluster(f, loop)


def test_traceback(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x = e.submit(div, 1, 0)
        tb = yield x._traceback()

        if sys.version_info[0] >= 3:
            assert any('x / y' in line for line in tb)

        yield e._shutdown()
    _test_cluster(f, loop)


def test_traceback_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            x = e.submit(div, 1, 0)
            tb = x.traceback()
            if sys.version_info[0] >= 3:
                assert any('x / y' in line for line in tb)

            y = e.submit(inc, x)
            tb2 = y.traceback()

            assert set(tb2).issuperset(set(tb))

            z = e.submit(div, 1, 2)
            tb = z.traceback()
            assert tb is None


def test_restart(loop):
    from distributed import Nanny, rpc
    c = Center('127.0.0.1', 8006)
    a = Nanny('127.0.0.1', 8007, 8008, '127.0.0.1', 8006, ncores=2)
    b = Nanny('127.0.0.1', 8009, 8010, '127.0.0.1', 8006, ncores=2)
    c.listen(c.port)
    @gen.coroutine
    def f():
        yield a._start()
        yield b._start()

        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()
        assert e.ncores == {a.worker_address: 2, b.worker_address: 2}

        x = e.submit(inc, 1)
        y = e.submit(inc, x)
        yield y._result()

        cc = rpc(ip=c.ip, port=c.port)
        who_has = yield cc.who_has()
        try:
            assert e.who_has == who_has
            assert set(e.who_has) == {x.key, y.key}

            yield e._restart()

            assert len(e.stacks) == 2
            assert len(e.processing) == 2

            who_has = yield cc.who_has()
            assert not who_has
            assert not e.who_has

            assert x.cancelled()
            assert y.cancelled()

        finally:
            yield a._close()
            yield b._close()
            yield e._shutdown()
            c.stop()

    loop.run_sync(f)


def test_restart_sync(loop):
    with cluster(nanny=True) as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            assert len(e.has_what) == 2
            x = e.submit(div, 1, 2)
            x.result()

            assert e.who_has
            e.restart()
            assert not e.who_has
            assert x.cancelled()

            with pytest.raises(CancelledError):
                x.result()

            assert set(e.stacks) == set(e.processing) == set(e.ncores)
            assert len(e.stacks) == 2

            y = e.submit(div, 1, 3)
            assert y.result() == 1 / 3


def test_restart_fast(loop):
    with cluster(nanny=True) as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            L = e.map(sleep, range(10))

            start = time()
            e.restart()
            assert time() - start < 5

            assert all(x.status == 'cancelled' for x in L)

            x = e.submit(inc, 1)
            assert x.result() == 2


def test_fast_kill(loop):
    from distributed import Nanny, rpc
    c = Center('127.0.0.1', 8006)
    a = Nanny('127.0.0.1', 8007, 8008, '127.0.0.1', 8006, ncores=2)
    b = Nanny('127.0.0.1', 8009, 8010, '127.0.0.1', 8006, ncores=2)
    e = Executor((c.ip, c.port), start=False, loop=loop)
    c.listen(c.port)
    @gen.coroutine
    def f():
        yield a._start()
        yield b._start()

        while len(c.ncores) < 2:
            yield gen.sleep(0.01)
        yield e._start()

        L = e.map(sleep, range(10))

        try:

            start = time()
            yield e._restart()
            assert time() - start < 5

            assert all(x.status == 'cancelled' for x in L)
            c.stop()

            x = e.submit(inc, 1)
            result = yield x._result()
            assert result == 2
        finally:
            yield a._close()
            yield b._close()
            yield e._shutdown()
            c.stop()

    loop.run_sync(f)

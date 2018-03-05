from __future__ import print_function, division, absolute_import

from operator import add

from collections import Iterator, deque
from concurrent.futures import CancelledError
import gc
import itertools
import logging
import os
import pickle
import random
import sys
import threading
from threading import Semaphore
from time import sleep
import traceback
import warnings
import weakref
import zipfile

import pytest
from toolz import (identity, isdistinct, concat, pluck, valmap,
                   partial, first)
from tornado import gen
from tornado.ioloop import IOLoop

import dask
from dask import delayed
from dask.context import _globals
from distributed import (Worker, Nanny, fire_and_forget, config,
                         get_client, secede, get_worker, Executor, profile)
from distributed.comm import CommClosedError
from distributed.client import (Client, Future, wait, as_completed, tokenize,
                                _get_global_client, default_client,
                                ensure_default_get, futures_of,
                                temp_default_client)
from distributed.compatibility import PY3

from distributed.metrics import time
from distributed.scheduler import Scheduler, KilledWorker
from distributed.sizeof import sizeof
from distributed.utils import (ignoring, mp_context, sync, tmp_text, tokey,
                               tmpfile)
from distributed.utils_test import (cluster, slow, slowinc, slowadd, slowdec,
                                    randominc, inc, dec, div, throws, geninc, asyncinc,
                                    gen_cluster, gen_test, double, deep, popen,
                                    captured_logger, varying, map_varying,
                                    wait_for, async_wait_for)
from distributed.utils_test import loop, loop_in_thread, nodebug  # noqa F401


@gen_cluster(client=True, timeout=None)
def test_submit(c, s, a, b):
    x = c.submit(inc, 10)
    assert not x.done()

    assert isinstance(x, Future)
    assert x.client is c

    result = yield x
    assert result == 11
    assert x.done()

    y = c.submit(inc, 20)
    z = c.submit(add, x, y)

    result = yield z
    assert result == 11 + 21
    s.validate_state()


@gen_cluster(client=True)
def test_map(c, s, a, b):
    L1 = c.map(inc, range(5))
    assert len(L1) == 5
    assert isdistinct(x.key for x in L1)
    assert all(isinstance(x, Future) for x in L1)

    result = yield L1[0]
    assert result == inc(0)
    assert len(s.tasks) == 5

    L2 = c.map(inc, L1)

    result = yield L2[1]
    assert result == inc(inc(1))
    assert len(s.tasks) == 10
    # assert L1[0].key in s.tasks[L2[0].key]

    total = c.submit(sum, L2)
    result = yield total
    assert result == sum(map(inc, map(inc, range(5))))

    L3 = c.map(add, L1, L2)
    result = yield L3[1]
    assert result == inc(1) + inc(inc(1))

    L4 = c.map(add, range(3), range(4))
    results = yield c.gather(L4)
    if sys.version_info[0] >= 3:
        assert results == list(map(add, range(3), range(4)))

    def f(x, y=10):
        return x + y

    L5 = c.map(f, range(5), y=5)
    results = yield c.gather(L5)
    assert results == list(range(5, 10))

    y = c.submit(f, 10)
    L6 = c.map(f, range(5), y=y)
    results = yield c.gather(L6)
    assert results == list(range(20, 25))
    s.validate_state()


@gen_cluster(client=True)
def test_map_empty(c, s, a, b):
    L1 = c.map(inc, [], pure=False)
    assert len(L1) == 0
    results = yield c.gather(L1)
    assert results == []


@gen_cluster(client=True)
def test_map_keynames(c, s, a, b):
    futures = c.map(inc, range(4), key='INC')
    assert all(f.key.startswith('INC') for f in futures)
    assert isdistinct(f.key for f in futures)

    futures2 = c.map(inc, [5, 6, 7, 8], key='INC')
    assert [f.key for f in futures] != [f.key for f in futures2]

    keys = ['inc-1', 'inc-2', 'inc-3', 'inc-4']
    futures = c.map(inc, range(4), key=keys)
    assert [f.key for f in futures] == keys


@gen_cluster(client=True)
def test_map_retries(c, s, a, b):
    args = [[ZeroDivisionError("one"), 2, 3],
            [4, 5, 6],
            [ZeroDivisionError("seven"), ZeroDivisionError("eight"), 9]]

    x, y, z = c.map(*map_varying(args), retries=2)
    assert (yield x) == 2
    assert (yield y) == 4
    assert (yield z) == 9

    x, y, z = c.map(*map_varying(args), retries=1, pure=False)
    assert (yield x) == 2
    assert (yield y) == 4
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield z
    exc_info.match("eight")

    x, y, z = c.map(*map_varying(args), retries=0, pure=False)
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield x
    exc_info.match("one")
    assert (yield y) == 4
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield z
    exc_info.match("seven")


@gen_cluster(client=True)
def test_compute_retries(c, s, a, b):
    args = [ZeroDivisionError("one"), ZeroDivisionError("two"), 3]

    # Sanity check for varying() use
    x = c.compute(delayed(varying(args))())
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield x
    exc_info.match("one")

    # Same retries for all
    x = c.compute(delayed(varying(args))(), retries=1)
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield x
    exc_info.match("two")

    x = c.compute(delayed(varying(args))(), retries=2)
    assert (yield x) == 3

    args.append(4)
    x = c.compute(delayed(varying(args))(), retries=2)
    assert (yield x) == 3

    # Per-future retries
    xargs = [ZeroDivisionError("one"), ZeroDivisionError("two"), 30, 40]
    yargs = [ZeroDivisionError("five"), ZeroDivisionError("six"), 70]
    zargs = [80, 90, 100]

    x, y = [delayed(varying(args))() for args in (xargs, yargs)]
    x, y = c.compute([x, y], retries={x: 2})
    gc.collect()

    assert (yield x) == 30
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield y
    exc_info.match("five")

    x, y, z = [delayed(varying(args))() for args in (xargs, yargs, zargs)]
    x, y, z = c.compute([x, y, z], retries={(y, z): 2})

    with pytest.raises(ZeroDivisionError) as exc_info:
        yield x
    exc_info.match("one")
    assert (yield y) == 70
    assert (yield z) == 80


@gen_cluster(client=True)
def test_compute_persisted_retries(c, s, a, b):
    args = [ZeroDivisionError("one"), ZeroDivisionError("two"), 3]

    # Sanity check
    x = c.persist(delayed(varying(args))())
    fut = c.compute(x)
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield fut
    exc_info.match("one")

    x = c.persist(delayed(varying(args))())
    fut = c.compute(x, retries=1)
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield fut
    exc_info.match("two")

    x = c.persist(delayed(varying(args))())
    fut = c.compute(x, retries=2)
    assert (yield fut) == 3

    args.append(4)
    x = c.persist(delayed(varying(args))())
    fut = c.compute(x, retries=3)
    assert (yield fut) == 3


@gen_cluster(client=True)
def test_persist_retries(c, s, a, b):
    # Same retries for all
    args = [ZeroDivisionError("one"), ZeroDivisionError("two"), 3]

    x = c.persist(delayed(varying(args))(), retries=1)
    x = c.compute(x)
    with pytest.raises(ZeroDivisionError) as exc_info:
        yield x
    exc_info.match("two")

    x = c.persist(delayed(varying(args))(), retries=2)
    x = c.compute(x)
    assert (yield x) == 3

    # Per-key retries
    xargs = [ZeroDivisionError("one"), ZeroDivisionError("two"), 30, 40]
    yargs = [ZeroDivisionError("five"), ZeroDivisionError("six"), 70]
    zargs = [80, 90, 100]

    x, y, z = [delayed(varying(args))() for args in (xargs, yargs, zargs)]
    x, y, z = c.persist([x, y, z], retries={(y, z): 2})
    x, y, z = c.compute([x, y, z])

    with pytest.raises(ZeroDivisionError) as exc_info:
        yield x
    exc_info.match("one")
    assert (yield y) == 70
    assert (yield z) == 80


@gen_cluster(client=True)
def test_retries_dask_array(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.ones((10, 10), chunks=(3, 3))
    future = c.compute(x.sum(), retries=2)
    y = yield future
    assert y == 100


@gen_cluster(client=True)
def test_future_repr(c, s, a, b):
    x = c.submit(inc, 10)
    for func in [repr, lambda x: x._repr_html_()]:
        assert str(x.key) in func(x)
        assert str(x.status) in func(x)
        assert str(x.status) in repr(c.futures[x.key])


@gen_cluster(client=True)
def test_future_tuple_repr(c, s, a, b):
    da = pytest.importorskip('dask.array')
    y = da.arange(10, chunks=(5,)).persist()
    f = futures_of(y)[0]
    for func in [repr, lambda x: x._repr_html_()]:
        for k in f.key:
            assert str(k) in func(f)


@gen_cluster(client=True)
def test_Future_exception(c, s, a, b):
    x = c.submit(div, 1, 0)
    result = yield x.exception()
    assert isinstance(result, ZeroDivisionError)

    x = c.submit(div, 1, 1)
    result = yield x.exception()
    assert result is None


def test_Future_exception_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(div, 1, 0)
            assert isinstance(x.exception(), ZeroDivisionError)

            x = c.submit(div, 1, 1)
            assert x.exception() is None


@gen_cluster(client=True)
def test_Future_release(c, s, a, b):
    # Released Futures should be removed timely from the Client
    x = c.submit(div, 1, 1)
    yield x
    x.release()
    yield gen.moment
    assert not c.futures

    x = c.submit(slowinc, 1, delay=0.5)
    x.release()
    yield gen.moment
    assert not c.futures

    x = c.submit(div, 1, 0)
    yield x.exception()
    x.release()
    yield gen.moment
    assert not c.futures


def test_Future_release_sync(loop):
    # Released Futures should be removed timely from the Client
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(div, 1, 1)
            x.result()
            x.release()
            wait_for(lambda: not c.futures, timeout=0.3)

            x = c.submit(slowinc, 1, delay=0.8)
            x.release()
            wait_for(lambda: not c.futures, timeout=0.3)

            x = c.submit(div, 1, 0)
            x.exception()
            x.release()
            wait_for(lambda: not c.futures, timeout=0.3)


def test_short_tracebacks(loop):
    tblib = pytest.importorskip('tblib')
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            future = c.submit(div, 1, 0)
            try:
                future.result()
            except Exception:
                _, _, tb = sys.exc_info()
            tb = tblib.Traceback(tb).to_dict()
            n = 0

            while tb is not None:
                n += 1
                tb = tb['tb_next']

            assert n < 5


@gen_cluster(client=True)
def test_map_naming(c, s, a, b):
    L1 = c.map(inc, range(5))
    L2 = c.map(inc, range(5))

    assert [x.key for x in L1] == [x.key for x in L2]

    L3 = c.map(inc, [1, 1, 1, 1])
    assert len({x._state for x in L3}) == 1

    L4 = c.map(inc, [1, 1, 1, 1], pure=False)
    assert len({x._state for x in L4}) == 4


@gen_cluster(client=True)
def test_submit_naming(c, s, a, b):
    a = c.submit(inc, 1)
    b = c.submit(inc, 1)

    assert a._state is b._state

    c = c.submit(inc, 1, pure=False)
    assert c.key != a.key


@gen_cluster(client=True)
def test_exceptions(c, s, a, b):
    x = c.submit(div, 1, 2)
    result = yield x
    assert result == 1 / 2

    x = c.submit(div, 1, 0)
    with pytest.raises(ZeroDivisionError):
        result = yield x

    x = c.submit(div, 10, 2)  # continues to operate
    result = yield x
    assert result == 10 / 2


@gen_cluster()
def test_gc(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)

    x = c.submit(inc, 10)
    yield x
    assert s.tasks[x.key].who_has
    x.__del__()
    yield async_wait_for(lambda: x.key not in s.tasks or not s.tasks[x.key].who_has, timeout=0.3)

    yield c.close()


def test_thread(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(inc, 1)
            assert x.result() == 2

            x = c.submit(slowinc, 1, delay=0.3)
            with pytest.raises(gen.TimeoutError):
                x.result(timeout=0.01)
            assert x.result() == 2


def test_sync_exceptions(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(div, 10, 2)
            assert x.result() == 5

            y = c.submit(div, 10, 0)
            try:
                y.result()
                assert False
            except ZeroDivisionError:
                pass

            z = c.submit(div, 10, 5)
            assert z.result() == 2


@gen_cluster(client=True)
def test_gather(c, s, a, b):
    x = c.submit(inc, 10)
    y = c.submit(inc, x)

    result = yield c.gather(x)
    assert result == 11
    result = yield c.gather([x])
    assert result == [11]
    result = yield c.gather({'x': x, 'y': [y]})
    assert result == {'x': 11, 'y': [12]}


@gen_cluster(client=True)
def test_gather_lost(c, s, a, b):
    [x] = yield c.scatter([1], workers=a.address)
    y = c.submit(inc, 1, workers=b.address)

    yield a._close()

    with pytest.raises(Exception):
        res = yield c.gather([x, y])


def test_gather_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(inc, 1)
            assert c.gather(x) == 2

            y = c.submit(div, 1, 0)

            with pytest.raises(ZeroDivisionError):
                c.gather([x, y])

            [xx] = c.gather([x, y], errors='skip')
            assert xx == 2


@gen_cluster(client=True)
def test_gather_strict(c, s, a, b):
    x = c.submit(div, 2, 1)
    y = c.submit(div, 1, 0)

    with pytest.raises(ZeroDivisionError):
        yield c.gather([x, y])

    [xx] = yield c.gather([x, y], errors='skip')
    assert xx == 2


@gen_cluster(client=True, timeout=None)
def test_get(c, s, a, b):
    future = c.get({'x': (inc, 1)}, 'x', sync=False)
    assert isinstance(future, Future)
    result = yield future
    assert result == 2

    futures = c.get({'x': (inc, 1)}, ['x'], sync=False)
    assert isinstance(futures[0], Future)
    result = yield futures
    assert result == [2]

    result = yield c.get({}, [], sync=False)
    assert result == []

    result = yield c.get({('x', 1): (inc, 1), ('x', 2): (inc, ('x', 1))},
                         ('x', 2), sync=False)
    assert result == 3


def test_get_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            assert c.get({'x': (inc, 1)}, 'x') == 2


def test_no_future_references(loop):
    from weakref import WeakSet
    ws = WeakSet()
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.map(inc, range(10))
            ws.update(futures)
            del futures
            import gc
            gc.collect()
            start = time()
            while list(ws):
                sleep(0.01)
                assert time() < start + 2


def test_get_sync_optimize_graph_passes_through(loop):
    import dask.bag as db
    import dask
    bag = db.range(10, npartitions=3).map(inc)
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            dask.compute(bag.sum(), optimize_graph=False, get=c.get)


@gen_cluster(client=True)
def test_gather_errors(c, s, a, b):
    def f(a, b):
        raise TypeError

    def g(a, b):
        raise AttributeError

    future_f = c.submit(f, 1, 2)
    future_g = c.submit(g, 1, 2)
    with pytest.raises(TypeError):
        yield c.gather(future_f)
    with pytest.raises(AttributeError):
        yield c.gather(future_g)

    yield a._close()


@gen_cluster(client=True)
def test_wait(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, 1)
    z = c.submit(inc, 2)

    done, not_done = yield wait([x, y, z])

    assert done == {x, y, z}
    assert not_done == set()
    assert x.status == y.status == 'finished'


@gen_cluster(client=True, timeout=2)
def test_wait_timeout(c, s, a, b):
    future = c.submit(sleep, 0.3)
    with pytest.raises(gen.TimeoutError):
        yield wait(future, timeout=0.01)


def test_wait_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(inc, 1)
            y = c.submit(inc, 2)

            done, not_done = wait([x, y])
            assert done == {x, y}
            assert not_done == set()
            assert x.status == y.status == 'finished'

            future = c.submit(sleep, 0.3)
            with pytest.raises(gen.TimeoutError):
                wait(future, timeout=0.01)


def test_wait_informative_error_for_timeouts(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(inc, 1)
            y = c.submit(inc, 2)

            try:
                wait(x, y)
            except Exception as e:
                assert "timeout" in str(e)
                assert "list" in str(e)


@gen_cluster(client=True)
def test_garbage_collection(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, 1)

    assert c.refcount[x.key] == 2
    x.__del__()
    yield gen.moment
    assert c.refcount[x.key] == 1

    z = c.submit(inc, y)
    y.__del__()
    yield gen.moment

    result = yield z
    assert result == 3

    ykey = y.key
    y.__del__()
    yield gen.moment
    assert ykey not in c.futures


@gen_cluster(client=True)
def test_garbage_collection_with_scatter(c, s, a, b):
    [future] = yield c.scatter([1])
    assert future.key in c.futures
    assert future.status == 'finished'
    assert s.who_wants[future.key] == {c.id}

    key = future.key
    assert c.refcount[key] == 1
    future.__del__()
    yield gen.moment
    assert c.refcount[key] == 0

    start = time()
    while True:
        if key not in s.tasks or not s.tasks[key].who_has:
            break
        else:
            assert time() < start + 3
            yield gen.sleep(0.1)


@gen_cluster(timeout=1000, client=True)
def test_recompute_released_key(c, s, a, b):
    x = c.submit(inc, 100)
    result1 = yield x
    xkey = x.key
    del x
    import gc
    gc.collect()
    yield gen.moment
    assert c.refcount[xkey] == 0

    # 1 second batching needs a second action to trigger
    while xkey in s.tasks and s.tasks[xkey].who_has or xkey in a.data or xkey in b.data:
        yield gen.sleep(0.1)

    x = c.submit(inc, 100)
    assert x.key in c.futures
    result2 = yield x
    assert result1 == result2


@slow
@gen_cluster(client=True)
def test_long_tasks_dont_trigger_timeout(c, s, a, b):
    from time import sleep
    x = c.submit(sleep, 3)
    yield x


@pytest.mark.skip
@gen_cluster(client=True)
def test_missing_data_heals(c, s, a, b):
    a.validate = False
    b.validate = False
    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(inc, y)

    yield wait([x, y, z])

    # Secretly delete y's key
    if y.key in a.data:
        del a.data[y.key]
        a.release_key(y.key)
    if y.key in b.data:
        del b.data[y.key]
        b.release_key(y.key)
    yield gen.moment

    w = c.submit(add, y, z)

    result = yield w
    assert result == 3 + 4


@pytest.mark.skip
@gen_cluster(client=True)
def test_gather_robust_to_missing_data(c, s, a, b):
    a.validate = False
    b.validate = False
    x, y, z = c.map(inc, range(3))
    yield wait([x, y, z])  # everything computed

    for f in [x, y]:
        for w in [a, b]:
            if f.key in w.data:
                del w.data[f.key]
                yield gen.moment
                w.release_key(f.key)

    xx, yy, zz = yield c.gather([x, y, z])
    assert (xx, yy, zz) == (1, 2, 3)


@pytest.mark.skip
@gen_cluster(client=True)
def test_gather_robust_to_nested_missing_data(c, s, a, b):
    a.validate = False
    b.validate = False
    w = c.submit(inc, 1)
    x = c.submit(inc, w)
    y = c.submit(inc, x)
    z = c.submit(inc, y)

    yield wait([z])

    for worker in [a, b]:
        for datum in [y, z]:
            if datum.key in worker.data:
                del worker.data[datum.key]
                yield gen.moment
                worker.release_key(datum.key)

    result = yield c.gather([z])

    assert result == [inc(inc(inc(inc(1))))]


@gen_cluster(client=True)
def test_tokenize_on_futures(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, 1)
    tok = tokenize(x)
    assert tokenize(x) == tokenize(x)
    assert tokenize(x) == tokenize(y)

    c.futures[x.key].finish()

    assert tok == tokenize(y)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster([('127.0.0.1', 1), ('127.0.0.2', 2)], client=True)
def test_restrictions_submit(c, s, a, b):
    x = c.submit(inc, 1, workers={a.ip})
    y = c.submit(inc, x, workers={b.ip})
    yield wait([x, y])

    assert s.host_restrictions[x.key] == {a.ip}
    assert x.key in a.data

    assert s.host_restrictions[y.key] == {b.ip}
    assert y.key in b.data


@gen_cluster(client=True)
def test_restrictions_ip_port(c, s, a, b):
    x = c.submit(inc, 1, workers={a.address})
    y = c.submit(inc, x, workers={b.address})
    yield wait([x, y])

    assert s.worker_restrictions[x.key] == {a.address}
    assert x.key in a.data

    assert s.worker_restrictions[y.key] == {b.address}
    assert y.key in b.data


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster([('127.0.0.1', 1), ('127.0.0.2', 2)], client=True)
def test_restrictions_map(c, s, a, b):
    L = c.map(inc, range(5), workers={a.ip})
    yield wait(L)

    assert set(a.data) == {x.key for x in L}
    assert not b.data
    for x in L:
        assert s.host_restrictions[x.key] == {a.ip}

    L = c.map(inc, [10, 11, 12], workers=[{a.ip},
                                          {a.ip, b.ip},
                                          {b.ip}])
    yield wait(L)

    assert s.host_restrictions[L[0].key] == {a.ip}
    assert s.host_restrictions[L[1].key] == {a.ip, b.ip}
    assert s.host_restrictions[L[2].key] == {b.ip}

    with pytest.raises(ValueError):
        c.map(inc, [10, 11, 12], workers=[{a.ip}])


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster([('127.0.0.1', 1), ('127.0.0.2', 2)], client=True)
def test_restrictions_get(c, s, a, b):
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    restrictions = {'y': {a.ip}, 'z': {b.ip}}

    futures = c.get(dsk, ['y', 'z'], restrictions, sync=False)
    result = yield futures
    assert result == [2, 3]
    assert 'y' in a.data
    assert 'z' in b.data


@gen_cluster(client=True)
def dont_test_bad_restrictions_raise_exception(c, s, a, b):
    z = c.submit(inc, 2, workers={'bad-address'})
    try:
        yield z
        assert False
    except ValueError as e:
        assert 'bad-address' in str(e)
        assert z.key in str(e)


@gen_cluster(client=True, timeout=None)
def test_remove_worker(c, s, a, b):
    L = c.map(inc, range(20))
    yield wait(L)

    yield b._close()

    assert b.address not in s.worker_info

    result = yield c.gather(L)
    assert result == list(map(inc, range(20)))


@gen_cluster(ncores=[('127.0.0.1', 1)], client=True)
def test_errors_dont_block(c, s, w):
    L = [c.submit(inc, 1),
         c.submit(throws, 1),
         c.submit(inc, 2),
         c.submit(throws, 2)]

    start = time()
    while not (L[0].status == L[2].status == 'finished'):
        assert time() < start + 5
        yield gen.sleep(0.01)

    result = yield c.gather([L[0], L[2]])
    assert result == [2, 3]


@gen_cluster(client=True)
def test_submit_quotes(c, s, a, b):
    def assert_list(x, z=[]):
        return isinstance(x, list) and isinstance(z, list)

    x = c.submit(assert_list, [1, 2, 3])
    result = yield x
    assert result

    x = c.submit(assert_list, [1, 2, 3], z=[4, 5, 6])
    result = yield x
    assert result

    x = c.submit(inc, 1)
    y = c.submit(inc, 2)
    z = c.submit(assert_list, [x, y])
    result = yield z
    assert result


@gen_cluster(client=True)
def test_map_quotes(c, s, a, b):
    def assert_list(x, z=[]):
        return isinstance(x, list) and isinstance(z, list)

    L = c.map(assert_list, [[1, 2, 3], [4]])
    result = yield c.gather(L)
    assert all(result)

    L = c.map(assert_list, [[1, 2, 3], [4]], z=[10])
    result = yield c.gather(L)
    assert all(result)

    L = c.map(assert_list, [[1, 2, 3], [4]], [[]] * 3)
    result = yield c.gather(L)
    assert all(result)


@gen_cluster()
def test_two_consecutive_clients_share_results(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)

    x = c.submit(random.randint, 0, 1000, pure=True)
    xx = yield x

    f = yield Client((s.ip, s.port), asynchronous=True)

    y = f.submit(random.randint, 0, 1000, pure=True)
    yy = yield y

    assert xx == yy

    yield c.close()
    yield f.close()


@gen_cluster(client=True)
def test_submit_then_get_with_Future(c, s, a, b):
    x = c.submit(slowinc, 1)
    dsk = {'y': (inc, x)}

    result = yield c.get(dsk, 'y', sync=False)
    assert result == 3


@gen_cluster(client=True)
def test_aliases(c, s, a, b):
    x = c.submit(inc, 1)

    dsk = {'y': x}
    result = yield c.get(dsk, 'y', sync=False)
    assert result == 2


@gen_cluster(client=True)
def test_aliases_2(c, s, a, b):
    dsk_keys = [
        ({'x': (inc, 1), 'y': 'x', 'z': 'x', 'w': (add, 'y', 'z')}, ['y', 'w']),
        ({'x': 'y', 'y': 1}, ['x']),
        ({'x': 1, 'y': 'x', 'z': 'y', 'w': (inc, 'z')}, ['w'])]
    for dsk, keys in dsk_keys:
        result = yield c.get(dsk, keys, sync=False)
        assert list(result) == list(dask.get(dsk, keys))
        yield gen.moment


@gen_cluster(client=True)
def test__scatter(c, s, a, b):
    d = yield c.scatter({'y': 20})
    assert isinstance(d['y'], Future)
    assert a.data.get('y') == 20 or b.data.get('y') == 20
    y_who_has = s.get_who_has(keys=['y'])['y']
    assert (a.address in y_who_has or
            b.address in y_who_has)
    assert s.get_nbytes(summary=False) == {'y': sizeof(20)}
    yy = yield c.gather([d['y']])
    assert yy == [20]

    [x] = yield c.scatter([10])
    assert isinstance(x, Future)
    assert a.data.get(x.key) == 10 or b.data.get(x.key) == 10
    xx = yield c.gather([x])
    x_who_has = s.get_who_has(keys=[x.key])[x.key]
    assert s.tasks[x.key].who_has
    assert (s.workers[a.address] in s.tasks[x.key].who_has or
            s.workers[b.address] in s.tasks[x.key].who_has)
    assert s.get_nbytes(summary=False) == {'y': sizeof(20), x.key: sizeof(10)}
    assert xx == [10]

    z = c.submit(add, x, d['y'])  # submit works on Future
    result = yield z
    assert result == 10 + 20
    result = yield c.gather([z, x])
    assert result == [30, 10]


@gen_cluster(client=True)
def test__scatter_types(c, s, a, b):
    d = yield c.scatter({'x': 1})
    assert isinstance(d, dict)
    assert list(d) == ['x']

    for seq in [[1], (1,), {1}, frozenset([1])]:
        L = yield c.scatter(seq)
        assert isinstance(L, type(seq))
        assert len(L) == 1
        s.validate_state()

    seq = yield c.scatter(range(5))
    assert isinstance(seq, list)
    assert len(seq) == 5
    s.validate_state()


@gen_cluster(client=True)
def test__scatter_non_list(c, s, a, b):
    x = yield c.scatter(1)
    assert isinstance(x, Future)
    result = yield x
    assert result == 1


@gen_cluster(client=True)
def test_scatter_hash(c, s, a, b):
    [a] = yield c.scatter([1])
    [b] = yield c.scatter([1])

    assert a.key == b.key
    s.validate_state()


@gen_cluster(client=True)
def test_scatter_tokenize_local(c, s, a, b):
    from dask.base import normalize_token

    class MyObj(object):
        pass

    L = []

    @normalize_token.register(MyObj)
    def f(x):
        L.append(x)
        return 'x'

    obj = MyObj()

    future = yield c.scatter(obj)
    assert L and L[0] is obj


@gen_cluster(client=True)
def test_scatter_singletons(c, s, a, b):
    np = pytest.importorskip('numpy')
    pd = pytest.importorskip('pandas')
    for x in [1, np.ones(5), pd.DataFrame({'x': [1, 2, 3]})]:
        future = yield c.scatter(x)
        result = yield future
        assert str(result) == str(x)


@gen_cluster(client=True)
def test_scatter_typename(c, s, a, b):
    future = yield c.scatter(123)
    assert future.key.startswith('int')


@gen_cluster(client=True)
def test_scatter_hash(c, s, a, b):
    x = yield c.scatter(123)
    y = yield c.scatter(123)
    assert x.key == y.key

    z = yield c.scatter(123, hash=False)
    assert z.key != y.key


@gen_cluster(client=True)
def test_get_releases_data(c, s, a, b):
    [x] = yield c.get({'x': (inc, 1)}, ['x'], sync=False)
    import gc
    gc.collect()
    yield gen.moment
    assert c.refcount['x'] == 0


def test_Current():
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            assert Client.current() is c
        with pytest.raises(ValueError):
            Client.current()
        with Client(s['address']) as c:
            assert Client.current() is c


def test_global_clients(loop):
    assert _get_global_client() is None
    with pytest.raises(ValueError):
        default_client()
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            assert _get_global_client() is c
            assert default_client() is c
            with Client(s['address'], loop=loop) as f:
                assert _get_global_client() is f
                assert default_client() is f
                assert default_client(c) is c
                assert default_client(f) is f

    assert _get_global_client() is None


@gen_cluster(client=True)
def test_exception_on_exception(c, s, a, b):
    x = c.submit(lambda: 1 / 0)
    y = c.submit(inc, x)

    with pytest.raises(ZeroDivisionError):
        yield y

    z = c.submit(inc, y)

    with pytest.raises(ZeroDivisionError):
        yield z


@gen_cluster(client=True)
def test_get_nbytes(c, s, a, b):
    [x] = yield c.scatter([1])
    assert s.get_nbytes(summary=False) == {x.key: sizeof(1)}

    y = c.submit(inc, x)
    yield y

    assert s.get_nbytes(summary=False) == {x.key: sizeof(1),
                                           y.key: sizeof(2)}


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster([('127.0.0.1', 1), ('127.0.0.2', 2)], client=True)
def test_nbytes_determines_worker(c, s, a, b):
    x = c.submit(identity, 1, workers=[a.ip])
    y = c.submit(identity, tuple(range(100)), workers=[b.ip])
    yield c.gather([x, y])

    z = c.submit(lambda x, y: None, x, y)
    yield z
    assert s.tasks[z.key].who_has == {s.workers[b.address]}


@gen_cluster(client=True)
def test_if_intermediates_clear_on_error(c, s, a, b):
    x = delayed(div, pure=True)(1, 0)
    y = delayed(div, pure=True)(1, 2)
    z = delayed(add, pure=True)(x, y)
    f = c.compute(z)
    with pytest.raises(ZeroDivisionError):
        yield f
    s.validate_state()
    assert not any(ts.who_has for ts in s.tasks.values())


@gen_cluster(client=True)
def test_pragmatic_move_small_data_to_large_data(c, s, a, b):
    np = pytest.importorskip('numpy')
    lists = c.map(np.ones, [10000] * 10, pure=False)
    sums = c.map(np.sum, lists)
    total = c.submit(sum, sums)

    def f(x, y):
        return None
    s.task_duration['f'] = 0.001
    results = c.map(f, lists, [total] * 10)

    yield wait([total])

    yield wait(results)

    assert sum(s.tasks[r.key].who_has.issubset(s.tasks[l.key].who_has)
               for l, r in zip(lists, results)) >= 9


@gen_cluster(client=True)
def test_get_with_non_list_key(c, s, a, b):
    dsk = {('x', 0): (inc, 1), 5: (inc, 2)}

    x = yield c.get(dsk, ('x', 0), sync=False)
    y = yield c.get(dsk, 5, sync=False)
    assert x == 2
    assert y == 3


@gen_cluster(client=True)
def test_get_with_error(c, s, a, b):
    dsk = {'x': (div, 1, 0), 'y': (inc, 'x')}
    with pytest.raises(ZeroDivisionError):
        yield c.get(dsk, 'y', sync=False)


def test_get_with_error_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            dsk = {'x': (div, 1, 0), 'y': (inc, 'x')}
            with pytest.raises(ZeroDivisionError):
                c.get(dsk, 'y')


@gen_cluster(client=True)
def test_directed_scatter(c, s, a, b):
    yield c.scatter([1, 2, 3], workers=[a.address])
    assert len(a.data) == 3
    assert not b.data

    yield c.scatter([4, 5], workers=[b.name])
    assert len(b.data) == 2


def test_directed_scatter_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.scatter([1, 2, 3], workers=[b['address']])
            has_what = sync(loop, c.scheduler.has_what)
            assert len(has_what[b['address']]) == len(futures)
            assert len(has_what[a['address']]) == 0


def test_iterator_scatter(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            aa = c.scatter([1, 2, 3])
            assert [1, 2, 3] == c.gather(aa)

            g = (i for i in range(10))
            futures = c.scatter(g)
            assert isinstance(futures, Iterator)

            a = next(futures)
            assert c.gather(a) == 0

            futures = list(futures)
            assert len(futures) == 9
            assert c.gather(futures) == [1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_queue_scatter(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as ee:
            from distributed.compatibility import Queue
            q = Queue()
            for d in range(10):
                q.put(d)

            futures = ee.scatter(q)
            assert isinstance(futures, Queue)
            a = futures.get()
            assert ee.gather(a) == 0


def test_queue_scatter_gather_maxsize(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            from distributed.compatibility import Queue
            q = Queue(maxsize=3)
            out = c.scatter(q, maxsize=10)
            assert out.maxsize == 10
            local = c.gather(q)
            assert not local.maxsize

            q = Queue()
            out = c.scatter(q)
            assert not out.maxsize
            local = c.gather(out, maxsize=10)
            assert local.maxsize == 10

            q = Queue(maxsize=3)
            out = c.scatter(q)
            assert not out.maxsize


def test_queue_gather(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as ee:
            from distributed.compatibility import Queue
            q = Queue()

            qin = list(range(10))
            for d in qin:
                q.put(d)

            futures = ee.scatter(q)
            assert isinstance(futures, Queue)

            ff = ee.gather(futures)
            assert isinstance(ff, Queue)

            qout = []
            for f in range(10):
                qout.append(ff.get())
            assert qout == qin


@pytest.mark.skip(reason="intermittent blocking failures")
def test_iterator_gather(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as ee:

            i_in = list(range(10))

            g = (d for d in i_in)
            futures = ee.scatter(g)
            assert isinstance(futures, Iterator)

            ff = ee.gather(futures)
            assert isinstance(ff, Iterator)

            i_out = list(ff)
            assert i_out == i_in

            i_in = ['a', 'b', 'c', StopIteration('f'), StopIteration, 'd', 'c']

            g = (d for d in i_in)
            futures = ee.scatter(g)

            ff = ee.gather(futures)
            i_out = list(ff)
            assert i_out[:3] == i_in[:3]
            # This is because StopIteration('f') != StopIteration('f')
            assert isinstance(i_out[3], StopIteration)
            assert i_out[3].args == i_in[3].args
            assert i_out[4:] == i_in[4:]


@gen_cluster(client=True)
def test_scatter_direct(c, s, a, b):
    future = yield c.scatter(123, direct=True)
    assert future.key in a.data or future.key in b.data
    assert s.tasks[future.key].who_has
    assert future.status == 'finished'
    result = yield future
    assert result == 123
    assert not s.counters['op'].components[0]['scatter']

    result = yield future
    assert not s.counters['op'].components[0]['gather']

    result = yield c.gather(future)
    assert not s.counters['op'].components[0]['gather']


@gen_cluster(client=True)
def test_scatter_direct_numpy(c, s, a, b):
    np = pytest.importorskip('numpy')
    x = np.ones(5)
    future = yield c.scatter(x, direct=True)
    result = yield future
    assert np.allclose(x, result)
    assert not s.counters['op'].components[0]['scatter']


@gen_cluster(client=True)
def test_scatter_direct_broadcast(c, s, a, b):
    future2 = yield c.scatter(456, direct=True, broadcast=True)
    assert future2.key in a.data
    assert future2.key in b.data
    assert s.tasks[future2.key].who_has == {s.workers[a.address],
                                            s.workers[b.address]}
    result = yield future2
    assert result == 456
    assert not s.counters['op'].components[0]['scatter']


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test_scatter_direct_balanced(c, s, *workers):
    futures = yield c.scatter([1, 2, 3], direct=True)
    assert sorted([len(w.data) for w in workers]) == [0, 1, 1, 1]


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test_scatter_direct_broadcast_target(c, s, *workers):
    futures = yield c.scatter([123, 456], direct=True,
                              workers=workers[0].address)
    assert futures[0].key in workers[0].data
    assert futures[1].key in workers[0].data

    futures = yield c.scatter([123, 456], direct=True, broadcast=True,
                              workers=[w.address for w in workers[:3]])
    assert (f.key in w.data and w.address in s.tasks[f.key].who_has
            for f in futures
            for w in workers[:3])


@gen_cluster(client=True, ncores=[])
def test_scatter_direct_empty(c, s):
    with pytest.raises((ValueError, gen.TimeoutError)):
        yield c.scatter(123, direct=True, timeout=0.1)


@gen_cluster(client=True, timeout=None, ncores=[('127.0.0.1', 1)] * 5)
def test_scatter_direct_spread_evenly(c, s, *workers):
    futures = []
    for i in range(10):
        future = yield c.scatter(i, direct=True)
        futures.append(future)

    assert all(w.data for w in workers)


@pytest.mark.parametrize('direct', [True, False])
@pytest.mark.parametrize('broadcast', [True, False])
def test_scatter_gather_sync(loop, direct, broadcast):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.scatter([1, 2, 3], direct=direct, broadcast=broadcast)
            results = c.gather(futures, direct=direct)
            assert results == [1, 2, 3]


@gen_cluster(client=True)
def test_gather_direct(c, s, a, b):
    futures = yield c.scatter([1, 2, 3])

    data = yield c.gather(futures, direct=True)
    assert data == [1, 2, 3]


@gen_cluster(client=True)
def test_many_submits_spread_evenly(c, s, a, b):
    L = [c.submit(inc, i) for i in range(10)]
    yield wait(L)

    assert a.data and b.data


@gen_cluster(client=True)
def test_traceback(c, s, a, b):
    x = c.submit(div, 1, 0)
    tb = yield x.traceback()

    if sys.version_info[0] >= 3:
        assert any('x / y' in line
                   for line in pluck(3, traceback.extract_tb(tb)))


@gen_cluster(client=True)
def test_get_traceback(c, s, a, b):
    try:
        yield c.get({'x': (div, 1, 0)}, 'x', sync=False)
    except ZeroDivisionError:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        L = traceback.format_tb(exc_traceback)
        assert any('x / y' in line for line in L)


@gen_cluster(client=True)
def test_gather_traceback(c, s, a, b):
    x = c.submit(div, 1, 0)
    try:
        yield c.gather(x)
    except ZeroDivisionError:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        L = traceback.format_tb(exc_traceback)
        assert any('x / y' in line for line in L)


def test_traceback_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(div, 1, 0)
            tb = x.traceback()
            if sys.version_info[0] >= 3:
                assert any('x / y' in line
                           for line in concat(traceback.extract_tb(tb))
                           if isinstance(line, str))

            y = c.submit(inc, x)
            tb2 = y.traceback()

            assert set(pluck(3, traceback.extract_tb(tb2))).issuperset(
                set(pluck(3, traceback.extract_tb(tb))))

            z = c.submit(div, 1, 2)
            tb = z.traceback()
            assert tb is None


@gen_cluster(client=True)
def test_upload_file(c, s, a, b):
    def g():
        import myfile
        return myfile.f()

    try:
        for value in [123, 456]:
            with tmp_text('myfile.py', 'def f():\n    return {}'.format(value)) as fn:
                yield c.upload_file(fn)

            x = c.submit(g, pure=False)
            result = yield x
            assert result == value
    finally:
        # Ensure that this test won't impact the others
        if 'myfile' in sys.modules:
            del sys.modules['myfile']


@gen_cluster(client=True)
def test_upload_file_zip(c, s, a, b):
    def g():
        import myfile
        return myfile.f()

    try:
        for value in [123, 456]:
            with tmp_text('myfile.py', 'def f():\n    return {}'.format(value)) as fn_my_file:
                with zipfile.ZipFile('myfile.zip', 'w') as z:
                    z.write(fn_my_file, arcname=os.path.basename(fn_my_file))
                yield c.upload_file('myfile.zip')

                x = c.submit(g, pure=False)
                result = yield x
                assert result == value
    finally:
        # Ensure that this test won't impact the others
        if os.path.exists('myfile.zip'):
            os.remove('myfile.zip')
        if 'myfile' in sys.modules:
            del sys.modules['myfile']
        for path in sys.path:
            if os.path.basename(path) == 'myfile.zip':
                sys.path.remove(path)
                break


@gen_cluster(client=True)
def test_upload_large_file(c, s, a, b):
    assert a.local_dir
    assert b.local_dir
    with tmp_text('myfile', 'abc') as fn:
        with tmp_text('myfile2', 'def') as fn2:
            yield c._upload_large_file(fn, remote_filename='x')
            yield c._upload_large_file(fn2)

            for w in [a, b]:
                assert os.path.exists(os.path.join(w.local_dir, 'x'))
                assert os.path.exists(os.path.join(w.local_dir, 'myfile2'))
                with open(os.path.join(w.local_dir, 'x')) as f:
                    assert f.read() == 'abc'
                with open(os.path.join(w.local_dir, 'myfile2')) as f:
                    assert f.read() == 'def'


def test_upload_file_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            def g():
                import myfile
                return myfile.x

            with tmp_text('myfile.py', 'x = 123') as fn:
                c.upload_file(fn)
                x = c.submit(g)
                assert x.result() == 123


@gen_cluster(client=True)
def test_upload_file_exception(c, s, a, b):
    with tmp_text('myfile.py', 'syntax-error!') as fn:
        with pytest.raises(SyntaxError):
            yield c.upload_file(fn)


def test_upload_file_exception_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with tmp_text('myfile.py', 'syntax-error!') as fn:
                with pytest.raises(SyntaxError):
                    c.upload_file(fn)


@pytest.mark.skip
@gen_cluster()
def test_multiple_clients(s, a, b):
    a = yield Client((s.ip, s.port), asynchronous=True)
    b = yield Client((s.ip, s.port), asynchronous=True)

    x = a.submit(inc, 1)
    y = b.submit(inc, 2)
    assert x.client is a
    assert y.client is b
    xx = yield x
    yy = yield y
    assert xx == 2
    assert yy == 3
    z = a.submit(add, x, y)
    assert z.client is a
    zz = yield z
    assert zz == 5

    yield a.close()
    yield b.close()


@gen_cluster(client=True)
def test_async_compute(c, s, a, b):
    from dask.delayed import delayed
    x = delayed(1)
    y = delayed(inc)(x)
    z = delayed(dec)(x)

    [yy, zz, aa] = c.compute([y, z, 3], sync=False)
    assert isinstance(yy, Future)
    assert isinstance(zz, Future)
    assert aa == 3

    result = yield c.gather([yy, zz])
    assert result == [2, 0]

    assert isinstance(c.compute(y), Future)
    assert isinstance(c.compute([y]), (tuple, list))


@gen_cluster(client=True)
def test_async_compute_with_scatter(c, s, a, b):
    d = yield c.scatter({('x', 1): 1, ('y', 1): 2})
    x, y = d[('x', 1)], d[('y', 1)]

    from dask.delayed import delayed
    z = delayed(add)(delayed(inc)(x), delayed(inc)(y))
    zz = c.compute(z)

    [result] = yield c.gather([zz])
    assert result == 2 + 3


def test_sync_compute(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = delayed(1)
            y = delayed(inc)(x)
            z = delayed(dec)(x)

            yy, zz = c.compute([y, z], sync=True)
            assert (yy, zz) == (2, 0)


@gen_cluster(client=True)
def test_remote_scatter_gather(c, s, a, b):
    x, y, z = yield c.scatter([1, 2, 3])

    assert x.key in a.data or x.key in b.data
    assert y.key in a.data or y.key in b.data
    assert z.key in a.data or z.key in b.data

    xx, yy, zz = yield c.gather([x, y, z])
    assert (xx, yy, zz) == (1, 2, 3)


@gen_cluster(timeout=1000, client=True)
def test_remote_submit_on_Future(c, s, a, b):
    x = c.submit(lambda x: x + 1, 1)
    y = c.submit(lambda x: x + 1, x)
    result = yield y
    assert result == 3


def test_start_is_idempotent(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            c.start()
            c.start()
            c.start()

            x = c.submit(inc, 1)
            assert x.result() == 2


@gen_cluster(client=True)
def test_client_with_scheduler(c, s, a, b):
    assert s.ncores == {a.address: a.ncores, b.address: b.ncores}

    x = c.submit(inc, 1)
    y = c.submit(inc, 2)
    z = c.submit(add, x, y)
    result = yield x
    assert result == 1 + 1
    result = yield z
    assert result == 1 + 1 + 1 + 2

    A, B, C = yield c.scatter([1, 2, 3])
    AA, BB, xx = yield c.gather([A, B, x])
    assert (AA, BB, xx) == (1, 2, 2)

    result = yield c.get({'x': (inc, 1), 'y': (add, 'x', 10)}, 'y', sync=False)
    assert result == 12


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster([('127.0.0.1', 1), ('127.0.0.2', 2)], client=True)
def test_allow_restrictions(c, s, a, b):
    aws = s.workers[a.address]
    bws = s.workers[a.address]

    x = c.submit(inc, 1, workers=a.ip)
    yield x
    assert s.tasks[x.key].who_has == {aws}
    assert not s.loose_restrictions

    x = c.submit(inc, 2, workers=a.ip, allow_other_workers=True)
    yield x
    assert s.tasks[x.key].who_has == {aws}
    assert x.key in s.loose_restrictions

    L = c.map(inc, range(3, 13), workers=a.ip, allow_other_workers=True)
    yield wait(L)
    assert all(s.tasks[f.key].who_has == {aws} for f in L)
    assert {f.key for f in L}.issubset(s.loose_restrictions)

    x = c.submit(inc, 15, workers='127.0.0.3', allow_other_workers=True)

    yield x
    assert s.tasks[x.key].who_has
    assert x.key in s.loose_restrictions

    L = c.map(inc, range(15, 25), workers='127.0.0.3', allow_other_workers=True)
    yield wait(L)
    assert all(s.tasks[f.key].who_has for f in L)
    assert {f.key for f in L}.issubset(s.loose_restrictions)

    with pytest.raises(ValueError):
        c.submit(inc, 1, allow_other_workers=True)

    with pytest.raises(ValueError):
        c.map(inc, [1], allow_other_workers=True)

    with pytest.raises(TypeError):
        c.submit(inc, 20, workers='127.0.0.1', allow_other_workers='Hello!')

    with pytest.raises(TypeError):
        c.map(inc, [20], workers='127.0.0.1', allow_other_workers='Hello!')


@pytest.mark.skipif('True', reason='because')
def test_bad_address():
    try:
        Client('123.123.123.123:1234', timeout=0.1)
    except (IOError, gen.TimeoutError) as e:
        assert "connect" in str(e).lower()

    try:
        Client('127.0.0.1:1234', timeout=0.1)
    except (IOError, gen.TimeoutError) as e:
        assert "connect" in str(e).lower()


@gen_cluster(client=True)
def test_long_error(c, s, a, b):
    def bad(x):
        raise ValueError('a' * 100000)

    x = c.submit(bad, 10)

    try:
        yield x
    except ValueError as e:
        assert len(str(e)) < 100000

    tb = yield x.traceback()
    assert all(len(line) < 100000
               for line in concat(traceback.extract_tb(tb))
               if isinstance(line, str))


@gen_cluster(client=True)
def test_map_on_futures_with_kwargs(c, s, a, b):
    def f(x, y=10):
        return x + y

    futures = c.map(inc, range(10))
    futures2 = c.map(f, futures, y=20)
    results = yield c.gather(futures2)
    assert results == [i + 1 + 20 for i in range(10)]

    future = c.submit(inc, 100)
    future2 = c.submit(f, future, y=200)
    result = yield future2
    assert result == 100 + 1 + 200


class BadlySerializedObject(object):
    def __getstate__(self):
        return 1

    def __setstate__(self, state):
        raise TypeError("hello!")


class FatallySerializedObject(object):
    def __getstate__(self):
        return 1

    def __setstate__(self, state):
        print("This should never have been deserialized, closing")
        import sys
        sys.exit(0)


@gen_cluster(client=True)
def test_badly_serialized_input(c, s, a, b):
    o = BadlySerializedObject()

    future = c.submit(inc, o)
    futures = c.map(inc, range(10))

    L = yield c.gather(futures)
    assert list(L) == list(map(inc, range(10)))
    assert future.status == 'error'


@pytest.mark.skipif('True', reason="")
def test_badly_serialized_input_stderr(capsys, loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            o = BadlySerializedObject()
            future = c.submit(inc, o)

            start = time()
            while True:
                sleep(0.01)
                out, err = capsys.readouterr()
                if 'hello!' in err:
                    break
                assert time() - start < 20
            assert future.status == 'error'


def test_repr(loop):
    funcs = [str, repr, lambda x: x._repr_html_()]
    with cluster(nworkers=3) as (s, [a, b, c]):
        with Client(s['address'], loop=loop) as c:
            for func in funcs:
                text = func(c)
                assert c.scheduler.address in text
                assert '3' in text

        for func in funcs:
            text = func(c)
            assert 'not connected' in text


@gen_cluster(client=True)
def test_forget_simple(c, s, a, b):
    x = c.submit(inc, 1, retries=2)
    y = c.submit(inc, 2)
    z = c.submit(add, x, y, workers=[a.ip], allow_other_workers=True)

    yield wait([x, y, z])
    assert not s.waiting_data.get(x.key)
    assert not s.waiting_data.get(y.key)

    assert set(s.tasks) == {x.key, y.key, z.key}

    s.client_releases_keys(keys=[x.key], client=c.id)
    assert x.key in s.tasks
    s.client_releases_keys(keys=[z.key], client=c.id)

    assert x.key not in s.tasks
    assert z.key not in s.tasks
    assert not s.tasks[y.key].dependents

    s.client_releases_keys(keys=[y.key], client=c.id)
    assert not s.tasks


@gen_cluster(client=True)
def test_forget_complex(e, s, A, B):
    a, b, c, d = yield e.scatter(list(range(4)))
    ab = e.submit(add, a, b)
    cd = e.submit(add, c, d)
    ac = e.submit(add, a, c)
    acab = e.submit(add, ac, ab)

    yield wait([a, b, c, d, ab, ac, cd, acab])

    assert set(s.tasks) == {f.key for f in [ab, ac, cd, acab, a, b, c, d]}

    s.client_releases_keys(keys=[ab.key], client=e.id)
    assert set(s.tasks) == {f.key for f in [ab, ac, cd, acab, a, b, c, d]}

    s.client_releases_keys(keys=[b.key], client=e.id)
    assert set(s.tasks) == {f.key for f in [ac, cd, acab, a, c, d]}

    s.client_releases_keys(keys=[acab.key], client=e.id)
    assert set(s.tasks) == {f.key for f in [ac, cd, a, c, d]}
    assert b.key not in s.tasks

    start = time()
    while b.key in A.data or b.key in B.data:
        yield gen.sleep(0.01)
        assert time() < start + 10

    s.client_releases_keys(keys=[ac.key], client=e.id)
    assert set(s.tasks) == {f.key for f in [cd, a, c, d]}


@gen_cluster(client=True)
def test_forget_in_flight(e, s, A, B):
    delayed2 = partial(delayed, pure=True)
    a, b, c, d = [delayed2(slowinc)(i) for i in range(4)]
    ab = delayed2(slowadd)(a, b, dask_key_name="ab")
    cd = delayed2(slowadd)(c, d, dask_key_name="cd")
    ac = delayed2(slowadd)(a, c, dask_key_name="ac")
    acab = delayed2(slowadd)(ac, ab, dask_key_name="acab")

    x, y = e.compute([ac, acab])
    s.validate_state()

    for i in range(5):
        yield gen.sleep(0.01)
        s.validate_state()

    s.client_releases_keys(keys=[y.key], client=e.id)
    s.validate_state()

    for k in [acab.key, ab.key, b.key]:
        assert k not in s.tasks


@gen_cluster(client=True)
def test_forget_errors(c, s, a, b):
    x = c.submit(div, 1, 0)
    y = c.submit(inc, x)
    z = c.submit(inc, y)
    yield wait([y])

    assert x.key in s.exceptions
    assert x.key in s.exceptions_blame
    assert y.key in s.exceptions_blame
    assert z.key in s.exceptions_blame

    s.client_releases_keys(keys=[z.key], client=c.id)

    assert x.key in s.exceptions
    assert x.key in s.exceptions_blame
    assert y.key in s.exceptions_blame
    assert z.key not in s.exceptions_blame

    s.client_releases_keys(keys=[x.key], client=c.id)

    assert x.key in s.exceptions
    assert x.key in s.exceptions_blame
    assert y.key in s.exceptions_blame
    assert z.key not in s.exceptions_blame

    s.client_releases_keys(keys=[y.key], client=c.id)

    assert x.key not in s.exceptions
    assert x.key not in s.exceptions_blame
    assert y.key not in s.exceptions_blame
    assert z.key not in s.exceptions_blame


def test_repr_sync(loop):
    with cluster(nworkers=3) as (s, [a, b, c]):
        with Client(s['address'], loop=loop) as c:
            s = str(c)
            r = repr(c)
            assert c.scheduler.address in s
            assert c.scheduler.address in r
            assert str(3) in s  # nworkers
            assert 'cores' in s


@gen_cluster(client=True)
def test_waiting_data(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, 2)
    z = c.submit(add, x, y, workers=[a.ip], allow_other_workers=True)

    yield wait([x, y, z])

    assert not s.waiting_data.get(x.key)
    assert not s.waiting_data.get(y.key)


@gen_cluster()
def test_multi_client(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)

    f = yield Client((s.ip, s.port), asynchronous=True)

    assert set(s.client_comms) == {c.id, f.id}

    x = c.submit(inc, 1)
    y = f.submit(inc, 2)
    y2 = c.submit(inc, 2)

    assert y.key == y2.key

    yield wait([x, y])

    assert s.wants_what == {c.id: {x.key, y.key},
                            f.id: {y.key},
                            'fire-and-forget': set()}
    assert s.who_wants == {x.key: {c.id}, y.key: {c.id, f.id}}

    yield c.close()

    start = time()
    while c.id in s.wants_what:
        yield gen.sleep(0.01)
        assert time() < start + 5

    assert c.id not in s.wants_what
    assert c.id not in s.who_wants[y.key]
    assert x.key not in s.who_wants

    yield f.close()

    assert not s.tasks


def long_running_client_connection(address):
    from distributed.utils_test import pristine_loop
    with pristine_loop():
        c = Client(address)
        x = c.submit(lambda x: x + 1, 10)
        x.result()
        sleep(100)


@gen_cluster()
def test_cleanup_after_broken_client_connection(s, a, b):
    proc = mp_context.Process(target=long_running_client_connection,
                              args=(s.address,))
    proc.daemon = True
    proc.start()

    start = time()
    while not s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 5

    proc.terminate()

    start = time()
    while s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster()
def test_multi_garbage_collection(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)

    f = yield Client((s.ip, s.port), asynchronous=True)

    x = c.submit(inc, 1)
    y = f.submit(inc, 2)
    y2 = c.submit(inc, 2)

    assert y.key == y2.key

    yield wait([x, y])

    x.__del__()
    start = time()
    while x.key in a.data or x.key in b.data:
        yield gen.sleep(0.01)
        assert time() < start + 5

    assert s.wants_what == {c.id: {y.key},
                            f.id: {y.key},
                            'fire-and-forget': set()}
    assert s.who_wants == {y.key: {c.id, f.id}}

    y.__del__()
    start = time()
    while x.key in s.wants_what[f.id]:
        yield gen.sleep(0.01)
        assert time() < start + 5

    yield gen.sleep(0.1)
    assert y.key in a.data or y.key in b.data
    assert s.wants_what == {c.id: {y.key},
                            f.id: set(),
                            'fire-and-forget': set()}
    assert s.who_wants == {y.key: {c.id}}

    y2.__del__()
    start = time()
    while y.key in a.data or y.key in b.data:
        yield gen.sleep(0.01)
        assert time() < start + 5

    assert not any(v for v in s.wants_what.values())
    assert not s.who_wants

    yield c.close()
    yield f.close()


@gen_cluster(client=True)
def test__broadcast(c, s, a, b):
    x, y = yield c.scatter([1, 2], broadcast=True)
    assert a.data == b.data == {x.key: 1, y.key: 2}


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test__broadcast_integer(c, s, *workers):
    x, y = yield c.scatter([1, 2], broadcast=2)
    assert len(s.tasks[x.key].who_has) == 2
    assert len(s.tasks[y.key].who_has) == 2


@gen_cluster(client=True)
def test__broadcast_dict(c, s, a, b):
    d = yield c.scatter({'x': 1}, broadcast=True)
    assert a.data == b.data == {'x': 1}


def test_broadcast(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x, y = c.scatter([1, 2], broadcast=True)

            has_what = sync(c.loop, c.scheduler.has_what)

            assert {k: set(v) for k, v in has_what.items()} == {
                a['address']: {x.key, y.key},
                b['address']: {x.key, y.key}}

            [z] = c.scatter([3], broadcast=True, workers=[a['address']])

            has_what = sync(c.loop, c.scheduler.has_what)
            assert {k: set(v) for k, v in has_what.items()} == {
                a['address']: {x.key, y.key, z.key},
                b['address']: {x.key, y.key}}


@gen_cluster(client=True)
def test__cancel(c, s, a, b):
    x = c.submit(slowinc, 1)
    y = c.submit(slowinc, x)

    while y.key not in s.tasks:
        yield gen.sleep(0.01)

    yield c.cancel([x])

    assert x.cancelled()
    assert 'cancel' in str(x)
    s.validate_state()

    start = time()
    while not y.cancelled():
        yield gen.sleep(0.01)
        assert time() < start + 5

    assert not s.tasks
    s.validate_state()


@gen_cluster(client=True)
def test__cancel_tuple_key(c, s, a, b):
    x = c.submit(inc, 1, key=('x', 0, 1))

    result = yield x
    yield c.cancel(x)
    with pytest.raises(CancelledError):
        yield x


@gen_cluster()
def test__cancel_multi_client(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)
    f = yield Client((s.ip, s.port), asynchronous=True)

    x = c.submit(slowinc, 1)
    y = f.submit(slowinc, 1)

    assert x.key == y.key

    yield c.cancel([x])

    assert x.cancelled()
    assert not y.cancelled()

    start = time()
    while y.key not in s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 5

    out = yield y
    assert out == 2

    with pytest.raises(CancelledError):
        yield x

    yield c.close()
    yield f.close()


@gen_cluster(client=True)
def test__cancel_collection(c, s, a, b):
    import dask.bag as db

    L = c.map(double, [[1], [2], [3]])
    x = db.Bag({('b', i): f for i, f in enumerate(L)}, 'b', 3)

    yield c.cancel(x)
    yield c.cancel([x])
    assert all(f.cancelled() for f in L)
    assert not s.tasks


def test_cancel(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(slowinc, 1, key='x')
            y = c.submit(slowinc, x, key='y')
            z = c.submit(slowinc, y, key='z')

            c.cancel([y])

            start = time()
            while not z.cancelled():
                sleep(0.01)
                assert time() < start + 5

            assert x.result() == 2

            z.cancel()
            assert z.cancelled()


@gen_cluster(client=True)
def test_future_type(c, s, a, b):
    x = c.submit(inc, 1)
    yield wait([x])
    assert x.type == int
    assert 'int' in str(x)


@gen_cluster(client=True)
def test_traceback_clean(c, s, a, b):
    x = c.submit(div, 1, 0)
    try:
        yield x
    except Exception as e:
        f = e
        exc_type, exc_value, tb = sys.exc_info()
        while tb:
            assert 'scheduler' not in tb.tb_frame.f_code.co_filename
            assert 'worker' not in tb.tb_frame.f_code.co_filename
            tb = tb.tb_next


@gen_cluster(client=True)
def test_map_queue(c, s, a, b):
    from distributed.compatibility import Queue, isqueue
    q_1 = Queue(maxsize=2)
    q_2 = c.map(inc, q_1)
    assert isqueue(q_2)
    assert not q_2.maxsize
    q_3 = c.map(double, q_2, maxsize=3)
    assert isqueue(q_3)
    assert q_3.maxsize == 3
    q_4 = yield c._gather(q_3)
    assert isqueue(q_4)

    q_1.put(1)

    f = q_4.get()
    assert isinstance(f, Future)
    result = yield f
    assert result == (1 + 1) * 2


@gen_cluster(client=True)
def test_map_iterator_with_return(c, s, a, b):
    def g():
        yield 1
        yield 2
        raise StopIteration(3)  # py2.7 compat.
    f1 = c.map(lambda x: x, g())
    assert isinstance(f1, Iterator)

    start = time()  # ensure that we compute eagerly
    while not s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 5

    g1 = g()
    try:
        while True:
            f = next(f1)
            n = yield f
            assert n == next(g1)
    except StopIteration as e:
        with pytest.raises(StopIteration) as exc_info:
            next(g1)
        assert e.args == exc_info.value.args


@gen_cluster(client=True)
def test_map_iterator(c, s, a, b):
    x = iter([1, 2, 3])
    y = iter([10, 20, 30])
    f1 = c.map(add, x, y)
    assert isinstance(f1, Iterator)

    start = time()  # ensure that we compute eagerly
    while not s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 5

    f2 = c.map(double, f1)
    assert isinstance(f2, Iterator)

    future = next(f2)
    result = yield future
    assert result == (1 + 10) * 2
    futures = list(f2)
    results = []
    for f in futures:
        r = yield f
        results.append(r)
    assert results == [(2 + 20) * 2, (3 + 30) * 2]

    items = enumerate(range(10))
    futures = c.map(lambda x: x, items)
    assert isinstance(futures, Iterator)

    result = yield next(futures)
    assert result == (0, 0)
    futures_l = list(futures)
    results = []
    for f in futures_l:
        r = yield f
        results.append(r)
    assert results == [(i, i) for i in range(1, 10)]


@gen_cluster(client=True)
def test_map_infinite_iterators(c, s, a, b):
    futures = c.map(add, [1, 2], itertools.repeat(10))
    assert len(futures) == 2


def test_map_iterator_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            items = enumerate(range(10))
            futures = c.map(lambda x: x, items)
            next(futures).result() == (0, 0)


@gen_cluster(client=True)
def test_map_differnet_lengths(c, s, a, b):
    assert len(c.map(add, [1, 2], [1, 2, 3])) == 2


def test_Future_exception_sync_2(loop, capsys):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            ensure_default_get(c)
            ensure_default_get(c)
            ensure_default_get(c)
            ensure_default_get(c)
            assert _globals['get'] == c.get

    out, err = capsys.readouterr()
    assert len(out.strip().split('\n')) == 1

    assert _globals.get('get') != c.get


@gen_cluster(timeout=60, client=True)
def test_async_persist(c, s, a, b):
    from dask.delayed import delayed, Delayed
    x = delayed(1)
    y = delayed(inc)(x)
    z = delayed(dec)(x)
    w = delayed(add)(y, z)

    yy, ww = c.persist([y, w])
    assert type(yy) == type(y)
    assert type(ww) == type(w)
    assert len(yy.dask) == 1
    assert len(ww.dask) == 1
    assert len(w.dask) > 1
    assert y.__dask_keys__() == yy.__dask_keys__()
    assert w.__dask_keys__() == ww.__dask_keys__()

    while y.key not in s.tasks and w.key not in s.tasks:
        yield gen.sleep(0.01)

    assert s.who_wants[y.key] == {c.id}
    assert s.who_wants[w.key] == {c.id}

    yyf, wwf = c.compute([yy, ww])
    yyy, www = yield c.gather([yyf, wwf])
    assert yyy == inc(1)
    assert www == add(inc(1), dec(1))

    assert isinstance(c.persist(y), Delayed)
    assert isinstance(c.persist([y]), (list, tuple))


@gen_cluster(client=True)
def test__persist(c, s, a, b):
    pytest.importorskip('dask.array')
    import dask.array as da

    x = da.ones((10, 10), chunks=(5, 10))
    y = 2 * (x + 1)
    assert len(y.dask) == 6
    yy = c.persist(y)

    assert len(y.dask) == 6
    assert len(yy.dask) == 2
    assert all(isinstance(v, Future) for v in yy.dask.values())
    assert yy.__dask_keys__() == y.__dask_keys__()

    g, h = c.compute([y, yy])

    gg, hh = yield c.gather([g, h])
    assert (gg == hh).all()


def test_persist(loop):
    pytest.importorskip('dask.array')
    import dask.array as da
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = da.ones((10, 10), chunks=(5, 10))
            y = 2 * (x + 1)
            assert len(y.dask) == 6
            yy = c.persist(y)
            assert len(y.dask) == 6
            assert len(yy.dask) == 2
            assert all(isinstance(v, Future) for v in yy.dask.values())
            assert yy.__dask_keys__() == y.__dask_keys__()

            zz = yy.compute(get=c.get)
            z = y.compute(get=c.get)
            assert (zz == z).all()


@gen_cluster(timeout=60, client=True)
def test_long_traceback(c, s, a, b):
    from distributed.protocol.pickle import dumps

    n = sys.getrecursionlimit()
    sys.setrecursionlimit(500)

    try:
        x = c.submit(deep, 1000)
        yield wait([x])
        assert len(dumps(c.futures[x.key].traceback)) < 10000
        assert isinstance(c.futures[x.key].exception, RuntimeError)
    finally:
        sys.setrecursionlimit(n)


@gen_cluster(client=True)
def test_wait_on_collections(c, s, a, b):
    import dask.bag as db

    L = c.map(double, [[1], [2], [3]])
    x = db.Bag({('b', i): f for i, f in enumerate(L)}, 'b', 3)

    yield wait(x)
    assert all(f.key in a.data or f.key in b.data for f in L)


@gen_cluster(client=True)
def test_futures_of(c, s, a, b):
    x, y, z = c.map(inc, [1, 2, 3])

    assert set(futures_of(0)) == set()
    assert set(futures_of(x)) == {x}
    assert set(futures_of([x, y, z])) == {x, y, z}
    assert set(futures_of([x, [y], [[z]]])) == {x, y, z}
    assert set(futures_of({'x': x, 'y': [y]})) == {x, y}

    import dask.bag as db
    b = db.Bag({('b', i): f for i, f in enumerate([x, y, z])}, 'b', 3)
    assert set(futures_of(b)) == {x, y, z}


def test_futures_of_class():
    da = pytest.importorskip('dask.array')
    assert futures_of([da.Array]) == []


@gen_cluster(client=True)
def test_futures_of_cancelled_raises(c, s, a, b):
    x = c.submit(inc, 1)
    yield c.cancel([x])

    with pytest.raises(CancelledError):
        yield x

    with pytest.raises(CancelledError):
        yield c.get({'x': (inc, x), 'y': (inc, 2)}, ['x', 'y'], sync=False)

    with pytest.raises(CancelledError):
        c.submit(inc, x)

    with pytest.raises(CancelledError):
        c.submit(add, 1, y=x)

    with pytest.raises(CancelledError):
        c.map(add, [1], y=x)

    assert 'y' not in s.tasks


@pytest.mark.skip
@gen_cluster(ncores=[('127.0.0.1', 1)], client=True)
def test_dont_delete_recomputed_results(c, s, w):
    x = c.submit(inc, 1)                        # compute first time
    yield wait([x])
    x.__del__()                                 # trigger garbage collection
    yield gen.moment
    xx = c.submit(inc, 1)                       # compute second time

    start = time()
    while xx.key not in w.data:                               # data shows up
        yield gen.sleep(0.01)
        assert time() < start + 1

    while time() < start + (s.delete_interval + 100) / 1000:  # and stays
        assert xx.key in w.data
        yield gen.sleep(0.01)


@gen_cluster(ncores=[], client=True)
def test_fatally_serialized_input(c, s):
    o = FatallySerializedObject()

    future = c.submit(inc, o)

    while not s.tasks:
        yield gen.sleep(0.01)


@pytest.mark.skip(reason='Use fast random selection now')
@gen_cluster(client=True)
def test_balance_tasks_by_stacks(c, s, a, b):
    x = c.submit(inc, 1)
    yield wait(x)

    y = c.submit(inc, 2)
    yield wait(y)

    assert len(a.data) == len(b.data) == 1


@gen_cluster(client=True)
def test_run(c, s, a, b):
    results = yield c.run(inc, 1)
    assert results == {a.address: 2, b.address: 2}

    results = yield c.run(inc, 1, workers=[a.address])
    assert results == {a.address: 2}

    results = yield c.run(inc, 1, workers=[])
    assert results == {}


@gen_cluster(client=True)
def test_run_handles_picklable_data(c, s, a, b):
    futures = c.map(inc, range(10))
    yield wait(futures)

    def func():
        return {}, set(), [], (), 1, 'hello', b'100'

    results = yield c.run_on_scheduler(func)
    assert results == func()

    results = yield c.run(func)
    assert results == {w.address: func() for w in [a, b]}


def test_run_sync(loop):
    def func(x, y=10):
        return x + y

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            result = c.run(func, 1, y=2)
            assert result == {a['address']: 3,
                              b['address']: 3}

            result = c.run(func, 1, y=2, workers=[a['address']])
            assert result == {a['address']: 3}


@gen_cluster(client=True)
def test_run_coroutine(c, s, a, b):
    results = yield c.run_coroutine(geninc, 1, delay=0.05)
    assert results == {a.address: 2, b.address: 2}

    results = yield c.run_coroutine(geninc, 1, delay=0.05, workers=[a.address])
    assert results == {a.address: 2}

    results = yield c.run_coroutine(geninc, 1, workers=[])
    assert results == {}

    with pytest.raises(RuntimeError) as exc_info:
        yield c.run_coroutine(throws, 1)
    assert "hello" in str(exc_info)

    if sys.version_info >= (3, 5):
        results = yield c.run_coroutine(asyncinc, 2, delay=0.01)
        assert results == {a.address: 3, b.address: 3}


def test_run_coroutine_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            result = c.run_coroutine(geninc, 2, delay=0.01)
            assert result == {a['address']: 3,
                              b['address']: 3}

            result = c.run_coroutine(geninc, 2,
                                     workers=[a['address']])
            assert result == {a['address']: 3}

            t1 = time()
            result = c.run_coroutine(geninc, 2, delay=10, wait=False)
            t2 = time()
            assert result is None
            assert t2 - t1 <= 1.0


def test_run_exception(loop):
    def raise_exception(exc_type, exc_msg):
        raise exc_type(exc_msg)

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            for exc_type in [ValueError, RuntimeError]:
                with pytest.raises(exc_type) as excinfo:
                    c.run(raise_exception, exc_type, 'informative message')
                assert 'informative message' in str(excinfo.value)


def test_diagnostic_ui(loop):
    with cluster() as (s, [a, b]):
        a_addr = a['address']
        b_addr = b['address']
        with Client(s['address'], loop=loop) as c:
            d = c.ncores()
            assert d == {a_addr: 1, b_addr: 1}

            d = c.ncores([a_addr])
            assert d == {a_addr: 1}
            d = c.ncores(a_addr)
            assert d == {a_addr: 1}
            d = c.ncores(a['address'])
            assert d == {a_addr: 1}

            x = c.submit(inc, 1)
            y = c.submit(inc, 2)
            z = c.submit(inc, 3)
            wait([x, y, z])
            d = c.who_has()
            assert set(d) == {x.key, y.key, z.key}
            assert all(w in [a_addr, b_addr] for v in d.values() for w in v)
            assert all(d.values())

            d = c.who_has([x, y])
            assert set(d) == {x.key, y.key}

            d = c.who_has(x)
            assert set(d) == {x.key}

            d = c.has_what()
            assert set(d) == {a_addr, b_addr}
            assert all(k in [x.key, y.key, z.key] for v in d.values() for k in v)

            d = c.has_what([a_addr])
            assert set(d) == {a_addr}

            d = c.has_what(a_addr)
            assert set(d) == {a_addr}


def test_diagnostic_nbytes_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            incs = c.map(inc, [1, 2, 3])
            doubles = c.map(double, [1, 2, 3])
            wait(incs + doubles)

            assert c.nbytes(summary=False) == {k.key: sizeof(1)
                                               for k in incs + doubles}
            assert c.nbytes(summary=True) == {'inc': sizeof(1) * 3,
                                              'double': sizeof(1) * 3}


@gen_cluster(client=True)
def test_diagnostic_nbytes(c, s, a, b):
    incs = c.map(inc, [1, 2, 3])
    doubles = c.map(double, [1, 2, 3])
    yield wait(incs + doubles)

    assert s.get_nbytes(summary=False) == {k.key: sizeof(1)
                                           for k in incs + doubles}
    assert s.get_nbytes(summary=True) == {'inc': sizeof(1) * 3,
                                          'double': sizeof(1) * 3}


@gen_test()
def test_worker_aliases():
    s = Scheduler(validate=True)
    s.start(0)
    a = Worker(s.ip, s.port, name='alice')
    b = Worker(s.ip, s.port, name='bob')
    w = Worker(s.ip, s.port, name=3)
    yield [a._start(), b._start(), w._start()]

    c = yield Client((s.ip, s.port), asynchronous=True)

    L = c.map(inc, range(10), workers='alice')
    future = yield c.scatter(123, workers=3)
    yield wait(L)
    assert len(a.data) == 10
    assert len(b.data) == 0
    assert dict(w.data) == {future.key: 123}

    for i, alias in enumerate([3, [3], 'alice']):
        result = yield c.submit(lambda x: x + 1, i, workers=alias)
        assert result == i + 1

    yield c.close()
    yield [a._close(), b._close(), w._close()]
    yield s.close()


def test_persist_get_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            dadd = delayed(add)
            x, y = delayed(1), delayed(2)
            xx = delayed(add)(x, x)
            yy = delayed(add)(y, y)
            xxyy = delayed(add)(xx, yy)

            xxyy2 = c.persist(xxyy)
            xxyy3 = delayed(add)(xxyy2, 10)

            assert xxyy3.compute(get=c.get) == ((1 + 1) + (2 + 2)) + 10


@gen_cluster(client=True)
def test_persist_get(c, s, a, b):
    dadd = delayed(add)
    x, y = delayed(1), delayed(2)
    xx = delayed(add)(x, x)
    yy = delayed(add)(y, y)
    xxyy = delayed(add)(xx, yy)

    xxyy2 = c.persist(xxyy)
    xxyy3 = delayed(add)(xxyy2, 10)

    yield gen.sleep(0.5)
    result = yield c.get(xxyy3.dask, xxyy3.__dask_keys__(), sync=False)
    assert result[0] == ((1 + 1) + (2 + 2)) + 10

    result = yield c.compute(xxyy3)
    assert result == ((1 + 1) + (2 + 2)) + 10

    result = yield c.compute(xxyy3)
    assert result == ((1 + 1) + (2 + 2)) + 10

    result = yield c.compute(xxyy3)
    assert result == ((1 + 1) + (2 + 2)) + 10


@pytest.mark.skipif(sys.platform.startswith('win'),
                    reason="num_fds not supported on windows")
def test_client_num_fds(loop):
    psutil = pytest.importorskip('psutil')
    with cluster() as (s, [a, b]):
        proc = psutil.Process()
        with Client(s['address'], loop=loop) as c:  # first client to start loop
            before = proc.num_fds()                 # measure
            for i in range(4):
                with Client(s['address'], loop=loop):   # start more clients
                    pass
            after = proc.num_fds()                  # measure

        assert before == after


@gen_cluster()
def test_startup_close_startup(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)
    yield c.close()

    c = yield Client((s.ip, s.port), asynchronous=True)
    yield c.close()


def test_startup_close_startup_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            sleep(0.1)
        with Client(s['address']) as c:
            pass
        with Client(s['address']) as c:
            pass
        sleep(0.1)
        with Client(s['address']) as c:
            pass


@gen_cluster(client=True)
def test_badly_serialized_exceptions(c, s, a, b):
    def f():
        class BadlySerializedException(Exception):
            def __reduce__(self):
                raise TypeError()
        raise BadlySerializedException('hello world')

    x = c.submit(f)

    try:
        result = yield x
    except Exception as e:
        assert 'hello world' in str(e)
    else:
        assert False


@gen_cluster(client=True)
def test_rebalance(c, s, a, b):
    aws = s.workers[a.address]
    bws = s.workers[b.address]

    x, y = yield c.scatter([1, 2], workers=[a.address])
    assert len(a.data) == 2
    assert len(b.data) == 0

    s.validate_state()
    yield c.rebalance()
    s.validate_state()

    assert len(b.data) == 1
    assert {ts.key for ts in bws.has_what} == set(b.data)
    assert bws in s.tasks[x.key].who_has or bws in s.tasks[y.key].who_has

    assert len(a.data) == 1
    assert {ts.key for ts in aws.has_what} == set(a.data)
    assert (aws not in s.tasks[x.key].who_has or
            aws not in s.tasks[y.key].who_has)


@gen_cluster(ncores=[('127.0.0.1', 1)] * 4, client=True)
def test_rebalance_workers(e, s, a, b, c, d):
    w, x, y, z = yield e.scatter([1, 2, 3, 4], workers=[a.address])
    assert len(a.data) == 4
    assert len(b.data) == 0
    assert len(c.data) == 0
    assert len(d.data) == 0

    yield e.rebalance([x, y], workers=[a.address, c.address])
    assert len(a.data) == 3
    assert len(b.data) == 0
    assert len(c.data) == 1
    assert len(d.data) == 0
    assert c.data == {x.key: 2} or c.data == {y.key: 3}

    yield e.rebalance()
    assert len(a.data) == 1
    assert len(b.data) == 1
    assert len(c.data) == 1
    assert len(d.data) == 1
    s.validate_state()


@gen_cluster(client=True)
def test_rebalance_execution(c, s, a, b):
    futures = c.map(inc, range(10), workers=a.address)
    yield c.rebalance(futures)
    assert len(a.data) == len(b.data) == 5
    s.validate_state()


def test_rebalance_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.map(inc, range(10), workers=[a['address']])
            c.rebalance(futures)

            has_what = c.has_what()
            assert len(has_what) == 2
            assert list(valmap(len, has_what).values()) == [5, 5]


@gen_cluster(client=True)
def test_rebalance_unprepared(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    yield gen.sleep(0.1)
    yield c.rebalance(futures)
    s.validate_state()


@gen_cluster(client=True)
def test_receive_lost_key(c, s, a, b):
    x = c.submit(inc, 1, workers=[a.address])
    result = yield x
    yield a._close()

    start = time()
    while x.status == 'finished':
        assert time() < start + 5
        yield gen.sleep(0.01)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster([('127.0.0.1', 1), ('127.0.0.2', 2)], client=True)
def test_unrunnable_task_runs(c, s, a, b):
    x = c.submit(inc, 1, workers=[a.ip])
    result = yield x

    yield a._close()
    start = time()
    while x.status == 'finished':
        assert time() < start + 5
        yield gen.sleep(0.01)

    assert s.tasks[x.key] in s.unrunnable
    assert s.get_task_status(keys=[x.key]) == {x.key: 'no-worker'}

    w = Worker(s.ip, s.port, loop=s.loop)
    yield w._start()

    start = time()
    while x.status != 'finished':
        assert time() < start + 2
        yield gen.sleep(0.01)

    assert s.tasks[x.key] not in s.unrunnable
    result = yield x
    assert result == 2
    yield w._close()


@gen_cluster(client=True, ncores=[])
def test_add_worker_after_tasks(c, s):
    futures = c.map(inc, range(10))

    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
    n.start(0)

    result = yield c.gather(futures)

    yield n._close()


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster([('127.0.0.1', 1), ('127.0.0.2', 2)], client=True)
def test_workers_register_indirect_data(c, s, a, b):
    [x] = yield c.scatter([1], workers=a.address)
    y = c.submit(inc, x, workers=b.ip)
    yield y
    assert b.data[x.key] == 1
    assert s.tasks[x.key].who_has == {s.workers[a.address],
                                      s.workers[b.address]}
    assert s.workers[b.address].has_what == {s.tasks[x.key], s.tasks[y.key]}
    s.validate_state()


@gen_cluster(client=True)
def test_submit_on_cancelled_future(c, s, a, b):
    x = c.submit(inc, 1)
    yield x

    yield c.cancel(x)

    with pytest.raises(CancelledError):
        y = c.submit(inc, x)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_replicate(c, s, *workers):
    [a, b] = yield c.scatter([1, 2])
    yield s.replicate(keys=[a.key, b.key], n=5)
    s.validate_state()

    assert len(s.tasks[a.key].who_has) == 5
    assert len(s.tasks[b.key].who_has) == 5

    assert sum(a.key in w.data for w in workers) == 5
    assert sum(b.key in w.data for w in workers) == 5


@gen_cluster(client=True)
def test_replicate_tuple_keys(c, s, a, b):
    x = delayed(inc)(1, dask_key_name=('x', 1))
    f = c.persist(x)
    yield c.replicate(f, n=5)
    s.validate_state()
    assert a.data and b.data

    yield c.rebalance(f)
    s.validate_state()


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_replicate_workers(c, s, *workers):

    [a, b] = yield c.scatter([1, 2], workers=[workers[0].address])
    yield s.replicate(keys=[a.key, b.key], n=5,
                      workers=[w.address for w in workers[:5]])

    assert len(s.tasks[a.key].who_has) == 5
    assert len(s.tasks[b.key].who_has) == 5

    assert sum(a.key in w.data for w in workers[:5]) == 5
    assert sum(b.key in w.data for w in workers[:5]) == 5
    assert sum(a.key in w.data for w in workers[5:]) == 0
    assert sum(b.key in w.data for w in workers[5:]) == 0

    yield s.replicate(keys=[a.key, b.key], n=1)

    assert len(s.tasks[a.key].who_has) == 1
    assert len(s.tasks[b.key].who_has) == 1
    assert sum(a.key in w.data for w in workers) == 1
    assert sum(b.key in w.data for w in workers) == 1

    s.validate_state()

    yield s.replicate(keys=[a.key, b.key], n=None)  # all
    assert len(s.tasks[a.key].who_has) == 10
    assert len(s.tasks[b.key].who_has) == 10
    s.validate_state()

    yield s.replicate(keys=[a.key, b.key], n=1,
                      workers=[w.address for w in workers[:5]])
    assert sum(a.key in w.data for w in workers[:5]) == 1
    assert sum(b.key in w.data for w in workers[:5]) == 1
    assert sum(a.key in w.data for w in workers[5:]) == 5
    assert sum(b.key in w.data for w in workers[5:]) == 5
    s.validate_state()


class CountSerialization(object):
    def __init__(self):
        self.n = 0

    def __setstate__(self, n):
        self.n = n + 1

    def __getstate__(self):
        return self.n


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_replicate_tree_branching(c, s, *workers):
    obj = CountSerialization()
    [future] = yield c.scatter([obj])
    yield s.replicate(keys=[future.key], n=10)

    max_count = max(w.data[future.key].n for w in workers)
    assert max_count > 1


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_client_replicate(c, s, *workers):
    x = c.submit(inc, 1)
    y = c.submit(inc, 2)
    yield c.replicate([x, y], n=5)

    assert len(s.tasks[x.key].who_has) == 5
    assert len(s.tasks[y.key].who_has) == 5

    yield c.replicate([x, y], n=3)

    assert len(s.tasks[x.key].who_has) == 3
    assert len(s.tasks[y.key].who_has) == 3

    yield c.replicate([x, y])
    s.validate_state()

    assert len(s.tasks[x.key].who_has) == 10
    assert len(s.tasks[y.key].who_has) == 10


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, ncores=[('127.0.0.1', 1),
                                  ('127.0.0.2', 1),
                                  ('127.0.0.2', 1)], timeout=None)
def test_client_replicate_host(client, s, a, b, c):
    aws = s.workers[a.address]
    bws = s.workers[b.address]
    cws = s.workers[c.address]

    x = client.submit(inc, 1, workers='127.0.0.2')
    yield wait([x])
    assert (s.tasks[x.key].who_has == {bws} or
            s.tasks[x.key].who_has == {cws})

    yield client.replicate([x], workers=['127.0.0.2'])
    assert s.tasks[x.key].who_has == {bws, cws}

    yield client.replicate([x], workers=['127.0.0.1'])
    assert s.tasks[x.key].who_has == {aws, bws, cws}


def test_client_replicate_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(inc, 1)
            y = c.submit(inc, 2)
            c.replicate([x, y], n=2)

            who_has = c.who_has()
            assert len(who_has[x.key]) == len(who_has[y.key]) == 2

            with pytest.raises(ValueError):
                c.replicate([x], n=0)

            assert y.result() == 3


@pytest.mark.skipif(sys.platform.startswith('win'),
                    reason="Windows timer too coarse-grained")
@gen_cluster(client=True, ncores=[('127.0.0.1', 4)] * 1)
def test_task_load_adapts_quickly(c, s, a):
    future = c.submit(slowinc, 1, delay=0.2)  # slow
    yield wait(future)
    assert 0.15 < s.task_duration['slowinc'] < 0.4

    futures = c.map(slowinc, range(10), delay=0)  # very fast
    yield wait(futures)

    assert 0 < s.task_duration['slowinc'] < 0.1


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_even_load_after_fast_functions(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)  # very fast
    y = c.submit(inc, 2, workers=b.address)  # very fast
    yield wait([x, y])

    futures = c.map(inc, range(2, 11))
    yield wait(futures)
    assert any(f.key in a.data for f in futures)
    assert any(f.key in b.data for f in futures)

    # assert abs(len(a.data) - len(b.data)) <= 3


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_even_load_on_startup(c, s, a, b):
    x, y = c.map(inc, [1, 2])
    yield wait([x, y])
    assert len(a.data) == len(b.data) == 1


@pytest.mark.skip
@gen_cluster(client=True, ncores=[('127.0.0.1', 2)] * 2)
def test_contiguous_load(c, s, a, b):
    w, x, y, z = c.map(inc, [1, 2, 3, 4])
    yield wait([w, x, y, z])

    groups = [set(a.data), set(b.data)]
    assert {w.key, x.key} in groups
    assert {y.key, z.key} in groups


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test_balanced_with_submit(c, s, *workers):
    L = [c.submit(slowinc, i) for i in range(4)]
    yield wait(L)
    for w in workers:
        assert len(w.data) == 1


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test_balanced_with_submit_and_resident_data(c, s, *workers):
    [x] = yield c.scatter([10], broadcast=True)
    L = [c.submit(slowinc, x, pure=False) for i in range(4)]
    yield wait(L)
    for w in workers:
        assert len(w.data) == 2


@gen_cluster(client=True, ncores=[('127.0.0.1', 20)] * 2)
def test_scheduler_saturates_cores(c, s, a, b):
    for delay in [0, 0.01, 0.1]:
        futures = c.map(slowinc, range(100), delay=delay)
        futures = c.map(slowinc, futures, delay=delay / 10)
        while not s.tasks:
            if s.tasks:
                assert all(len(p) >= 20
                           for w in s.workers.values()
                           for p in w.processing.values())
            yield gen.sleep(0.01)


@gen_cluster(client=True, ncores=[('127.0.0.1', 20)] * 2)
def test_scheduler_saturates_cores_random(c, s, a, b):
    for delay in [0, 0.01, 0.1]:
        futures = c.map(randominc, range(100), scale=0.1)
        while not s.tasks:
            if s.tasks:
                assert all(len(p) >= 20
                           for w in s.workers.values()
                           for p in w.processing.values())
            yield gen.sleep(0.01)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test_cancel_clears_processing(c, s, *workers):
    da = pytest.importorskip('dask.array')
    x = c.submit(slowinc, 1, delay=0.2)
    while not s.tasks:
        yield gen.sleep(0.01)

    yield c.cancel(x)

    start = time()
    while any(v for w in s.workers.values() for v in w.processing):
        assert time() < start + 0.2
        yield gen.sleep(0.01)
    s.validate_state()


def test_default_get():
    with cluster() as (s, [a, b]):
        pre_get = _globals.get('get')
        pre_shuffle = _globals.get('shuffle')
        with Client(s['address'], set_as_default=True) as c:
            assert _globals['get'] == c.get
            assert _globals['shuffle'] == 'tasks'

        assert _globals['get'] is pre_get
        assert _globals['shuffle'] == pre_shuffle

        c = Client(s['address'], set_as_default=False)
        assert _globals['get'] is pre_get
        assert _globals['shuffle'] == pre_shuffle
        c.close()

        c = Client(s['address'], set_as_default=True)
        assert _globals['shuffle'] == 'tasks'
        assert _globals['get'] == c.get
        c.close()
        assert _globals['get'] is pre_get
        assert _globals['shuffle'] == pre_shuffle

        with Client(s['address']) as c:
            assert _globals['get'] == c.get

        with Client(s['address'], set_as_default=False) as c:
            assert _globals['get'] != c.get
            dask.set_options(get=c.get)
            assert _globals['get'] == c.get
        assert _globals['get'] != c.get


@gen_cluster(client=True)
def test_get_processing(c, s, a, b):
    processing = yield c.scheduler.processing()
    assert processing == valmap(list, s.processing)

    futures = c.map(slowinc, range(10), delay=0.1, workers=[a.address],
                    allow_other_workers=True)

    yield gen.sleep(0.2)

    x = yield c.scheduler.processing()
    assert set(x) == {a.address, b.address}

    x = yield c.scheduler.processing(workers=[a.address])
    assert isinstance(x[a.address], list)


@gen_cluster(client=True)
def test_get_foo(c, s, a, b):
    futures = c.map(inc, range(10))
    yield wait(futures)

    x = yield c.scheduler.ncores()
    assert x == s.ncores

    x = yield c.scheduler.ncores(workers=[a.address])
    assert x == {a.address: s.ncores[a.address]}

    x = yield c.scheduler.has_what()
    assert valmap(sorted, x) == valmap(sorted, s.has_what)

    x = yield c.scheduler.has_what(workers=[a.address])
    assert valmap(sorted, x) == {a.address: sorted(s.has_what[a.address])}

    x = yield c.scheduler.nbytes(summary=False)
    assert x == s.get_nbytes(summary=False)

    x = yield c.scheduler.nbytes(keys=[futures[0].key], summary=False)
    assert x == {futures[0].key: s.tasks[futures[0].key].nbytes}

    x = yield c.scheduler.who_has()
    assert valmap(sorted, x) == valmap(sorted, s.who_has)

    x = yield c.scheduler.who_has(keys=[futures[0].key])
    assert valmap(sorted, x) == {futures[0].key: sorted(s.who_has[futures[0].key])}


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_get_foo_lost_keys(c, s, u, v, w):
    x = c.submit(inc, 1, workers=[u.address])
    y = yield c.scatter(3, workers=[v.address])
    yield wait([x, y])

    ua, va, wa = u.address, v.address, w.address

    d = yield c.scheduler.has_what()
    assert d == {ua: [x.key], va: [y.key], wa: []}
    d = yield c.scheduler.has_what(workers=[ua, va])
    assert d == {ua: [x.key], va: [y.key]}
    d = yield c.scheduler.who_has()
    assert d == {x.key: [ua], y.key: [va]}
    d = yield c.scheduler.who_has(keys=[x.key, y.key])
    assert d == {x.key: [ua], y.key: [va]}

    yield u._close()
    yield v._close()

    d = yield c.scheduler.has_what()
    assert d == {wa: []}
    d = yield c.scheduler.has_what(workers=[ua, va])
    assert d == {ua: [], va: []}
    # The scattered key cannot be recomputed so it is forgotten
    d = yield c.scheduler.who_has()
    assert d == {x.key: []}
    # ... but when passed explicitly, it is included in the result
    d = yield c.scheduler.who_has(keys=[x.key, y.key])
    assert d == {x.key: [], y.key: []}


@slow
@gen_cluster(client=True, Worker=Nanny)
def test_bad_tasks_fail(c, s, a, b):
    f = c.submit(sys.exit, 1)
    with pytest.raises(KilledWorker):
        yield f


def test_get_processing_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            processing = c.processing()
            assert not any(v for v in processing.values())

            futures = c.map(slowinc, range(10), delay=0.1,
                            workers=[a['address']],
                            allow_other_workers=False)

            sleep(0.2)

            aa = a['address']
            bb = b['address']
            processing = c.processing()

            assert set(c.processing(aa)) == {aa}
            assert set(c.processing([aa])) == {aa}

            c.cancel(futures)


def test_close_idempotent(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            c.close()
            c.close()
            c.close()


@nodebug
def test_get_returns_early(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            start = time()
            with ignoring(RuntimeError):
                result = c.get({'x': (throws, 1), 'y': (sleep, 1)}, ['x', 'y'])
            assert time() < start + 0.5
            # Futures should be released and forgotten
            wait_for(lambda: not c.futures, timeout=0.1)

            wait_for(lambda: not any(c.processing().values()), timeout=3)

            x = c.submit(inc, 1)
            x.result()

            with ignoring(RuntimeError):
                result = c.get({'x': (throws, 1), x.key: (inc, 1)}, ['x', x.key])
            assert x.key in c.futures


@slow
@gen_cluster(Worker=Nanny, client=True)
def test_Client_clears_references_after_restart(c, s, a, b):
    x = c.submit(inc, 1)
    assert x.key in c.refcount

    yield c.restart()
    assert x.key not in c.refcount

    key = x.key
    del x
    import gc
    gc.collect()
    yield gen.moment

    assert key not in c.refcount


def test_get_stops_work_after_error(loop):
    with cluster(active_rpc_timeout=10) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with pytest.raises(RuntimeError):
                c.get({'x': (throws, 1), 'y': (sleep, 1.5)}, ['x', 'y'])

            start = time()
            while any(c.processing().values()):
                sleep(0.01)
                assert time() < start + 0.5


def test_as_completed_list(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            seq = c.map(inc, iter(range(5)))
            seq2 = list(as_completed(seq))
            assert set(c.gather(seq2)) == {1, 2, 3, 4, 5}


def test_as_completed_results(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            seq = c.map(inc, range(5))
            seq2 = list(as_completed(seq, with_results=True))
            assert set(pluck(1, seq2)) == {1, 2, 3, 4, 5}
            assert set(pluck(0, seq2)) == set(seq)


@pytest.mark.parametrize('with_results', [True, False])
def test_as_completed_batches(loop, with_results):
    n = 50
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.map(slowinc, range(n), delay=0.01)
            out = []
            for batch in as_completed(futures, with_results=with_results).batches():
                assert isinstance(batch, (tuple, list))
                sleep(0.05)
                out.extend(batch)

            assert len(out) == n
            if with_results:
                assert set(pluck(1, out)) == set(range(1, n + 1))
            else:
                assert set(out) == set(futures)


def test_as_completed_next_batch(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.map(slowinc, range(2), delay=0.1)
            ac = as_completed(futures)
            assert ac.next_batch(block=False) == []
            assert set(ac.next_batch(block=True)).issubset(futures)


@gen_test()
def test_status():
    s = Scheduler()
    s.start(0)

    c = yield Client((s.ip, s.port), asynchronous=True)
    assert c.status == 'running'
    x = c.submit(inc, 1)

    yield c.close()
    assert c.status == 'closed'

    yield s.close()


@gen_cluster(client=True)
def test_persist_optimize_graph(c, s, a, b):
    i = 10
    import dask.bag as db
    for method in [c.persist, c.compute]:
        b = db.range(i, npartitions=2)
        i += 1
        b2 = b.map(inc)
        b3 = b2.map(inc)

        b4 = method(b3, optimize_graph=False)
        yield wait(b4)

        assert set(map(tokey, b3.__dask_keys__())).issubset(s.tasks)

        b = db.range(i, npartitions=2)
        i += 1
        b2 = b.map(inc)
        b3 = b2.map(inc)

        b4 = method(b3, optimize_graph=True)
        yield wait(b4)

        assert not any(tokey(k) in s.tasks for k in b2.__dask_keys__())


@gen_cluster(client=True, ncores=[])
def test_scatter_raises_if_no_workers(c, s):
    with pytest.raises(gen.TimeoutError):
        yield c.scatter([1])


@slow
def test_reconnect(loop):
    w = Worker('127.0.0.1', 9393, loop=loop)
    w.start()

    scheduler_cli = ['dask-scheduler', '--host', '127.0.0.1',
                     '--port', '9393', '--no-bokeh']
    with popen(scheduler_cli) as s:
        c = Client('127.0.0.1:9393', loop=loop)
        start = time()
        while len(c.ncores()) != 1:
            sleep(0.1)
            assert time() < start + 3

        x = c.submit(inc, 1)
        assert x.result() == 2

    start = time()
    while c.status != 'connecting':
        assert time() < start + 5
        sleep(0.01)

    with pytest.raises(Exception):
        c.ncores()

    assert x.status == 'cancelled'
    with pytest.raises(CancelledError):
        x.result()

    with popen(scheduler_cli) as s:
        start = time()
        while c.status != 'running':
            sleep(0.1)
            assert time() < start + 5
        start = time()
        while len(c.ncores()) != 1:
            sleep(0.05)
            assert time() < start + 15

        x = c.submit(inc, 1)
        assert x.result() == 2

    start = time()
    while True:
        try:
            x.result()
            assert False
        except CommClosedError:
            continue
        except CancelledError:
            break
        assert time() < start + 5
        sleep(0.1)

    sync(loop, w._close)
    c.close()


# On Python 2, heavy process spawning can deadlock (e.g. on a logging IO lock)
_params = ([(Worker, 100, 5), (Nanny, 10, 20)]
           if sys.version_info >= (3,)
           else [(Worker, 100, 5)])


@slow
@pytest.mark.skipif(sys.platform.startswith('win'),
                    reason="num_fds not supported on windows")
@pytest.mark.parametrize("worker,count,repeat", _params)
def test_open_close_many_workers(loop, worker, count, repeat):
    psutil = pytest.importorskip('psutil')
    proc = psutil.Process()

    with cluster(nworkers=0, active_rpc_timeout=20) as (s, []):
        gc.collect()
        before = proc.num_fds()
        done = Semaphore(0)
        running = weakref.WeakKeyDictionary()

        @gen.coroutine
        def start_worker(sleep, duration, repeat=1):
            for i in range(repeat):
                yield gen.sleep(sleep)
                w = worker(s['address'], loop=loop)
                running[w] = None
                yield w._start()
                addr = w.worker_address
                running[w] = addr
                yield gen.sleep(duration)
                yield w._close()
                del w
                yield gen.moment
            done.release()

        for i in range(count):
            loop.add_callback(start_worker, random.random() / 5, random.random() / 5,
                              repeat=repeat)

        with Client(s['address'], loop=loop) as c:
            sleep(1)

            for i in range(count):
                done.acquire()
                gc.collect()
                if not running:
                    break

            start = time()
            while c.ncores():
                sleep(0.2)
                assert time() < start + 10

    start = time()
    while proc.num_fds() > before:
        print("fds:", before, proc.num_fds())
        sleep(0.1)
        assert time() < start + 10


@gen_cluster(client=False, timeout=None)
def test_idempotence(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)
    f = yield Client((s.ip, s.port), asynchronous=True)

    # Submit
    x = c.submit(inc, 1)
    yield x
    log = list(s.transition_log)

    len_single_submit = len(log)  # see last assert

    y = f.submit(inc, 1)
    assert x.key == y.key
    yield y
    yield gen.sleep(0.1)
    log2 = list(s.transition_log)
    assert log == log2

    # Error
    a = c.submit(div, 1, 0)
    yield wait(a)
    assert a.status == 'error'
    log = list(s.transition_log)

    b = f.submit(div, 1, 0)
    assert a.key == b.key
    yield wait(b)
    yield gen.sleep(0.1)
    log2 = list(s.transition_log)
    assert log == log2

    s.transition_log.clear()
    # Simultaneous Submit
    d = c.submit(inc, 2)
    e = c.submit(inc, 2)
    yield wait([d, e])

    assert len(s.transition_log) == len_single_submit

    yield c.close()
    yield f.close()


def test_scheduler_info(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            info = c.scheduler_info()
            assert isinstance(info, dict)
            assert len(info['workers']) == 2


def test_write_scheduler_file(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            info = c.scheduler_info()
            with tmpfile('json') as scheduler_file:
                c.write_scheduler_file(scheduler_file)
                with Client(scheduler_file=scheduler_file) as c2:
                    info2 = c2.scheduler_info()
                    assert c.scheduler.address == c2.scheduler.address

                # test that a ValueError is raised if the scheduler_file
                # attribute is already set
                with pytest.raises(ValueError):
                    c.write_scheduler_file(scheduler_file)


def test_get_versions(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            v = c.get_versions()
            assert v['scheduler'] is not None
            assert v['client'] is not None
            assert len(v['workers']) == 2
            for k, v in v['workers'].items():
                assert v is not None

            c.get_versions(check=True)
            # smoke test for versions
            # that this does not raise


def test_threaded_get_within_distributed(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            import dask.multiprocessing
            for get in [dask.local.get_sync,
                        dask.multiprocessing.get,
                        dask.threaded.get]:
                def f():
                    return get({'x': (lambda: 1,)}, 'x')

                future = c.submit(f)
                assert future.result() == 1


@gen_cluster(client=True)
def test_lose_scattered_data(c, s, a, b):
    [x] = yield c.scatter([1], workers=a.address)

    yield a._close()
    yield gen.sleep(0.1)

    assert x.status == 'cancelled'
    assert x.key not in s.tasks


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_partially_lose_scattered_data(e, s, a, b, c):
    [x] = yield e.scatter([1], workers=a.address)
    yield e.replicate(x, n=2)

    yield a._close()
    yield gen.sleep(0.1)

    assert x.status == 'finished'
    assert s.get_task_status(keys=[x.key]) == {x.key: 'memory'}


@gen_cluster(client=True)
def test_scatter_compute_lose(c, s, a, b):
    [x] = yield c.scatter([[1, 2, 3, 4]], workers=a.address)
    y = c.submit(inc, 1, workers=b.address)

    z = c.submit(slowadd, x, y, delay=0.2)
    yield gen.sleep(0.1)

    yield a._close()

    assert x.status == 'cancelled'
    assert y.status == 'finished'
    assert z.status == 'cancelled'

    with pytest.raises(CancelledError):
        yield wait(z)


@gen_cluster(client=True)
def test_scatter_compute_store_lose(c, s, a, b):
    """
    Create irreplaceable data on one machine,
    cause a dependent computation to occur on another and complete

    Kill the machine with the irreplaceable data.  What happens to the complete
    result?  How about after it GCs and tries to come back?
    """
    x = yield c.scatter(1, workers=a.address)
    xx = c.submit(inc, x, workers=a.address)
    y = c.submit(inc, 1)

    z = c.submit(slowadd, xx, y, delay=0.2, workers=b.address)
    yield wait(z)

    yield a._close()

    start = time()
    while x.status == 'finished':
        yield gen.sleep(0.01)
        assert time() < start + 2

    # assert xx.status == 'finished'
    assert y.status == 'finished'
    assert z.status == 'finished'

    zz = c.submit(inc, z)
    yield wait(zz)

    zkey = z.key
    del z

    start = time()
    while s.get_task_status(keys=[zkey]) != {zkey: 'released'}:
        yield gen.sleep(0.01)
        assert time() < start + 2

    xxkey = xx.key
    del xx

    start = time()
    while (x.key in s.tasks and
           zkey not in s.tasks and
           xxkey not in s.tasks):
        yield gen.sleep(0.01)
        assert time() < start + 2


@gen_cluster(client=True)
def test_scatter_compute_store_lose_processing(c, s, a, b):
    """
    Create irreplaceable data on one machine,
    cause a dependent computation to occur on another and complete

    Kill the machine with the irreplaceable data.  What happens to the complete
    result?  How about after it GCs and tries to come back?
    """
    [x] = yield c.scatter([1], workers=a.address)

    y = c.submit(slowinc, x, delay=0.2)
    z = c.submit(inc, y)
    yield gen.sleep(0.1)
    yield a._close()

    start = time()
    while x.status == 'finished':
        yield gen.sleep(0.01)
        assert time() < start + 2

    assert y.status == 'cancelled'
    assert z.status == 'cancelled'


@gen_cluster(client=False)
def test_serialize_future(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)
    f = yield Client((s.ip, s.port), asynchronous=True)

    future = c.submit(lambda: 1)
    result = yield future

    with temp_default_client(f):
        future2 = pickle.loads(pickle.dumps(future))
        assert future2.client is f
        assert tokey(future2.key) in f.futures
        result2 = yield future2
        assert result == result2

    yield c.close()
    yield f.close()


@gen_cluster(client=False)
def test_temp_client(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)
    f = yield Client((s.ip, s.port), asynchronous=True)

    with temp_default_client(c):
        assert default_client() is c
        assert default_client(f) is f

    with temp_default_client(f):
        assert default_client() is f
        assert default_client(c) is c

    yield c.close()
    yield f.close()


@nodebug  # test timing is fragile
@gen_cluster(ncores=[('127.0.0.1', 1)] * 3, client=True)
def test_persist_workers(e, s, a, b, c):
    L1 = [delayed(inc)(i) for i in range(4)]
    total = delayed(sum)(L1)
    L2 = [delayed(add)(i, total) for i in L1]

    out = e.persist(L1 + L2 + [total],
                    workers={tuple(L1): a.address,
                             total: b.address,
                             tuple(L2): [c.address]},
                    allow_other_workers=L1 + [total])

    yield wait(out)
    assert all(v.key in a.data for v in L1)
    assert total.key in b.data
    assert all(v.key in c.data for v in L2)

    assert s.loose_restrictions == {total.key} | {v.key for v in L1}


@gen_cluster(ncores=[('127.0.0.1', 1)] * 3, client=True)
def test_compute_workers(e, s, a, b, c):
    L1 = [delayed(inc)(i) for i in range(4)]
    total = delayed(sum)(L1)
    L2 = [delayed(add)(i, total) for i in L1]

    out = e.compute(L1 + L2 + [total],
                    workers={tuple(L1): a.address,
                             total: b.address,
                             tuple(L2): [c.address]},
                    allow_other_workers=L1 + [total])

    yield wait(out)
    for v in L1:
        assert s.worker_restrictions[v.key] == {a.address}
    for v in L2:
        assert s.worker_restrictions[v.key] == {c.address}
    assert s.worker_restrictions[total.key] == {b.address}

    assert s.loose_restrictions == {total.key} | {v.key for v in L1}


@gen_cluster(client=True)
def test_compute_nested_containers(c, s, a, b):
    da = pytest.importorskip('dask.array')
    np = pytest.importorskip('numpy')
    x = da.ones(10, chunks=(5,)) + 1

    future = c.compute({'x': [x], 'y': 123})
    result = yield future

    assert isinstance(result, dict)
    assert (result['x'][0] == np.ones(10) + 1).all()
    assert result['y'] == 123


def test_get_restrictions():
    L1 = [delayed(inc)(i) for i in range(4)]
    total = delayed(sum)(L1)
    L2 = [delayed(add)(i, total) for i in L1]

    r1, loose = Client.get_restrictions(L2, '127.0.0.1', False)
    assert r1 == {d.key: ['127.0.0.1'] for d in L2}
    assert not loose

    r1, loose = Client.get_restrictions(L2, ['127.0.0.1'], True)
    assert r1 == {d.key: ['127.0.0.1'] for d in L2}
    assert set(loose) == {d.key for d in L2}

    r1, loose = Client.get_restrictions(L2, {total: '127.0.0.1'}, True)
    assert r1 == {total.key: ['127.0.0.1']}
    assert loose == [total.key]

    r1, loose = Client.get_restrictions(L2, {(total,): '127.0.0.1'}, True)
    assert r1 == {total.key: ['127.0.0.1']}
    assert loose == [total.key]


@gen_cluster(client=True)
def test_scatter_type(c, s, a, b):
    [future] = yield c.scatter([1])
    assert future.type == int

    d = yield c.scatter({'x': 1.0})
    assert d['x'].type == float


@gen_cluster(client=True)
def test_retire_workers(c, s, a, b):
    [x] = yield c.scatter([1], workers=a.address)

    yield s.retire_workers(workers=[a.address])
    assert b.data == {x.key: 1}
    assert s.who_has == {x.key: {b.address}}
    assert s.has_what == {b.address: {x.key}}

    assert a.address not in s.worker_info


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_retire_many_workers(c, s, *workers):
    futures = yield c.scatter(list(range(100)))

    yield s.retire_workers(workers=[w.address for w in workers[:7]])

    results = yield c.gather(futures)
    assert results == list(range(100))

    assert len(s.has_what) == len(s.ncores) == 3
    for w, keys in s.has_what.items():
        assert 20 < len(keys) < 50


@gen_cluster(client=True,
             ncores=[('127.0.0.1', 3)] * 2)
def test_weight_occupancy_against_data_movement(c, s, a, b):
    s.extensions['stealing']._pc.callback_time = 1000000
    s.task_duration['f'] = 0.01

    def f(x, y=0, z=0):
        sleep(0.01)
        return x

    y = yield c.scatter([[1, 2, 3, 4]], workers=[a.address])
    z = yield c.scatter([1], workers=[b.address])

    futures = c.map(f, [1, 2, 3, 4], y=y, z=z)

    yield wait(futures)

    assert sum(f.key in a.data for f in futures) >= 2
    assert sum(f.key in b.data for f in futures) >= 1


@gen_cluster(client=True,
             ncores=[('127.0.0.1', 1), ('127.0.0.1', 10)])
def test_distribute_tasks_by_ncores(c, s, a, b):
    s.task_duration['f'] = 0.01
    s.extensions['stealing']._pc.callback_time = 1000000

    def f(x, y=0):
        sleep(0.01)
        return x

    y = yield c.scatter([1], broadcast=True)

    futures = c.map(f, range(20), y=y)

    yield wait(futures)

    assert len(b.data) > 2 * len(a.data)


@gen_cluster(client=True)
def test_add_done_callback(c, s, a, b):
    S = set()

    def f(future):
        future.add_done_callback(g)

    def g(future):
        S.add((future.key, future.status))

    u = c.submit(inc, 1, key='u')
    v = c.submit(throws, "hello", key='v')
    w = c.submit(slowinc, 2, delay=0.3, key='w')
    x = c.submit(inc, 3, key='x')
    u.add_done_callback(f)
    v.add_done_callback(f)
    w.add_done_callback(f)

    yield wait((u, v, w, x))

    x.add_done_callback(f)

    t = time()
    while len(S) < 4 and time() - t < 2.0:
        yield gen.sleep(0.01)

    assert S == {(f.key, f.status) for f in (u, v, w, x)}


@gen_cluster(client=True)
def test_normalize_collection(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    z = delayed(inc)(y)

    yy = c.persist(y)

    zz = c.normalize_collection(z)
    assert len(z.dask) == len(y.dask) + 1

    assert isinstance(zz.dask[y.key], Future)
    assert len(zz.dask) < len(z.dask)


@gen_cluster(client=True)
def test_normalize_collection_dask_array(c, s, a, b):
    da = pytest.importorskip('dask.array')

    x = da.ones(10, chunks=(5,))
    y = x + 1
    yy = c.persist(y)

    z = y.sum()
    zdsk = dict(z.dask)
    zz = c.normalize_collection(z)
    assert z.dask == zdsk  # do not mutate input

    assert len(z.dask) > len(zz.dask)
    assert any(isinstance(v, Future) for v in zz.dask.values())

    for k, v in yy.dask.items():
        assert zz.dask[k].key == v.key

    result1 = yield c.compute(z)
    result2 = yield c.compute(zz)
    assert result1 == result2


@gen_cluster(client=True)
def test_auto_normalize_collection(c, s, a, b):
    da = pytest.importorskip('dask.array')

    x = da.ones(10, chunks=5)
    assert len(x.dask) == 2

    with dask.set_options(optimizations=[c._optimize_insert_futures]):
        y = x.map_blocks(slowinc, delay=1, dtype=x.dtype)
        yy = c.persist(y)

        yield wait(yy)

        start = time()
        future = c.compute(y.sum())
        yield future
        end = time()
        assert end - start < 1

        start = time()
        z = c.persist(y + 1)
        yield wait(z)
        end = time()
        assert end - start < 1


def test_auto_normalize_collection_sync(loop):
    da = pytest.importorskip('dask.array')
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = da.ones(10, chunks=5)

            y = x.map_blocks(slowinc, delay=1, dtype=x.dtype)
            yy = c.persist(y)

            wait(yy)

            with dask.set_options(optimizations=[c._optimize_insert_futures]):
                start = time()
                y.sum().compute()
                end = time()
                assert end - start < 1


def assert_no_data_loss(scheduler):
    for key, start, finish, recommendations, _ in scheduler.transition_log:
        if start == 'memory' and finish == 'released':
            for k, v in recommendations.items():
                assert not (k == key and v == 'waiting')


@gen_cluster(client=True, timeout=None)
def test_interleave_computations(c, s, a, b):
    import distributed
    distributed.g = s
    xs = [delayed(slowinc)(i, delay=0.02) for i in range(30)]
    ys = [delayed(slowdec)(x, delay=0.02) for x in xs]
    zs = [delayed(slowadd)(x, y, delay=0.02) for x, y in zip(xs, ys)]

    total = delayed(sum)(zs)

    future = c.compute(total)

    done = ('memory', 'released')

    yield gen.sleep(0.1)

    x_keys = [x.key for x in xs]
    y_keys = [y.key for y in ys]
    z_keys = [z.key for z in zs]

    while not s.tasks or any(w.processing for w in s.workers.values()):
        yield gen.sleep(0.05)
        x_done = sum(state in done
                     for state in s.get_task_status(keys=x_keys).values())
        y_done = sum(state in done
                     for state in s.get_task_status(keys=y_keys).values())
        z_done = sum(state in done
                     for state in s.get_task_status(keys=z_keys).values())

        assert x_done >= y_done >= z_done
        assert x_done < y_done + 10
        assert y_done < z_done + 10

    assert_no_data_loss(s)


@pytest.mark.skip(reason="Now prefer first-in-first-out")
@gen_cluster(client=True, timeout=None)
def test_interleave_computations_map(c, s, a, b):
    xs = c.map(slowinc, range(30), delay=0.02)
    ys = c.map(slowdec, xs, delay=0.02)
    zs = c.map(slowadd, xs, ys, delay=0.02)

    done = ('memory', 'released')

    x_keys = [x.key for x in xs]
    y_keys = [y.key for y in ys]
    z_keys = [z.key for z in zs]

    while not s.tasks or any(w.processing for w in s.workers.values()):
        yield gen.sleep(0.05)
        x_done = sum(state in done
                     for state in s.get_task_status(keys=x_keys).values())
        y_done = sum(state in done
                     for state in s.get_task_status(keys=y_keys).values())
        z_done = sum(state in done
                     for state in s.get_task_status(keys=z_keys).values())

        assert x_done >= y_done >= z_done
        assert x_done < y_done + 10
        assert y_done < z_done + 10


@gen_cluster(client=True)
def test_scatter_dict_workers(c, s, a, b):
    yield c.scatter({'a': 10}, workers=[a.address, b.address])
    assert 'a' in a.data or 'a' in b.data


@slow
@gen_test()
def test_client_timeout():
    loop = IOLoop.current()
    c = Client('127.0.0.1:57484', asynchronous=True)

    s = Scheduler(loop=loop)
    yield gen.sleep(4)
    try:
        s.start(('127.0.0.1', 57484))
    except EnvironmentError:  # port in use
        return

    start = time()
    while not c.scheduler_comm:
        yield gen.sleep(0.1)
        assert time() < start + 2

    yield c.close()
    yield s.close()


@gen_cluster(client=True)
def test_submit_list_kwargs(c, s, a, b):
    futures = yield c.scatter([1, 2, 3])

    def f(L=None):
        return sum(L)

    future = c.submit(f, L=futures)
    result = yield future
    assert result == 1 + 2 + 3


@gen_cluster(client=True)
def test_map_list_kwargs(c, s, a, b):
    futures = yield c.scatter([1, 2, 3])

    def f(i, L=None):
        return i + sum(L)

    futures = c.map(f, range(10), L=futures)
    results = yield c.gather(futures)
    assert results == [i + 6 for i in range(10)]


@gen_cluster(client=True)
def test_dont_clear_waiting_data(c, s, a, b):
    [x] = yield c.scatter([1])
    y = c.submit(slowinc, x, delay=0.2)
    while y.key not in s.tasks:
        yield gen.sleep(0.01)
    [x] = yield c.scatter([1])
    for i in range(5):
        assert s.waiting_data[x.key]
        yield gen.moment


@gen_cluster(client=True)
def test_get_future_error_simple(c, s, a, b):
    f = c.submit(div, 1, 0)
    yield wait(f)
    assert f.status == 'error'

    function, args, kwargs, deps = yield c._get_futures_error(f)
    # args contains only solid values, not keys
    assert function.__name__ == 'div'
    with pytest.raises(ZeroDivisionError):
        function(*args, **kwargs)


@gen_cluster(client=True)
def test_get_futures_error(c, s, a, b):
    x0 = delayed(dec)(2, dask_key_name='x0')
    y0 = delayed(dec)(1, dask_key_name='y0')
    x = delayed(div)(1, x0, dask_key_name='x')
    y = delayed(div)(1, y0, dask_key_name='y')
    tot = delayed(sum)(x, y, dask_key_name='tot')

    f = c.compute(tot)
    yield wait(f)
    assert f.status == 'error'

    function, args, kwargs, deps = yield c._get_futures_error(f)
    assert function.__name__ == 'div'
    assert args == (1, y0.key)


@gen_cluster(client=True)
def test_recreate_error_delayed(c, s, a, b):
    x0 = delayed(dec)(2)
    y0 = delayed(dec)(1)
    x = delayed(div)(1, x0)
    y = delayed(div)(1, y0)
    tot = delayed(sum)(x, y)

    f = c.compute(tot)

    assert f.status == 'pending'

    function, args, kwargs = yield c._recreate_error_locally(f)
    assert f.status == 'error'
    assert function.__name__ == 'div'
    assert args == (1, 0)
    with pytest.raises(ZeroDivisionError):
        function(*args, **kwargs)


@gen_cluster(client=True)
def test_recreate_error_futures(c, s, a, b):
    x0 = c.submit(dec, 2)
    y0 = c.submit(dec, 1)
    x = c.submit(div, 1, x0)
    y = c.submit(div, 1, y0)
    tot = c.submit(sum, x, y)
    f = c.compute(tot)

    assert f.status == 'pending'

    function, args, kwargs = yield c._recreate_error_locally(f)
    assert f.status == 'error'
    assert function.__name__ == 'div'
    assert args == (1, 0)
    with pytest.raises(ZeroDivisionError):
        function(*args, **kwargs)


@gen_cluster(client=True)
def test_recreate_error_collection(c, s, a, b):
    import dask.bag as db
    b = db.range(10, npartitions=4)
    b = b.map(lambda x: 1 / x)
    b = b.persist()
    f = c.compute(b)

    function, args, kwargs = yield c._recreate_error_locally(f)
    with pytest.raises(ZeroDivisionError):
        function(*args, **kwargs)

    dd = pytest.importorskip('dask.dataframe')
    import pandas as pd
    df = dd.from_pandas(pd.DataFrame({'a': [0, 1, 2, 3, 4]}), chunksize=2)

    def make_err(x):
        # because pandas would happily work with NaN
        if x == 0:
            raise ValueError
        return x
    df2 = df.a.map(make_err)
    f = c.compute(df2)
    function, args, kwargs = yield c._recreate_error_locally(f)
    with pytest.raises(ValueError):
        function(*args, **kwargs)

    # with persist
    df3 = c.persist(df2)
    function, args, kwargs = yield c._recreate_error_locally(df3)
    with pytest.raises(ValueError):
        function(*args, **kwargs)


def test_recreate_error_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x0 = c.submit(dec, 2)
            y0 = c.submit(dec, 1)
            x = c.submit(div, 1, x0)
            y = c.submit(div, 1, y0)
            tot = c.submit(sum, x, y)
            f = c.compute(tot)

            with pytest.raises(ZeroDivisionError) as e:
                c.recreate_error_locally(f)
            assert f.status == 'error'


def test_recreate_error_not_error(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            f = c.submit(dec, 2)
            with pytest.raises(ValueError) as e:
                c.recreate_error_locally(f)
            assert "No errored futures passed" in str(e)


@gen_cluster(client=True)
def test_retire_workers(c, s, a, b):
    assert set(s.workers) == {a.address, b.address}
    yield c.scheduler.retire_workers(workers=[a.address], close_workers=True)
    assert set(s.workers) == {b.address}

    start = time()
    while a.status != 'closed':
        yield gen.sleep(0.01)
        assert time() < start + 5


class MyException(Exception):
    pass


@gen_cluster(client=True)
def test_robust_unserializable(c, s, a, b):
    class Foo(object):
        def __getstate__(self):
            raise MyException()

    with pytest.raises(MyException):
        future = c.submit(identity, Foo())

    futures = c.map(inc, range(10))
    results = yield c.gather(futures)

    assert results == list(map(inc, range(10)))
    assert a.data and b.data


@gen_cluster(client=True)
def test_robust_undeserializable(c, s, a, b):
    class Foo(object):
        def __getstate__(self):
            return 1

        def __setstate__(self, state):
            raise MyException('hello')

    future = c.submit(identity, Foo())
    with pytest.raises(MyException):
        yield future

    futures = c.map(inc, range(10))
    results = yield c.gather(futures)

    assert results == list(map(inc, range(10)))
    assert a.data and b.data


@gen_cluster(client=True)
def test_robust_undeserializable_function(c, s, a, b):
    class Foo(object):
        def __getstate__(self):
            return 1

        def __setstate__(self, state):
            raise MyException('hello')

        def __call__(self, *args):
            return 1

    future = c.submit(Foo(), 1)
    with pytest.raises(MyException) as e:
        yield future

    futures = c.map(inc, range(10))
    results = yield c.gather(futures)

    assert results == list(map(inc, range(10)))
    assert a.data and b.data


@gen_cluster(client=True)
def test_fire_and_forget(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.1)
    import distributed

    def f(x):
        distributed.foo = 123

    try:
        fire_and_forget(c.submit(f, future))

        start = time()
        while not hasattr(distributed, 'foo'):
            yield gen.sleep(0.01)
            assert time() < start + 2
        assert distributed.foo == 123
    finally:
        del distributed.foo

    start = time()
    while len(s.tasks) > 1:
        yield gen.sleep(0.01)
        assert time() < start + 2

    assert set(s.who_wants) == {future.key}
    assert set(s.tasks) == {future.key}


@gen_cluster(client=True)
def test_fire_and_forget_err(c, s, a, b):
    fire_and_forget(c.submit(div, 1, 0))
    yield gen.sleep(0.1)

    # erred task should clear out quickly
    start = time()
    while s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 1


def test_quiet_client_close(loop):
    with captured_logger(logging.getLogger('distributed')) as logger:
        with Client(loop=loop, processes=False, threads_per_worker=4) as c:
            futures = c.map(slowinc, range(1000), delay=0.01)
            sleep(0.200)  # stop part-way
        sleep(0.5)  # let things settle

        out = logger.getvalue()
        assert not out


@gen_cluster()
def test_close(s, a, b):
    c = yield Client(s.address, asynchronous=True)
    future = c.submit(inc, 1)
    yield wait(future)
    assert c.id in s.wants_what
    yield c.close()

    start = time()
    while c.id in s.wants_what or s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 5


def test_threadsafe(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            def f(_):
                d = deque(maxlen=50)
                for i in range(100):
                    future = c.submit(inc, random.randint(0, 100))
                    d.append(future)
                    sleep(0.001)
                c.gather(list(d))
                total = c.submit(sum, list(d))
                return total.result()

            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(20) as e:
                results = list(e.map(f, range(20)))
                assert results and all(results)
                del results


@slow
def test_threadsafe_get(loop):
    da = pytest.importorskip('dask.array')
    x = da.arange(100, chunks=(10,))
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            def f(_):
                total = 0
                for i in range(20):
                    total += (x + random.randint(0, 20)).sum().compute()
                    sleep(0.001)
                return total

            from concurrent.futures import ThreadPoolExecutor
            e = ThreadPoolExecutor(30)
            results = list(e.map(f, range(30)))
            assert results and all(results)


@slow
def test_threadsafe_compute(loop):
    da = pytest.importorskip('dask.array')
    x = da.arange(100, chunks=(10,))
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            def f(_):
                total = 0
                for i in range(20):
                    future = c.compute((x + random.randint(0, 20)).sum())
                    total += future.result()
                    sleep(0.001)
                return total

            from concurrent.futures import ThreadPoolExecutor
            e = ThreadPoolExecutor(30)
            results = list(e.map(f, range(30)))
            assert results and all(results)


@gen_cluster(client=True)
def test_identity(c, s, a, b):
    assert c.id.lower().startswith('client')
    assert a.id.lower().startswith('worker')
    assert b.id.lower().startswith('worker')
    assert s.id.lower().startswith('scheduler')


@gen_cluster(client=True, ncores=[('127.0.0.1', 4)] * 2)
def test_get_client(c, s, a, b):
    assert get_client() is c
    assert c.asynchronous

    def f(x):
        client = get_client()
        future = client.submit(inc, x)
        import distributed
        assert not client.asynchronous
        assert client is distributed.tmp_client
        return future.result()

    import distributed
    distributed.tmp_client = c
    try:
        futures = c.map(f, range(5))
        results = yield c.gather(futures)
        assert results == list(map(inc, range(5)))
    finally:
        del distributed.tmp_client


def test_get_client_no_cluster():
    # Clean up any global workers added by other tests. This test requires that
    # there are no global workers.
    from distributed.worker import _global_workers
    del _global_workers[:]

    msg = 'No global client found and no address provided'
    with pytest.raises(ValueError, match=r'^{}$'.format(msg)):
        get_client()


@gen_cluster(client=True)
def test_serialize_collections(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.arange(10, chunks=(5,)).persist()

    def f(x):
        assert isinstance(x, da.Array)
        return x.sum().compute()

    future = c.submit(f, x)
    result = yield future
    assert result == sum(range(10))


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 1, timeout=100)
def test_secede_simple(c, s, a):
    def f():
        client = get_client()
        secede()
        return client.submit(inc, 1).result()

    result = yield c.submit(f)
    assert result == 2


@slow
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2, timeout=60)
def test_secede_balances(c, s, a, b):
    count = threading.active_count()

    def f(x):
        client = get_client()
        sleep(0.01)  # do some work
        secede()
        futures = client.map(slowinc, range(10), pure=False, delay=0.01)
        total = client.submit(sum, futures).result()
        return total

    futures = c.map(f, range(100))
    start = time()
    while not all(f.status == 'finished' for f in futures):
        yield gen.sleep(0.01)
        assert threading.active_count() < count + 50

    # assert 0.005 < s.task_duration['f'] < 0.1
    assert len(a.log) < 2 * len(b.log)
    assert len(b.log) < 2 * len(a.log)

    results = yield c.gather(futures)
    assert results == [sum(map(inc, range(10)))] * 100


@gen_cluster(client=True)
def test_sub_submit_priority(c, s, a, b):
    def f():
        client = get_client()
        client.submit(slowinc, 1, delay=0.2, key='slowinc')

    future = c.submit(f, key='f')
    yield gen.sleep(0.1)
    if len(s.tasks) == 2:
        assert s.priorities['f'] > s.priorities['slowinc']  # lower values schedule first


def test_get_client_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            results = c.run(lambda: get_worker().scheduler.address)
            assert results == {w['address']: s['address'] for w in [a, b]}

            results = c.run(lambda: get_client().scheduler.address)
            assert results == {w['address']: s['address'] for w in [a, b]}


@gen_cluster(client=True)
def test_serialize_collections_of_futures(c, s, a, b):
    pd = pytest.importorskip('pandas')
    dd = pytest.importorskip('dask.dataframe')
    from dask.dataframe.utils import assert_eq

    df = pd.DataFrame({'x': [1, 2, 3]})
    ddf = dd.from_pandas(df, npartitions=2).persist()
    future = yield c.scatter(ddf)

    ddf2 = yield future
    df2 = yield c.compute(ddf2)

    assert_eq(df, df2)


def test_serialize_collections_of_futures_sync(loop):
    pd = pytest.importorskip('pandas')
    dd = pytest.importorskip('dask.dataframe')
    from dask.dataframe.utils import assert_eq

    df = pd.DataFrame({'x': [1, 2, 3]})
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            ddf = dd.from_pandas(df, npartitions=2).persist()
            future = c.scatter(ddf)

            result = future.result()
            assert_eq(result.compute(), df)

            assert future.type == dd.DataFrame
            assert c.submit(lambda x, y:  assert_eq(x.compute(), y), future, df).result()


def _dynamic_workload(x, delay=0.01):
    if delay == 'random':
        sleep(random.random() / 2)
    else:
        sleep(delay)
    if x > 4:
        return 4
    secede()
    client = get_client()
    futures = client.map(_dynamic_workload, [x + i + 1 for i in range(2)],
                         pure=False, delay=delay)
    total = client.submit(sum, futures)
    return total.result()


def _test_dynamic_workloads_sync(loop, delay):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            future = c.submit(_dynamic_workload, 0, delay=delay)
            assert future.result(timeout=40) == 52


def test_dynamic_workloads_sync(loop):
    _test_dynamic_workloads_sync(loop, delay=0.02)


@slow
def test_dynamic_workloads_sync_random(loop):
    _test_dynamic_workloads_sync(loop, delay='random')


@gen_cluster(client=True)
def test_bytes_keys(c, s, a, b):
    key = b'inc-123'
    future = c.submit(inc, 1, key=key)
    result = yield future
    assert type(future.key) is bytes
    assert set(s.tasks) == {key}
    assert key in a.data or key in b.data
    assert result == 2


@gen_cluster(client=True)
def test_unicode_ascii_keys(c, s, a, b):
    # cross-version unicode type (py2: unicode, py3: str)
    uni_type = type(u"")
    key = u'inc-123'
    future = c.submit(inc, 1, key=key)
    result = yield future
    assert type(future.key) is uni_type
    assert set(s.tasks) == {key}
    assert key in a.data or key in b.data
    assert result == 2


@gen_cluster(client=True)
def test_unicode_keys(c, s, a, b):
    # cross-version unicode type (py2: unicode, py3: str)
    uni_type = type(u"")
    key = u'inc-123\u03bc'
    future = c.submit(inc, 1, key=key)
    result = yield future
    assert type(future.key) is uni_type
    assert set(s.tasks) == {key}
    assert key in a.data or key in b.data
    assert result == 2

    future2 = c.submit(inc, future)
    result2 = yield future2
    assert result2 == 3

    future3 = yield c.scatter({u'data-123': 123})
    result3 = yield future3[u'data-123']
    assert result3 == 123


def test_use_synchronous_client_in_async_context(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            @gen.coroutine
            def f():
                x = yield c.scatter(123)
                y = c.submit(inc, x)
                z = yield c.gather(y)
                raise gen.Return(z)

            z = sync(loop, f)
            assert z == 124


def test_quiet_quit_when_cluster_leaves(loop_in_thread):
    from distributed import LocalCluster

    loop = loop_in_thread
    with LocalCluster(loop=loop, scheduler_port=0, diagnostics_port=None,
                      silence_logs=False) as cluster:
        with captured_logger('distributed.comm') as sio:
            with Client(cluster, loop=loop) as client:
                futures = client.map(lambda x: x + 1, range(10))
                sleep(0.05)
                cluster.close()
                sleep(0.05)

        text = sio.getvalue()
        assert not text


def test_warn_executor(loop):
    with cluster() as (s, [a, b]):
        with warnings.catch_warnings(record=True) as record:
            with Executor(s['address'], loop=loop) as c:
                pass

        assert any('Client' in str(r.message) for r in record)


@gen_cluster([('127.0.0.1', 4)] * 2, client=True)
def test_call_stack_future(c, s, a, b):
    x = c.submit(slowdec, 1, delay=0.5)
    future = c.submit(slowinc, 1, delay=0.5)
    yield gen.sleep(0.1)
    results = yield [c.call_stack(future), c.call_stack(keys=[future.key])]
    assert all(list(first(result.values())) == [future.key] for result in results)
    assert results[0] == results[1]
    result = results[0]
    w = a if future.key in a.executing else b
    assert list(result) == [w.address]
    assert list(result[w.address]) == [future.key]
    assert 'slowinc' in str(result)
    assert 'slowdec' not in str(result)


@gen_cluster([('127.0.0.1', 4)] * 2, client=True)
def test_call_stack_all(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.5)
    yield gen.sleep(0.1)
    result = yield c.call_stack()
    w = a if a.executing else b
    assert list(result) == [w.address]
    assert list(result[w.address]) == [future.key]
    assert 'slowinc' in str(result)


@gen_cluster([('127.0.0.1', 4)] * 2, client=True)
def test_call_stack_collections(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.random.random(100, chunks=(10,)).map_blocks(slowinc, delay=0.5).persist()
    while not a.executing and not b.executing:
        yield gen.sleep(0.001)
    result = yield c.call_stack(x)
    assert result


@gen_cluster([('127.0.0.1', 4)] * 2, client=True)
def test_call_stack_collections_all(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.random.random(100, chunks=(10,)).map_blocks(slowinc, delay=0.5).persist()
    while not a.executing and not b.executing:
        yield gen.sleep(0.001)
    result = yield c.call_stack()
    assert result


@gen_cluster(client=True, worker_kwargs={'profile_cycle_interval': 100})
def test_profile(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    yield wait(futures)

    x = yield c.profile(start=time() + 10, stop=time() + 20)
    assert not x['count']

    x = yield c.profile(start=0, stop=time())
    assert x['count'] == sum(p['count'] for _, p in a.profile_history) + a.profile_recent['count']

    y = yield c.profile(start=time() - 0.300, stop=time())
    assert 0 < y['count'] < x['count']

    assert not any(p['count'] for _, p in b.profile_history)
    result = yield c.profile(workers=b.address)
    assert not result['count']


@gen_cluster(client=True, worker_kwargs={'profile_cycle_interval': 100})
def test_profile_keys(c, s, a, b):
    x = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    y = c.map(slowdec, range(10), delay=0.05, workers=a.address)
    yield wait(x + y)

    xp = yield c.profile('slowinc')
    yp = yield c.profile('slowdec')
    p = yield c.profile()

    assert p['count'] == xp['count'] + yp['count']

    with captured_logger(logging.getLogger('distributed')) as logger:
        prof = yield c.profile('does-not-exist')
        assert prof == profile.create()
    out = logger.getvalue()
    assert not out


@gen_cluster()
def test_client_with_name(s, a, b):
    with captured_logger('distributed.scheduler') as sio:
        client = yield Client(s.address, asynchronous=True, name='foo',
                              silence_logs=False)
        assert 'foo' in client.id
        yield client.close()

    text = sio.getvalue()
    assert 'foo' in text


@gen_cluster(client=True)
def test_future_defaults_to_default_client(c, s, a, b):
    x = c.submit(inc, 1)
    yield wait(x)

    future = Future(x.key)
    assert future.client is c


@gen_cluster(client=True)
def test_future_auto_inform(c, s, a, b):
    x = c.submit(inc, 1)
    yield wait(x)

    client = yield Client(s.address, asynchronous=True)
    future = Future(x.key, client)

    start = time()
    while future.status != 'finished':
        yield gen.sleep(0.01)
        assert time() < start + 1

    yield client.close()


def test_client_async_before_loop_starts():
    loop = IOLoop()
    client = Client(asynchronous=True, loop=loop)
    assert client.asynchronous
    client.close()
    # Avoid long wait for cluster close at shutdown
    loop.close()


@slow
@gen_cluster(client=True, Worker=Nanny if PY3 else Worker, timeout=60,
             ncores=[('127.0.0.1', 3)] * 2)
def test_nested_compute(c, s, a, b):
    def fib(x):
        assert get_worker().get_current_task()
        if x < 2:
            return x
        a = delayed(fib)(x - 1)
        b = delayed(fib)(x - 2)
        c = a + b
        return c.compute()

    future = c.submit(fib, 8)
    result = yield future
    assert result == 21
    assert len(s.transition_log) > 50


@gen_cluster(client=True)
def test_task_metadata(c, s, a, b):
    yield c.set_metadata('x', 1)
    result = yield c.get_metadata('x')
    assert result == 1

    future = c.submit(inc, 1)
    key = future.key
    yield wait(future)
    yield c.set_metadata(key, 123)
    result = yield c.get_metadata(key)
    assert result == 123

    del future

    while key in s.tasks:
        yield gen.sleep(0.01)

    with pytest.raises(KeyError):
        yield c.get_metadata(key)

    result = yield c.get_metadata(key, None)
    assert result is None

    yield c.set_metadata(['x', 'a'], 1)
    result = yield c.get_metadata('x')
    assert result == {'a': 1}
    yield c.set_metadata(['x', 'b'], 2)
    result = yield c.get_metadata('x')
    assert result == {'a': 1, 'b': 2}
    result = yield c.get_metadata(['x', 'a'])
    assert result == 1

    yield c.set_metadata(['x', 'a', 'c', 'd'], 1)
    result = yield c.get_metadata('x')
    assert result == {'a': {'c': {'d': 1}}, 'b': 2}


@gen_cluster(client=True)
def test_logs(c, s, a, b):
    yield wait(c.map(inc, range(5)))
    logs = yield c.get_scheduler_logs(n=5)
    assert logs

    for _, msg in logs:
        assert 'distributed.scheduler' in msg

    w_logs = yield c.get_worker_logs(n=5)
    assert set(w_logs.keys()) == {a.address, b.address}
    for log in w_logs.values():
        for _, msg in log:
            assert 'distributed.worker' in msg


@gen_cluster(client=True)
def test_avoid_delayed_finalize(c, s, a, b):
    x = delayed(inc)(1)
    future = c.compute(x)
    result = yield future
    assert result == 2
    assert list(s.tasks) == [future.key] == [x.key]


@gen_cluster()
def test_config_scheduler_address(s, a, b):
    config['scheduler-address'] = s.address
    with captured_logger('distributed.client') as sio:
        c = yield Client(asynchronous=True)
        assert c.scheduler.address == s.address

    text = sio.getvalue()
    assert s.address in text

    del config['scheduler-address']
    yield c.close()


@gen_cluster(client=True)
def test_warn_when_submitting_large_values(c, s, a, b):
    with warnings.catch_warnings(record=True) as record:
        future = c.submit(lambda x: x + 1, b'0' * 2000000)

    text = str(record[0].message)
    assert '2.00 MB' in text
    assert 'large' in text
    assert '...' in text
    assert "'000" in text
    assert "000'" in text
    assert len(text) < 2000

    with warnings.catch_warnings(record=True) as record:
        data = b'0' * 2000000
        for i in range(10):
            future = c.submit(lambda x, y: x, data, i)

    assert len(record) < 2


@gen_cluster()
def test_scatter_direct(s, a, b):
    c = yield Client(s.address, asynchronous=True, heartbeat_interval=10)

    last = s.clients[c.id].last_seen

    start = time()
    while s.clients[c.id].last_seen == last:
        yield gen.sleep(0.10)
        assert time() < start + 5

    yield c._close()


@pytest.mark.skipif(sys.version_info[0] < 3, reason="cloudpickle Py27 issue")
@gen_cluster(client=True)
def test_unhashable_function(c, s, a, b):
    d = {'a': 1}
    result = yield c.submit(d.get, 'a')
    assert result == 1


@gen_cluster()
def test_client_name(s, a, b):
    config['client-name'] = 'hello-world'
    try:
        c = yield Client(s.address, asynchronous=True)
        assert any("hello-world" in name for name in list(s.clients))
    finally:
        del config['client-name']

    yield c._close()


def test_client_doesnt_close_given_loop(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            assert c.submit(inc, 1).result() == 2
        with Client(s['address'], loop=loop) as c:
            assert c.submit(inc, 2).result() == 3


@gen_cluster(client=True, ncores=[])
def test_quiet_scheduler_loss(c, s):
    c._periodic_callbacks['scheduler-info'].interval = 10
    with captured_logger(logging.getLogger('distributed.client')) as logger:
        yield s.close()
        yield c._update_scheduler_info()
    text = logger.getvalue()
    assert "BrokenPipeError" not in text


@pytest.mark.skipif('USER' not in os.environ, reason='no USER env variable')
def test_diagnostics_link_env_variable(loop):
    pytest.importorskip('bokeh')
    from distributed.bokeh.scheduler import BokehScheduler
    with cluster(scheduler_kwargs={'services': {('bokeh', 12355): BokehScheduler}}) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            config['diagnostics-link'] = 'http://foo-{USER}:{port}/status'
            try:
                text = c._repr_html_()
                link = 'http://foo-' + os.environ['USER'] + ':12355/status'
                assert link in text
            finally:
                del config['diagnostics-link']


if sys.version_info >= (3, 5):
    from distributed.tests.py3_test_client import *  # noqa F401

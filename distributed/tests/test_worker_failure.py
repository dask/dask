from __future__ import print_function, division, absolute_import

from concurrent.futures import CancelledError
from operator import add
from time import time, sleep

from dask import delayed
import pytest
from toolz import partition_all
from tornado import gen

from distributed.executor import _wait
from distributed.utils import sync
from distributed.utils_test import (gen_cluster, cluster, inc, loop, slow, div,
        slowinc, slowadd)
from distributed import Executor, Nanny, wait


def test_submit_after_failed_worker(loop):
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            L = e.map(inc, range(10))
            wait(L)
            a['proc'].terminate()
            total = e.submit(sum, L)
            assert total.result() == sum(map(inc, range(10)))


def test_gather_after_failed_worker(loop):
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            L = e.map(inc, range(10))
            wait(L)
            a['proc'].terminate()
            result = e.gather(L)
            assert result == list(map(inc, range(10)))


@slow
def test_gather_then_submit_after_failed_workers(loop):
    with cluster(nworkers=4) as (s, [w, x, y, z]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            L = e.map(inc, range(20))
            wait(L)
            w['proc'].terminate()
            total = e.submit(sum, L)
            wait([total])

            (_, port) = first(e.scheduler.who_has[total.key])
            for d in [x, y, z]:
                if d['port'] == port:
                    d['proc'].terminate()

            result = e.gather([total])
            assert result == [sum(map(inc, range(20)))]


@gen_cluster(Worker=Nanny, timeout=60, executor=True)
def test_failed_worker_without_warning(e, s, a, b):
    L = e.map(inc, range(10))
    yield _wait(L)

    a.process.terminate()
    start = time()
    while not a.process.is_alive():
        yield gen.sleep(0.01)
        assert time() - start < 10

    yield gen.sleep(0.5)

    start = time()
    while len(s.ncores) < 2:
        yield gen.sleep(0.01)
        assert time() - start < 10

    yield _wait(L)

    L2 = e.map(inc, range(10, 20))
    yield _wait(L2)
    assert all(len(keys) > 0 for keys in s.has_what.values())
    ncores2 = s.ncores.copy()

    yield e._restart()

    L = e.map(inc, range(10))
    yield _wait(L)
    assert all(len(keys) > 0 for keys in s.has_what.values())

    assert not (set(ncores2) & set(s.ncores))  # no overlap


@gen_cluster(Worker=Nanny, executor=True)
def test_restart(e, s, a, b):
    assert s.ncores == {a.worker_address: 1, b.worker_address: 2}

    x = e.submit(inc, 1)
    y = e.submit(inc, x)
    z = e.submit(div, 1, 0)
    yield y._result()

    assert set(s.who_has) == {x.key, y.key}

    f = yield e._restart()
    assert f is e

    assert len(s.stacks) == 2
    assert len(s.processing) == 2

    assert not s.who_has

    assert x.cancelled()
    assert y.cancelled()
    assert z.cancelled()
    assert z.key not in s.exceptions

    assert not s.who_wants
    assert not s.wants_what


@gen_cluster(Worker=Nanny, executor=True)
def test_restart_cleared(e, s, a, b):
    x = 2 * delayed(1) + 1
    f = e.compute(x)
    yield _wait([f])
    assert s.released

    yield e._restart()

    for coll in [s.tasks, s.dependencies, s.dependents, s.waiting,
            s.waiting_data, s.who_has, s.restrictions, s.loose_restrictions,
            s.released, s.priority, s.exceptions, s.who_wants,
            s.exceptions_blame]:
        assert not coll


def test_restart_sync_no_center(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            x = e.submit(inc, 1)
            e.restart()
            assert x.cancelled()
            y = e.submit(inc, 2)
            assert y.result() == 3
            assert len(e.ncores()) == 2


def test_restart_sync(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            x = e.submit(div, 1, 2)
            x.result()

            assert sync(loop, e.scheduler.who_has)
            e.restart()
            assert not sync(loop, e.scheduler.who_has)
            assert x.cancelled()
            assert len(e.ncores()) == 2

            with pytest.raises(CancelledError):
                x.result()

            y = e.submit(div, 1, 3)
            assert y.result() == 1 / 3


def test_restart_fast(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            L = e.map(sleep, range(10))

            start = time()
            e.restart()
            assert time() - start < 5
            assert len(e.ncores()) == 2

            assert all(x.status == 'cancelled' for x in L)

            x = e.submit(inc, 1)
            assert x.result() == 2


@gen_cluster(Worker=Nanny, executor=True)
def test_fast_kill(e, s, a, b):
    L = e.map(sleep, range(10))

    start = time()
    yield e._restart()
    assert time() - start < 5

    assert all(x.status == 'cancelled' for x in L)

    x = e.submit(inc, 1)
    result = yield x._result()
    assert result == 2


@gen_cluster(Worker=Nanny)
def test_multiple_executors_restart(s, a, b):
    e1 = Executor((s.ip, s.port), start=False)
    yield e1._start()
    e2 = Executor((s.ip, s.port), start=False)
    yield e2._start()

    x = e1.submit(inc, 1)
    y = e2.submit(inc, 2)
    xx = yield x._result()
    yy = yield y._result()
    assert xx == 2
    assert yy == 3

    yield e1._restart()

    assert x.cancelled()
    assert y.cancelled()

    yield e1._shutdown(fast=True)
    yield e2._shutdown(fast=True)


@gen_cluster(Worker=Nanny, executor=True)
def test_forgotten_futures_dont_clean_up_new_futures(e, s, a, b):
    x = e.submit(inc, 1)
    yield e._restart()
    y = e.submit(inc, 1)
    del x
    import gc; gc.collect()
    yield gen.sleep(0.1)
    yield y._result()


@gen_cluster(executor=True, timeout=60)
def test_broken_worker_during_computation(e, s, a, b):
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
    n.start(0)

    start = time()
    while len(s.ncores) < 3:
        yield gen.sleep(0.01)
        assert time() < start + 5

    L = e.map(inc, range(256))
    for i in range(8):
        L = e.map(add, *zip(*partition_all(2, L)))

    from random import random
    yield gen.sleep(random() / 2)
    n.process.terminate()
    yield gen.sleep(random() / 2)
    n.process.terminate()

    result = yield e._gather(L)
    assert isinstance(result[0], int)

    yield n._close()


@gen_cluster(executor=True, Worker=Nanny)
def test_restart_during_computation(e, s, a, b):
    xs = [delayed(slowinc)(i, delay=0.01) for i in range(50)]
    ys = [delayed(slowinc)(i, delay=0.01) for i in xs]
    zs = [delayed(slowadd)(x, y, delay=0.01) for x, y in zip(xs, ys)]
    total = delayed(sum)(zs)
    result = e.compute(total)

    yield gen.sleep(0.5)
    assert s.rprocessing
    yield e._restart()
    assert not s.rprocessing

    assert len(s.ncores) == 2
    assert not s.task_state

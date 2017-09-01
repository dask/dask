from __future__ import print_function, division, absolute_import

from concurrent.futures import CancelledError
from operator import add
import os
from time import sleep

import pytest
from toolz import partition_all
from tornado import gen

from dask import delayed
from distributed import Client, Nanny, wait
from distributed.comm import CommClosedError
from distributed.client import wait
from distributed.metrics import time
from distributed.utils import sync, ignoring
from distributed.utils_test import (gen_cluster, cluster, inc, slow, div,
                                    slowinc, slowadd, captured_logger)
from distributed.utils_test import loop # flake8: noqa


def test_submit_after_failed_worker_sync(loop):
    with cluster(active_rpc_timeout=10) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            L = c.map(inc, range(10))
            wait(L)
            a['proc']().terminate()
            total = c.submit(sum, L)
            assert total.result() == sum(map(inc, range(10)))


@gen_cluster(client=True, timeout=60, active_rpc_timeout=10)
def test_submit_after_failed_worker_async(c, s, a, b):
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
    n.start(0)
    while len(s.workers) < 3:
        yield gen.sleep(0.1)

    L = c.map(inc, range(10))
    yield wait(L)

    s.loop.add_callback(n.kill)
    total = c.submit(sum, L)
    result = yield total
    assert result == sum(map(inc, range(10)))

    yield n._close()


@gen_cluster(client=True)
def test_submit_after_failed_worker(c, s, a, b):
    L = c.map(inc, range(10))
    yield wait(L)
    yield a._close()

    total = c.submit(sum, L)
    result = yield total
    assert result == sum(map(inc, range(10)))


def test_gather_after_failed_worker(loop):
    with cluster(active_rpc_timeout=10) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            L = c.map(inc, range(10))
            wait(L)
            a['proc']().terminate()
            result = c.gather(L)
            assert result == list(map(inc, range(10)))


@slow
def test_gather_then_submit_after_failed_workers(loop):
    with cluster(nworkers=4, active_rpc_timeout=10) as (s, [w, x, y, z]):
        with Client(s['address'], loop=loop) as c:
            L = c.map(inc, range(20))
            wait(L)
            w['proc']().terminate()
            total = c.submit(sum, L)
            wait([total])

            addr = c.who_has()[total.key][0]
            for d in [x, y, z]:
                if d['address'] == addr:
                    d['proc']().terminate()
                    break
            else:
                assert 0, "Could not find worker %r" % (addr,)

            result = c.gather([total])
            assert result == [sum(map(inc, range(20)))]


@gen_cluster(Worker=Nanny, timeout=60, client=True)
def test_failed_worker_without_warning(c, s, a, b):
    L = c.map(inc, range(10))
    yield wait(L)

    original_pid = a.pid
    with ignoring(CommClosedError):
        yield c._run(os._exit, 1, workers=[a.worker_address])
    start = time()
    while a.pid == original_pid:
        yield gen.sleep(0.01)
        assert time() - start < 10

    yield gen.sleep(0.5)

    start = time()
    while len(s.ncores) < 2:
        yield gen.sleep(0.01)
        assert time() - start < 10

    yield wait(L)

    L2 = c.map(inc, range(10, 20))
    yield wait(L2)
    assert all(len(keys) > 0 for keys in s.has_what.values())
    ncores2 = s.ncores.copy()

    yield c._restart()

    L = c.map(inc, range(10))
    yield wait(L)
    assert all(len(keys) > 0 for keys in s.has_what.values())

    assert not (set(ncores2) & set(s.ncores))  # no overlap


@gen_cluster(Worker=Nanny, client=True)
def test_restart(c, s, a, b):
    assert s.ncores == {a.worker_address: 1, b.worker_address: 2}

    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(div, 1, 0)
    yield y

    assert set(s.who_has) == {x.key, y.key}

    f = yield c._restart()
    assert f is c

    assert len(s.processing) == 2
    assert len(s.occupancy) == 2
    assert not any(s.occupancy.values())

    assert not s.who_has

    assert x.cancelled()
    assert y.cancelled()
    assert z.cancelled()
    assert z.key not in s.exceptions

    assert not s.who_wants
    assert not s.wants_what


@gen_cluster(Worker=Nanny, client=True)
def test_restart_cleared(c, s, a, b):
    x = 2 * delayed(1) + 1
    f = c.compute(x)
    yield wait([f])
    assert s.released

    yield c._restart()

    for coll in [s.tasks, s.dependencies, s.dependents, s.waiting,
                 s.waiting_data, s.who_has, s.host_restrictions,
                 s.worker_restrictions, s.loose_restrictions,
                 s.released, s.priority, s.exceptions, s.who_wants,
                 s.exceptions_blame]:
        assert not coll


def test_restart_sync_no_center(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(inc, 1)
            c.restart()
            assert x.cancelled()
            y = c.submit(inc, 2)
            assert y.result() == 3
            assert len(c.ncores()) == 2


def test_restart_sync(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(div, 1, 2)
            x.result()

            assert sync(loop, c.scheduler.who_has)
            c.restart()
            assert not sync(loop, c.scheduler.who_has)
            assert x.cancelled()
            assert len(c.ncores()) == 2

            with pytest.raises(CancelledError):
                x.result()

            y = c.submit(div, 1, 3)
            assert y.result() == 1 / 3


@gen_cluster(Worker=Nanny, client=True, timeout=20)
def test_restart_fast(c, s, a, b):
    L = c.map(sleep, range(10))

    start = time()
    yield c._restart()
    assert time() - start < 10
    assert len(s.ncores) == 2

    assert all(x.status == 'cancelled' for x in L)

    x = c.submit(inc, 1)
    result = yield x
    assert result == 2


def test_restart_fast_sync(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            L = c.map(sleep, range(10))

            start = time()
            c.restart()
            assert time() - start < 10
            assert len(c.ncores()) == 2

            assert all(x.status == 'cancelled' for x in L)

            x = c.submit(inc, 1)
            assert x.result() == 2


@gen_cluster(Worker=Nanny, client=True, timeout=20)
def test_fast_kill(c, s, a, b):
    L = c.map(sleep, range(10))

    start = time()
    yield c._restart()
    assert time() - start < 10

    assert all(x.status == 'cancelled' for x in L)

    x = c.submit(inc, 1)
    result = yield x
    assert result == 2


@gen_cluster(Worker=Nanny)
def test_multiple_clients_restart(s, a, b):
    e1 = yield Client((s.ip, s.port), asynchronous=True)
    e2 = yield Client((s.ip, s.port), asynchronous=True)

    x = e1.submit(inc, 1)
    y = e2.submit(inc, 2)
    xx = yield x
    yy = yield y
    assert xx == 2
    assert yy == 3

    yield e1._restart()

    assert x.cancelled()
    assert y.cancelled()

    yield e1._close(fast=True)
    yield e2._close(fast=True)


@gen_cluster(Worker=Nanny)
def test_restart_scheduler(s, a, b):
    import gc
    gc.collect()
    addrs = (a.worker_address, b.worker_address)
    yield s.restart()
    assert len(s.ncores) == 2
    addrs2 = (a.worker_address, b.worker_address)

    assert addrs != addrs2


@gen_cluster(Worker=Nanny, client=True)
def test_forgotten_futures_dont_clean_up_new_futures(c, s, a, b):
    x = c.submit(inc, 1)
    yield c._restart()
    y = c.submit(inc, 1)
    del x
    import gc
    gc.collect()
    yield gen.sleep(0.1)
    yield y


@gen_cluster(client=True, timeout=60, active_rpc_timeout=10)
def test_broken_worker_during_computation(c, s, a, b):
    s.allowed_failures = 100
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
    n.start(0)

    start = time()
    while len(s.ncores) < 3:
        yield gen.sleep(0.01)
        assert time() < start + 5

    L = c.map(inc, range(256))
    for i in range(8):
        L = c.map(add, *zip(*partition_all(2, L)))

    from random import random
    yield gen.sleep(random() / 2)
    with ignoring(CommClosedError):  # comm will be closed abrupty
        yield c._run(os._exit, 1, workers=[n.worker_address])
    yield gen.sleep(random() / 2)
    with ignoring(CommClosedError, EnvironmentError):  # perhaps new worker can't be contacted yet
        yield c._run(os._exit, 1, workers=[n.worker_address])

    result = yield c.gather(L)
    assert isinstance(result[0], int)

    yield n._close()


@gen_cluster(client=True, Worker=Nanny)
def test_restart_during_computation(c, s, a, b):
    xs = [delayed(slowinc)(i, delay=0.01) for i in range(50)]
    ys = [delayed(slowinc)(i, delay=0.01) for i in xs]
    zs = [delayed(slowadd)(x, y, delay=0.01) for x, y in zip(xs, ys)]
    total = delayed(sum)(zs)
    result = c.compute(total)

    yield gen.sleep(0.5)
    assert s.rprocessing
    yield c._restart()
    assert not s.rprocessing

    assert len(s.ncores) == 2
    assert not s.task_state


@gen_cluster(client=True, timeout=None)
def test_worker_who_has_clears_after_failed_connection(c, s, a, b):
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
    n.start(0)

    start = time()
    while len(s.ncores) < 3:
        yield gen.sleep(0.01)
        assert time() < start + 5

    futures = c.map(slowinc, range(20), delay=0.01)
    yield wait(futures)

    result = yield c.submit(sum, futures, workers=a.address)
    for dep in set(a.dep_state) - set(a.task_state):
        a.release_dep(dep, report=True)

    n_worker_address = n.worker_address
    with ignoring(CommClosedError):
        yield c._run(os._exit, 1, workers=[n_worker_address])

    while len(s.workers) > 2:
        yield gen.sleep(0.01)

    total = c.submit(sum, futures, workers=a.address)
    yield total

    assert not a.has_what.get(n_worker_address)
    assert not any(n_worker_address in s for s in a.who_has.values())

    yield n._close()


@slow
@gen_cluster(client=True, timeout=60, Worker=Nanny, ncores=[('127.0.0.1', 1)])
def test_restart_timeout_on_long_running_task(c, s, a):
    with captured_logger('distributed.scheduler') as sio:
        future = c.submit(sleep, 3600)
        yield gen.sleep(0.1)
        yield c.restart(timeout=20)

    text = sio.getvalue()
    assert 'timeout' not in text.lower()

from __future__ import print_function, division, absolute_import

from concurrent.futures import CancelledError
from datetime import timedelta
from operator import add
import os
from time import sleep

import pytest
from toolz import partition_all
from tornado import gen

from dask import delayed
from distributed import Client, Nanny, wait
from distributed.compatibility import PY3
from distributed.client import _wait
from distributed.core import coerce_to_address
from distributed.metrics import time
from distributed.nanny import isalive
from distributed.utils import sync, ignoring
from distributed.utils_test import (gen_cluster, cluster, inc, loop, slow, div,
        slowinc, slowadd)


def test_submit_after_failed_worker(loop):
    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            L = c.map(inc, range(10))
            wait(L)
            a['proc'].terminate()
            total = c.submit(sum, L)
            assert total.result() == sum(map(inc, range(10)))


def test_gather_after_failed_worker(loop):
    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            L = c.map(inc, range(10))
            wait(L)
            a['proc'].terminate()
            result = c.gather(L)
            assert result == list(map(inc, range(10)))


@slow
def test_gather_then_submit_after_failed_workers(loop):
    with cluster(nworkers=4) as (s, [w, x, y, z]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            L = c.map(inc, range(20))
            wait(L)
            w['proc'].terminate()
            total = c.submit(sum, L)
            wait([total])

            addr = c.who_has()[total.key][0]
            _, port = coerce_to_address(addr, out=tuple)
            for d in [x, y, z]:
                if d['port'] == port:
                    d['proc'].terminate()
                    break
            else:
                assert 0, "Could not find worker"

            result = c.gather([total])
            assert result == [sum(map(inc, range(20)))]


@gen_cluster(Worker=Nanny, timeout=60, client=True)
def test_failed_worker_without_warning(c, s, a, b):
    L = c.map(inc, range(10))
    yield _wait(L)

    original_process = a.process
    a.process.terminate()
    start = time()
    while a.process is original_process and not isalive(a.process):
        yield gen.sleep(0.01)
        assert time() - start < 10

    yield gen.sleep(0.5)

    start = time()
    while len(s.ncores) < 2:
        yield gen.sleep(0.01)
        assert time() - start < 10

    yield _wait(L)

    L2 = c.map(inc, range(10, 20))
    yield _wait(L2)
    assert all(len(keys) > 0 for keys in s.has_what.values())
    ncores2 = s.ncores.copy()

    yield c._restart()

    L = c.map(inc, range(10))
    yield _wait(L)
    assert all(len(keys) > 0 for keys in s.has_what.values())

    assert not (set(ncores2) & set(s.ncores))  # no overlap


@gen_cluster(Worker=Nanny, client=True)
def test_restart(c, s, a, b):
    assert s.ncores == {a.worker_address: 1, b.worker_address: 2}

    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(div, 1, 0)
    yield y._result()

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
    yield _wait([f])
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
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            x = c.submit(inc, 1)
            c.restart()
            assert x.cancelled()
            y = c.submit(inc, 2)
            assert y.result() == 3
            assert len(c.ncores()) == 2


def test_restart_sync(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
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
    result = yield x._result()
    assert result == 2


def test_restart_fast_sync(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
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
    result = yield x._result()
    assert result == 2


@gen_cluster(Worker=Nanny)
def test_multiple_clients_restart(s, a, b):
    e1 = Client((s.ip, s.port), start=False)
    yield e1._start()
    e2 = Client((s.ip, s.port), start=False)
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


@gen_cluster(Worker=Nanny)
def test_restart_scheduler(s, a, b):
    import gc; gc.collect()
    yield s.restart()
    assert len(s.ncores) == 2


@gen_cluster(Worker=Nanny, client=True)
def test_forgotten_futures_dont_clean_up_new_futures(c, s, a, b):
    x = c.submit(inc, 1)
    yield c._restart()
    y = c.submit(inc, 1)
    del x
    import gc; gc.collect()
    yield gen.sleep(0.1)
    yield y._result()


@gen_cluster(client=True, timeout=60, active_rpc_timeout=10)
def test_broken_worker_during_computation(c, s, a, b):
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
    with ignoring(OSError):
        n.process.terminate()
    yield gen.sleep(random() / 2)
    with ignoring(OSError):
        n.process.terminate()

    result = yield c._gather(L)
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


@pytest.mark.skipif(not os.path.exists('myenv.zip') or not PY3,
                    reason='Depends on large local file')
@gen_cluster(client=True, Worker=Nanny, timeout=120)
def test_upload_environment(c, s, a, b):
    responses = yield c._upload_environment('myenv.zip')
    assert os.path.exists(os.path.join(a.local_dir, 'myenv'))
    assert os.path.exists(os.path.join(b.local_dir, 'myenv'))


@pytest.mark.skipif(not os.path.exists('myenv.zip') or not PY3,
                    reason='Depends on large local file')
@gen_cluster(client=True, Worker=Nanny, timeout=120)
def test_restart_environment(c, s, a, b):
    yield c._restart(environment='myenv.zip')

    def get_executable():
        import sys
        return sys.executable

    results = yield c._run(get_executable)
    assert results == {n.worker_address:
                        os.path.join(n.local_dir, 'myenv', 'bin', 'python')
                        for n in [a, b]}

from __future__ import print_function, division, absolute_import

from concurrent.futures import CancelledError
from datetime import timedelta
from operator import add
import random
import sys
from time import sleep

from dask import delayed
import pytest
from toolz import concat, sliding_window, first

from distributed import Client, wait, Nanny
from distributed.config import config
from distributed.metrics import time
from distributed.utils import All
from distributed.utils_test import (gen_cluster, cluster, inc, slowinc, loop,
        slowadd, slow, slowsum)
from distributed.client import _wait
from tornado import gen


@gen_cluster(client=True)
def test_stress_1(c, s, a, b):
    n = 2**6

    seq = c.map(inc, range(n))
    while len(seq) > 1:
        yield gen.sleep(0.1)
        seq = [c.submit(add, seq[i], seq[i + 1])
                for i in range(0, len(seq), 2)]
    result = yield seq[0]
    assert result == sum(map(inc, range(n)))


@pytest.mark.parametrize(('func', 'n'), [(slowinc, 100), (inc, 1000)])
def test_stress_gc(loop, func, n):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(func, 1)
            for i in range(n):
                x = c.submit(func, x)

            assert x.result() == n + 2


@pytest.mark.skipif(sys.platform.startswith('win'),
                    reason="test can leave dangling RPC objects")
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4, timeout=None)
def test_cancel_stress(c, s, *workers):
    da = pytest.importorskip('dask.array')
    x = da.random.random((40, 40), chunks=(1, 1))
    x = c.persist(x)
    yield _wait([x])
    y = (x.sum(axis=0) + x.sum(axis=1) + 1).std()
    for i in range(5):
        f = c.compute(y)
        while len(s.waiting) > (len(y.dask) - len(x.dask)) / 2:
            yield gen.sleep(0.01)
        yield c._cancel(f)


def test_cancel_stress_sync(loop):
    da = pytest.importorskip('dask.array')
    x = da.random.random((40, 40), chunks=(1, 1))
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.persist(x)
            y = (x.sum(axis=0) + x.sum(axis=1) + 1).std()
            wait(x)
            for i in range(5):
                f = c.compute(y)
                sleep(1)
                c.cancel(f)


@gen_cluster(ncores=[], client=True, timeout=None)
def test_stress_creation_and_deletion(c, s):
    # Assertions are handled by the validate mechanism in the scheduler
    s.allowed_failures = 100000
    da = pytest.importorskip('dask.array')

    x = da.random.random(size=(2000, 2000), chunks=(100, 100))
    y = (x + 1).T + (x * 2) - x.mean(axis=1)

    z = c.persist(y)

    @gen.coroutine
    def create_and_destroy_worker(delay):
        start = time()
        while time() < start + 10:
            n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
            n.start(0)

            yield gen.sleep(delay)

            yield n._close()
            print("Killed nanny")

    yield gen.with_timeout(timedelta(minutes=1),
                          All([create_and_destroy_worker(0.1 * i) for i in
                              range(10)]))


@gen_cluster(ncores=[('127.0.0.1', 1)] * 10, client=True, timeout=60)
def test_stress_scatter_death(c, s, *workers):
    import random
    s.allowed_failures = 1000
    np = pytest.importorskip('numpy')
    L = yield c._scatter([np.random.random(10000) for i in range(len(workers))])
    yield c._replicate(L, n=2)

    adds = [delayed(slowadd, pure=True)(random.choice(L),
                                        random.choice(L),
                                        delay=0.05)
            for i in range(50)]

    adds = [delayed(slowadd, pure=True)(a, b, delay=0.02)
            for a, b in sliding_window(2, adds)]

    futures = c.compute(adds)

    alive = list(workers)

    from distributed.scheduler import logger

    for i in range(7):
        yield gen.sleep(0.1)
        try:
            s.validate_state()
        except Exception as c:
            logger.exception(c)
            if config.get('log-on-err'):
                import pdb; pdb.set_trace()
            else:
                raise
        w = random.choice(alive)
        yield w._close()
        alive.remove(w)

    try:
        yield gen.with_timeout(timedelta(seconds=25), c._gather(futures))
    except gen.TimeoutError:
        ws = {w.address: w for w in workers if w.status != 'closed'}
        print(s.processing)
        print(ws)
        print(futures)
        try:
            worker = [w for w in ws.values() if w.waiting_for_data][0]
        except:
            pass
        if config.get('log-on-err'):
            import pdb; pdb.set_trace()
        else:
            raise
    except CancelledError:
        pass


def vsum(*args):
    return sum(args)


@pytest.mark.avoid_travis
@slow
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 80, timeout=1000)
def test_stress_communication(c, s, *workers):
    s.validate = False # very slow otherwise
    da = pytest.importorskip('dask.array')
    # Test consumes many file descriptors and can hang if the limit is too low
    resource = pytest.importorskip('resource')
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        lim = 8192
        if soft < lim:
            resource.setrlimit(resource.RLIMIT_NOFILE, (lim, max(hard, lim)))
    except Exception as e:
        pytest.skip("file descriptor limit too low and can't be increased :"
                    + str(e))

    n = 20
    xs = [da.random.random((100, 100), chunks=(5, 5)) for i in range(n)]
    ys = [x + x.T for x in xs]
    z = da.atop(vsum, 'ij', *concat(zip(ys, ['ij'] * n)), dtype='float64')

    future = c.compute(z.sum())

    result = yield future
    assert isinstance(result, float)


@pytest.mark.skip
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10, timeout=60)
def test_stress_steal(c, s, *workers):
    s.validate = False
    for w in workers:
        w.validate = False

    dinc = delayed(slowinc)
    L = [delayed(slowinc)(i, delay=0.005) for i in range(100)]
    for i in range(5):
        L = [delayed(slowsum)(part, delay=0.005)
             for part in sliding_window(5, L)]

    total = delayed(sum)(L)
    future = c.compute(total)

    while future.status != 'finished':
        yield gen.sleep(0.1)
        for i in range(3):
            a = random.choice(workers)
            b = random.choice(workers)
            if a is not b:
                s.work_steal(a.address, b.address, 0.5)
        if not s.processing:
            break


@slow
@gen_cluster(ncores=[('127.0.0.1', 1)] * 10, client=True, timeout=120)
def test_close_connections(c, s, *workers):
    da = pytest.importorskip('dask.array')
    x = da.random.random(size=(1000, 1000), chunks=(1000, 1))
    for i in range(3):
        x = x.rechunk((1, 1000))
        x = x.rechunk((1000, 1))

    future = c.compute(x.sum())
    while any(s.processing.values()):
        yield gen.sleep(0.5)
        worker = random.choice(list(workers))
        for comm in worker._comms:
            comm.abort()
        # print(frequencies(s.task_state.values()))
        # for w in workers:
        #     print(w)

    yield _wait(future)

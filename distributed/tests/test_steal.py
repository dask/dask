from __future__ import print_function, division, absolute_import

import sys

import pytest
from toolz import sliding_window
from tornado import gen

import dask
from dask import delayed
from distributed.client import Client, _wait, wait
from distributed.utils_test import (cluster, slowinc, slowadd, randominc,
        loop, inc, dec, div, throws, gen_cluster, gen_test, double, deep)

import pytest


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, ncores=[('127.0.0.1', 2), ('127.0.0.2', 2)],
        timeout=20)
def test_work_stealing(c, s, a, b):
    [x] = yield c._scatter([1])
    futures = c.map(slowadd, range(50), [x] * 50)
    yield _wait(futures)
    assert len(a.data) > 10
    assert len(b.data) > 10


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_dont_steal_expensive_data_fast_computation(c, s, a, b):
    np = pytest.importorskip('numpy')
    x = c.submit(np.arange, 1000000, workers=a.address)
    yield _wait([x])
    future = c.submit(np.sum, [1], workers=a.address)  # learn that sum is fast
    yield _wait([future])

    cheap = [c.submit(np.sum, x, pure=False, workers=a.address,
                      allow_other_workers=True) for i in range(10)]
    yield _wait(cheap)
    assert len(b.data) == 0
    assert len(a.data) == 12


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_steal_cheap_data_slow_computation(c, s, a, b):
    x = c.submit(slowinc, 100, delay=0.1)  # learn that slowinc is slow
    yield _wait([x])

    futures = c.map(slowinc, range(10), delay=0.01, workers=a.address,
                    allow_other_workers=True)
    yield _wait(futures)
    assert abs(len(a.data) - len(b.data)) <= 3


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_steal_expensive_data_slow_computation(c, s, a, b):
    np = pytest.importorskip('numpy')

    x = c.submit(slowinc, 100, delay=0.1, workers=a.address)
    yield _wait([x])  # learn that slowinc is slow

    x = c.submit(np.arange, 1000000, workers=a.address)  # put expensive data
    yield _wait([x])

    slow = [c.submit(slowinc, x, delay=0.1, pure=False) for i in range(4)]
    yield _wait([slow])

    assert b.data  # not empty


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_worksteal_many_thieves(c, s, *workers):
    x = c.submit(slowinc, -1, delay=0.1)
    yield x._result()

    xs = c.map(slowinc, [x] * 100, pure=False, delay=0.01)

    yield _wait(xs)

    for w, keys in s.has_what.items():
        assert 2 < len(keys) < 50


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_dont_steal_unknown_functions(c, s, a, b):
    futures = c.map(inc, [1, 2], workers=a.address, allow_other_workers=True)
    yield _wait(futures)
    assert len(a.data) == 2
    assert len(b.data) == 0


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_eventually_steal_unknown_functions(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1,  workers=a.address,
                    allow_other_workers=True)
    yield _wait(futures)
    assert len(a.data) >= 3
    assert len(b.data) >= 3


@pytest.mark.xfail(reason='')
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_steal_related_tasks(e, s, a, b, c):
    futures = e.map(slowinc, range(20), delay=0.05, workers=a.address,
                    allow_other_workers=True)

    yield _wait(futures)

    nearby = 0
    for f1, f2 in sliding_window(2, futures):
        if s.who_has[f1.key] == s.who_has[f2.key]:
            nearby += 1

    assert nearby > 10


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10, timeout=1000)
def test_dont_steal_fast_tasks(c, s, *workers):
    np = pytest.importorskip('numpy')
    x = c.submit(np.random.random, 10000000, workers=workers[0].address)

    def do_nothing(x, y=None):
        pass

    futures = c.map(do_nothing, range(1000), y=x)

    yield _wait(futures)

    assert len(s.has_what[workers[0].address]) == 1001

"""
Various functional tests for TLS networking.
Most are taken from other test files and adapted.
"""

from __future__ import print_function, division, absolute_import

from operator import add
from time import sleep

import pytest
from toolz import take
from tornado import gen

from distributed import Nanny, worker_client
from distributed.client import _wait
from distributed.metrics import time
from distributed.utils_test import (gen_cluster, tls_only_security,
                                    inc, double, slowinc, slowadd)


def gen_tls_cluster(**kwargs):
    kwargs.setdefault('ncores', [('tls://127.0.0.1', 1), ('tls://127.0.0.1', 2)])
    return gen_cluster(scheduler='tls://127.0.0.1',
                       security=tls_only_security(), **kwargs)


@gen_tls_cluster(client=True)
def test_channel(c, s, a, b):
    assert s.address.startswith('tls://')

    x = c.channel('x')
    y = c.channel('y')

    assert len(x) == 0

    future = c.submit(inc, 1)

    x.append(future)

    while not x.data:
        yield gen.sleep(0.01)

    assert len(x) == 1


@gen_tls_cluster(client=True, timeout=None)
def test_client_submit(c, s, a, b):
    assert s.address.startswith('tls://')

    x = c.submit(inc, 10)
    result = yield x
    assert result == 11

    yy = [c.submit(slowinc, i) for i in range(10)]
    results = []
    for y in yy:
        results.append((yield y))
    assert results == list(range(1, 11))


@gen_tls_cluster(client=True)
def test_gather(c, s, a, b):
    assert s.address.startswith('tls://')

    x = c.submit(inc, 10)
    y = c.submit(inc, x)

    result = yield c._gather(x)
    assert result == 11
    result = yield c._gather([x])
    assert result == [11]
    result = yield c._gather({'x': x, 'y': [y]})
    assert result == {'x': 11, 'y': [12]}


@gen_tls_cluster(client=True)
def test_scatter(c, s, a, b):
    assert s.address.startswith('tls://')

    d = yield c._scatter({'y': 20})
    assert s.who_has['y']
    assert s.nbytes['y'] > 0
    yy = yield c._gather([d['y']])
    assert yy == [20]


@gen_tls_cluster(client=True, Worker=Nanny)
def test_nanny(c, s, a, b):
    assert s.address.startswith('tls://')
    for n in [a, b]:
        assert isinstance(n, Nanny)
        assert n.address.startswith('tls://')
        assert n.worker_address.startswith('tls://')
    assert s.ncores == {n.worker_address: n.ncores for n in [a, b]}

    x = c.submit(inc, 10)
    result = yield x
    assert result == 11


@gen_tls_cluster(client=True)
def test_rebalance(c, s, a, b):
    x, y = yield c._scatter([1, 2], workers=[a.address])
    assert len(a.data) == 2
    assert len(b.data) == 0

    yield c._rebalance()

    assert len(a.data) == 1
    assert len(b.data) == 1


@gen_tls_cluster(client=True, ncores=[('tls://127.0.0.1', 1)] * 2)
def test_work_stealing(c, s, a, b):
    [x] = yield c._scatter([1], workers=a.address)
    futures = c.map(slowadd, range(50), [x] * 50)
    yield gen.sleep(0.1)
    yield _wait(futures)
    assert len(a.data) > 10
    assert len(b.data) > 10
    assert len(a.data) > len(b.data) - 5


@gen_tls_cluster(client=True)
def test_worker_client(c, s, a, b):
    def func(x):
        with worker_client() as c:
            x = c.submit(inc, x)
            y = c.submit(double, x)
            result = x.result() + y.result()
            return result

    x, y = c.map(func, [10, 20])
    xx, yy = yield c._gather([x, y])

    assert xx == 10 + 1 + (10 + 1) * 2
    assert yy == 20 + 1 + (20 + 1) * 2


@gen_tls_cluster(client=True, ncores=[('tls://127.0.0.1', 1)] * 2)
def test_worker_client_gather(c, s, a, b):
    a_address = a.address
    b_address = b.address
    assert a_address.startswith('tls://')
    assert b_address.startswith('tls://')
    assert a_address != b_address

    def func():
        with worker_client() as ee:
            x = ee.submit(inc, 1, workers=a_address)
            y = ee.submit(inc, 2, workers=b_address)

            xx, yy = ee.gather([x, y])
        return xx, yy

    future = c.submit(func)
    result = yield future

    assert result == (2, 3)


@gen_tls_cluster(client=True)
def test_worker_client_executor(c, s, a, b):
    def mysum():
        with worker_client() as c:
            with c.get_executor() as e:
                return sum(e.map(double, range(30)))

    future = c.submit(mysum)
    result = yield future
    assert result == 30 * 29

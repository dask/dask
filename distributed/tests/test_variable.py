from __future__ import print_function, division, absolute_import

from operator import add
from time import sleep
import sys

import pytest
from toolz import take
from tornado import gen

from distributed import Client, Variable, worker_client, Nanny
from distributed.metrics import time
from distributed.utils_test import (gen_cluster, inc, loop, cluster, slowinc,
                                    slow)


@gen_cluster(client=True)
def test_variable(c, s, a, b):
    x = Variable('x')
    xx = Variable('x')
    assert x.client is c

    future = c.submit(inc, 1)

    yield x.set(future)
    future2 = yield xx.get()
    assert future.key == future2.key

    del future, future2

    yield gen.sleep(0.1)
    assert s.task_state  # future still present

    x.delete()

    start = time()
    while s.task_state:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True)
def test_queue_with_data(c, s, a, b):
    x = Variable('x')
    xx = Variable('x')
    assert x.client is c

    yield x.set([1, 'hello'])
    data = yield xx.get()

    assert data == [1, 'hello']


def test_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            future = c.submit(lambda x: x + 1, 10)
            x = Variable('x')
            xx = Variable('x')
            x.set(future)
            future2 = xx.get()

            assert future2.result() == 11


@gen_cluster()
def test_hold_futures(s, a, b):
    c1 = yield Client(s.address, asynchronous=True)
    future = c1.submit(lambda x: x + 1, 10)
    x1 = Variable('x')
    yield x1.set(future)
    del x1
    yield c1.shutdown()

    yield gen.sleep(0.1)

    c2 = yield Client(s.address, asynchronous=True)
    x2 = Variable('x')
    future2 = yield x2.get()
    result = yield future2

    assert result == 11
    yield c2.shutdown()


@gen_cluster(client=True)
def test_timeout(c, s, a, b):
    v = Variable('v')

    start = time()
    with pytest.raises(gen.TimeoutError):
        yield v.get(timeout=0.1)
    assert time() - start < 0.5


@gen_cluster(client=True)
def test_cleanup(c, s, a, b):
    v = Variable('v')
    vv = Variable('v')

    x = c.submit(lambda x: x + 1, 10)
    y = c.submit(lambda x: x + 1, 20)
    x_key = x.key

    yield v.set(x)
    del x
    yield gen.sleep(0.1)

    t_future = xx = vv._get()
    yield gen.moment
    v._set(y)

    future = yield t_future
    assert future.key == x_key
    result = yield future
    assert result == 11


def test_pickleable(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            v = Variable('v')

            def f(x):
                v.set(x + 1)

            c.submit(f, 10).result()
            assert v.get() == 11


@gen_cluster(client=True)
def test_timeout_get(c, s, a, b):
    v = Variable('v')

    tornado_future = v.get()

    vv = Variable('v')
    yield vv.set(1)

    result = yield tornado_future
    assert result == 1


@pytest.mark.skipif(sys.version_info[0] == 2, reason='Multi-client issues')
@slow
@gen_cluster(client=True, ncores=[('127.0.0.1', 2)] * 5, Worker=Nanny,
             timeout=None)
def test_race(c, s, *workers):
    def f(i):
        with worker_client() as c:
            v = Variable('x', client=c)
            for _ in range(100):
                future = v.get()
                x = future.result()
                y = c.submit(inc, x)
                v.set(y)
                sleep(0.01)
            result = v.get().result()
            sleep(0.1)  # allow fire-and-forget messages to clear
            return result

    v = Variable('x', client=c)
    x = yield c.scatter(1)
    yield v.set(x)

    futures = c.map(f, range(10))
    results = yield c.gather(futures)
    assert all(r > 80 for r in results)

    start = time()
    while len(s.wants_what['variable-x']) != 1:
        yield gen.sleep(0.01)
        if not time() - start < 2:
            import pdb; pdb.set_trace()

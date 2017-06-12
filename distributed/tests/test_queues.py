from __future__ import print_function, division, absolute_import

from operator import add
from time import sleep
import sys

import pytest
from toolz import take
from tornado import gen

from distributed import Client, Queue, Nanny, worker_client
from distributed.metrics import time
from distributed.utils_test import (gen_cluster, inc, loop, cluster, slowinc,
                                    slow)


@gen_cluster(client=True)
def test_queue(c, s, a, b):
    x = yield Queue('x')
    y = yield Queue('y')
    xx = yield Queue('x')
    assert x.client is c

    future = c.submit(inc, 1)

    yield x.put(future)
    yield y.put(future)
    future2 = yield xx.get()
    assert future.key == future2.key

    with pytest.raises(gen.TimeoutError):
        yield x.get(timeout=0.1)

    del future, future2

    yield gen.sleep(0.1)
    assert s.task_state  # future still present in y's queue
    yield y.get()  # burn future

    start = time()
    while s.task_state:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True)
def test_queue_with_data(c, s, a, b):
    x = yield Queue('x')
    xx = yield Queue('x')
    assert x.client is c

    yield x.put([1, 'hello'])
    data = yield xx.get()

    assert data == [1, 'hello']

    with pytest.raises(gen.TimeoutError):
        yield x.get(timeout=0.1)


def test_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            future = c.submit(lambda x: x + 1, 10)
            x = Queue('x')
            xx = Queue('x')
            x.put(future)
            assert x.qsize() == 1
            assert xx.qsize() == 1
            future2 = xx.get()

            assert future2.result() == 11


@gen_cluster()
def test_hold_futures(s, a, b):
    c1 = yield Client(s.address, asynchronous=True)
    future = c1.submit(lambda x: x + 1, 10)
    q1 = yield Queue('q')
    yield q1.put(future)
    del q1
    yield c1.shutdown()

    yield gen.sleep(0.1)

    c2 = yield Client(s.address, asynchronous=True)
    q2 = yield Queue('q')
    future2 = yield q2.get()
    result = yield future2

    assert result == 11
    yield c2.shutdown()


@pytest.mark.skip(reason='getting same client from main thread')
@gen_cluster(client=True)
def test_picklability(c, s, a, b):
    q = Queue()

    def f(x):
        q.put(x + 1)

    yield c.submit(f, 10)
    result = yield q.get()
    assert result == 11


def test_picklability_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            q = Queue()

            def f(x):
                q.put(x + 1)

            c.submit(f, 10).result()

            assert q.get() == 11


@pytest.mark.skipif(sys.version_info[0] == 2, reason='Multi-client issues')
@slow
@gen_cluster(client=True, ncores=[('127.0.0.1', 2)] * 5, Worker=Nanny,
             timeout=None)
def test_race(c, s, *workers):
    def f(i):
        with worker_client() as c:
            q = Queue('x', client=c)
            for _ in range(100):
                future = q.get()
                x = future.result()
                y = c.submit(inc, x)
                q.put(y)
                sleep(0.01)
            result = q.get().result()
            return result

    q = Queue('x', client=c)
    L = yield c.scatter(range(5))
    for future in L:
        yield q.put(future)

    futures = c.map(f, range(5))
    results = yield c.gather(futures)
    assert all(r > 80 for r in results)
    qsize = yield q.qsize()
    assert not qsize


@gen_cluster(client=True)
def test_same_futures(c, s, a, b):
    q = Queue('x')
    future = yield c.scatter(123)

    for i in range(5):
        yield q.put(future)

    assert s.wants_what['queue-x'] == {future.key}

    for i in range(4):
        future2 = yield q.get()
        assert s.wants_what['queue-x'] == {future.key}
        yield gen.sleep(0.05)
        assert s.wants_what['queue-x'] == {future.key}

    yield q.get()

    start = time()
    while s.wants_what['queue-x']:
        yield gen.sleep(0.01)
        assert time() - start < 2

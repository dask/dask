from __future__ import print_function, division, absolute_import

import random
from time import sleep
import warnings

import dask
from dask import delayed
import pytest
from tornado import gen

from distributed import (worker_client, Client, as_completed, get_worker, wait,
                         get_client)
from distributed.metrics import time
from distributed.utils_test import cluster, double, gen_cluster, inc
from distributed.utils_test import loop # flake8: noqa


@gen_cluster(client=True)
def test_submit_from_worker(c, s, a, b):
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

    assert len(s.transition_log) > 10
    assert len([id for id in s.wants_what
                if id.lower().startswith('client')]) == 1


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_scatter_from_worker(c, s, a, b):
    def func():
        with worker_client() as c:
            futures = c.scatter([1, 2, 3, 4, 5])
            assert isinstance(futures, (list, tuple))
            assert len(futures) == 5

            x = dict(get_worker().data)
            y = {f.key: i for f, i in zip(futures, [1, 2, 3, 4, 5])}
            assert x == y

            total = c.submit(sum, futures)
            return total.result()

    future = c.submit(func)
    result = yield future
    assert result == sum([1, 2, 3, 4, 5])

    def func():
        with worker_client() as c:
            correct = True
            for data in [[1, 2], (1, 2), {1, 2}]:
                futures = c.scatter(data)
                correct &= type(futures) == type(data)

            o = object()
            futures = c.scatter({'x': o})
            correct &= get_worker().data['x'] is o
            return correct

    future = c.submit(func)
    result = yield future
    assert result is True

    start = time()
    while not all(v == 1 for v in s.ncores.values()):
        yield gen.sleep(0.1)
        assert time() < start + 5


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_scatter_singleton(c, s, a, b):
    np = pytest.importorskip('numpy')

    def func():
        with worker_client() as c:
            x = np.ones(5)
            future = c.scatter(x)
            assert future.type == np.ndarray

    yield c.submit(func)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_gather_multi_machine(c, s, a, b):
    a_address = a.address
    b_address = b.address
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


@gen_cluster(client=True)
def test_same_loop(c, s, a, b):
    def f():
        with worker_client() as lc:
            return lc.loop is get_worker().loop

    future = c.submit(f)
    result = yield future
    assert result


def test_sync(loop):
    def mysum():
        result = 0
        sub_tasks = [delayed(double)(i) for i in range(100)]

        with worker_client() as lc:
            futures = lc.compute(sub_tasks)
            for f in as_completed(futures):
                result += f.result()
        return result

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            assert delayed(mysum)().compute(get=c.get) == 9900


@gen_cluster(client=True)
def test_async(c, s, a, b):
    def mysum():
        result = 0
        sub_tasks = [delayed(double)(i) for i in range(100)]

        with worker_client() as lc:
            futures = lc.compute(sub_tasks)
            for f in as_completed(futures):
                result += f.result()
        return result

    future = c.compute(delayed(mysum)())
    yield future

    start = time()
    while len(a.data) + len(b.data) > 1:
        yield gen.sleep(0.1)
        assert time() < start + 3


@gen_cluster(client=True, ncores=[('127.0.0.1', 3)])
def test_separate_thread_false(c, s, a):
    a.count = 0

    def f(i):
        with worker_client(separate_thread=False) as client:
            get_worker().count += 1
            assert get_worker().count <= 3
            sleep(random.random() / 40)
            assert get_worker().count <= 3
            get_worker().count -= 1
        return i

    futures = c.map(f, range(20))
    results = yield c._gather(futures)
    assert list(results) == list(range(20))


@gen_cluster(client=True)
def test_client_executor(c, s, a, b):
    def mysum():
        with worker_client() as c:
            with c.get_executor() as e:
                return sum(e.map(double, range(30)))

    future = c.submit(mysum)
    result = yield future
    assert result == 30 * 29


def test_dont_override_default_get(loop):
    import dask.bag as db

    def f(x):
        with worker_client() as c:
            return True

    b = db.from_sequence([1, 2])
    b2 = b.map(f)

    with Client(loop=loop, processes=False, set_as_default=True) as c:
        assert dask.context._globals['get'] == c.get
        for i in range(2):
            b2.compute()

        assert dask.context._globals['get'] == c.get


@gen_cluster(client=True)
def test_local_client_warning(c, s, a, b):
    from distributed import local_client

    def func(x):
        with warnings.catch_warnings(record=True) as record:
            with local_client() as c:
                x = c.submit(inc, x)
                result = x.result()
            assert any("worker_client" in str(r.message) for r in record)
            return result

    future = c.submit(func, 10)
    result = yield future
    assert result == 11


@gen_cluster(client=True)
def test_closing_worker_doesnt_close_client(c, s, a, b):
    def func(x):
        get_client()
        return

    yield wait(c.map(func, range(10)))
    yield a._close()
    assert c.status == 'running'


def test_timeout(loop):
    def func():
        with worker_client(timeout=0) as wc:
            print('hello')

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            future = c.submit(func)
            with pytest.raises(EnvironmentError):
                result = future.result()


def test_secede_without_stealing_issue_1262():
    """
    Tests that seceding works with the Stealing extension disabled
    https://github.com/dask/distributed/issues/1262
    """

    # turn off all extensions
    extensions = []

    # run the loop as an inner function so all workers are closed
    # and exceptions can be examined
    @gen_cluster(client=True, scheduler_kwargs={'extensions': extensions})
    def secede_test(c, s, a, b):
        def func(x):
            with worker_client() as wc:
                y = wc.submit(lambda: 1 + x)
                return wc.gather(y)
        f = yield c.gather(c.submit(func, 1))

        raise gen.Return((c, s, a, b, f))

    c, s, a, b, f = secede_test()

    assert f == 2
    # ensure no workers had errors
    assert all([f.exception() is None for f in s._worker_coroutines])

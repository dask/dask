from __future__ import print_function, division, absolute_import

from datetime import timedelta

from tornado import gen

from distributed import local_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, double


@gen_cluster(client=True)
def test_submit_from_worker(c, s, a, b):
    def func(x):
        with local_client() as c:
            x = c.submit(inc, x)
            y = c.submit(double, x)
            result = x.result() + y.result()
            return result

    x, y = c.map(func, [10, 20])
    xx, yy = yield c._gather([x, y])

    assert xx == 10 + 1 + (10 + 1) * 2
    assert yy == 20 + 1 + (20 + 1) * 2

    assert len(s.transition_log) > 10
    assert len(s.wants_what) == 1


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_scatter_from_worker(c, s, a, b):
    def func():
        with local_client() as c:
            futures = c.scatter([1, 2, 3, 4, 5])
            assert isinstance(futures, (list, tuple))
            assert len(futures) == 5

            x = dict(c.worker.data)
            y = {f.key: i for f, i in zip(futures, [1, 2, 3, 4, 5])}
            assert x == y

            total = c.submit(sum, futures)
            return total.result()

    future = c.submit(func)
    result = yield future._result()
    assert result == sum([1, 2, 3, 4, 5])

    def func():
        with local_client() as c:
            correct = True
            for data in [[1, 2], (1, 2), {1, 2}]:
                futures = c.scatter(data)
                correct &= type(futures) == type(data)

            o = object()
            futures = c.scatter({'x': o})
            correct &= c.worker.data['x'] is o
            return correct

    future = c.submit(func)
    result = yield future._result()
    assert result is True

    start = time()
    while not all(v == 1 for v in s.ncores.values()):
        yield gen.sleep(0.1)
        print(s.ncores)
        assert time() < start + 5


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_gather_multi_machine(c, s, a, b):
    a_address = b.address
    b_address = b.address
    def func():
        with local_client() as ee:
            x = ee.submit(inc, 1, workers=a_address)
            y = ee.submit(inc, 2, workers=b_address)

            xx, yy = ee.gather([x, y])
        return xx, yy

    future = c.submit(func)
    result = yield future._result()

    assert result == (2, 3)

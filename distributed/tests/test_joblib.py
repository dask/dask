from __future__ import print_function, division, absolute_import

import pytest
from random import random
from time import sleep

from distributed.utils_test import inc, cluster, loop

distributed_joblib = pytest.importorskip('distributed.joblib')
joblibs = [distributed_joblib.joblib, distributed_joblib.sk_joblib]
joblib_funcname = distributed_joblib.joblib_funcname


def slow_raise_value_error(condition, duration=0.05):
    sleep(duration)
    if condition:
        raise ValueError("condition evaluated to True")


@pytest.mark.skip(reason="intermittent blocking failures")
@pytest.mark.parametrize('joblib', joblibs)
def test_simple(loop, joblib):
    if joblib is None:
        pytest.skip()
    Parallel = joblib.Parallel
    delayed = joblib.delayed
    with cluster() as (s, [a, b]):
        with joblib.parallel_backend('dask.distributed', loop=loop,
                                     scheduler_host=s['address']) as (ba, _):

            seq = Parallel()(delayed(inc)(i) for i in range(10))
            assert seq == [inc(i) for i in range(10)]

            with pytest.raises(ValueError):
                Parallel()(delayed(slow_raise_value_error)(i == 3)
                           for i in range(10))

            seq = Parallel()(delayed(inc)(i) for i in range(10))
            assert seq == [inc(i) for i in range(10)]

            ba.client.shutdown()


def random2():
    return random()


@pytest.mark.parametrize('joblib', joblibs)
def test_dont_assume_function_purity(loop, joblib):
    if joblib is None:
        pytest.skip()
    Parallel = joblib.Parallel
    delayed = joblib.delayed
    with cluster() as (s, [a, b]):
        with joblib.parallel_backend('dask.distributed', loop=loop,
                                     scheduler_host=s['address']) as (ba, _):

            x, y = Parallel()(delayed(random2)() for i in range(2))
            assert x != y

            ba.client.shutdown()


@pytest.mark.parametrize('joblib', joblibs)
def test_joblib_funcname(joblib):
    if joblib is None:
        pytest.skip()
    BatchedCalls = joblib.parallel.BatchedCalls
    func = BatchedCalls([(random2,), (random2,)])
    assert joblib_funcname(func) == 'random2'
    assert joblib_funcname(random2) == 'random2'


@pytest.mark.parametrize('joblib', joblibs)
def test_joblib_backend_subclass(joblib):
    if joblib is None:
        pytest.skip()

    assert issubclass(distributed_joblib.DaskDistributedBackend,
                      joblib.parallel.ParallelBackendBase)


def add5(a, b, c, d=0, e=0):
    return a + b + c + d + e


class CountSerialized(object):
    def __init__(self, x):
        self.x = x
        self.count = 0

    def __add__(self, other):
        return self.x + getattr(other, 'x', other)

    __radd__ = __add__

    def __reduce__(self):
        self.count += 1
        return (CountSerialized, (self.x,))


@pytest.mark.parametrize('joblib', joblibs)
def test_joblib_scatter(loop, joblib):
    if joblib is None:
        pytest.skip()

    Parallel = joblib.Parallel
    delayed = joblib.delayed

    x = CountSerialized(1)
    y = CountSerialized(2)
    z = CountSerialized(3)

    with cluster() as (s, [a, b]):
        with joblib.parallel_backend('dask.distributed', loop=loop,
                                     scheduler_host=s['address'],
                                     scatter=[x, y]) as (ba, _):
            f = delayed(add5)
            tasks = [f(x, y, z, d=4, e=5),
                     f(x, z, y, d=5, e=4),
                     f(y, x, z, d=x, e=5),
                     f(z, z, x, d=z, e=y)]
            sols = [func(*args, **kwargs) for func, args, kwargs in tasks]
            results = Parallel()(tasks)

            ba.client.shutdown()

        # Scatter must take a list/tuple
        with pytest.raises(TypeError):
            with joblib.parallel_backend('dask.distributed', loop=loop,
                                         scheduler_host=s['address'],
                                         scatter=1):
                pass

    for l, r in zip(sols, results):
        assert l == r

    # Scattered variables only serialized once
    assert x.count == 1
    assert y.count == 1
    assert z.count == 4

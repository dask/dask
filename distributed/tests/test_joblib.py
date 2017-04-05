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
                                     scheduler_host=s['address']):

            seq = Parallel()(delayed(inc)(i) for i in range(10))
            assert seq == [inc(i) for i in range(10)]

            with pytest.raises(ValueError):
                Parallel()(delayed(slow_raise_value_error)(i == 3)
                           for i in range(10))

            seq = Parallel()(delayed(inc)(i) for i in range(10))
            assert seq == [inc(i) for i in range(10)]

            ba, _ = joblib.parallel.get_active_backend()
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
                                     scheduler_host=s['address']):

            x, y = Parallel()(delayed(random2)() for i in range(2))
            assert x != y

            ba, _ = joblib.parallel.get_active_backend()
            ba.client.shutdown()


@pytest.mark.parametrize('joblib', joblibs)
def test_joblib_funcname(joblib):
    if joblib is None:
        pytest.skip()
    BatchedCalls = joblib.parallel.BatchedCalls
    func = BatchedCalls([(random2,), (random2,)])
    assert joblib_funcname(func) == 'random2'
    assert joblib_funcname(random2) == 'random2'

from __future__ import print_function, division, absolute_import

import pytest
from random import random
from time import sleep

from distributed.utils_test import inc, cluster, loop

backend = pytest.importorskip('distributed.joblib')
joblibs = [backend.joblib, backend.sk_joblib]


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
                scheduler_host=('127.0.0.1', s['port'])):

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
                scheduler_host=('127.0.0.1', s['port'])):

            x, y = Parallel()(delayed(random2)() for i in range(2))
            assert x != y

            ba, _ = joblib.parallel.get_active_backend()
            ba.client.shutdown()

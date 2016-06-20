from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('joblib')
from random import random

from joblib import parallel_backend, Parallel, delayed

import distributed.joblib
from distributed.utils_test import inc, cluster, loop
from time import sleep


def slow_raise_value_error(condition, duration=0.05):
    sleep(duration)
    if condition:
        raise ValueError("condition evaluated to True")


def test_simple(loop):
    with cluster() as (s, [a, b]):
        with parallel_backend('distributed', loop=loop,
                scheduler_host=('127.0.0.1', s['port'])) as p:

            seq = Parallel()(delayed(inc)(i) for i in range(10))
            assert seq == [inc(i) for i in range(10)]

            with pytest.raises(ValueError):
                Parallel()(delayed(slow_raise_value_error)(i == 3)
                    for i in range(10))

            seq = Parallel()(delayed(inc)(i) for i in range(10))
            assert seq == [inc(i) for i in range(10)]

            from joblib.parallel import get_active_backend
            ba, _ = get_active_backend()
            ba.executor.shutdown()


def random2():
    return random()

def test_dont_assume_function_purity(loop):
    with cluster() as (s, [a, b]):
        with parallel_backend('distributed', loop=loop,
                scheduler_host=('127.0.0.1', s['port'])) as p:

            x, y = Parallel()(delayed(random2)() for i in range(2))
            assert x != y

            from joblib.parallel import get_active_backend
            ba, _ = get_active_backend()
            ba.executor.shutdown()

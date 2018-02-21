from __future__ import print_function, division, absolute_import
import os
import importlib
from distutils.version import LooseVersion

import pytest
from random import random
from time import sleep

from distributed import Client
from distributed.utils_test import cluster, inc
from distributed.utils_test import loop # noqa F401

distributed_joblib = pytest.importorskip('distributed.joblib')
joblib_funcname = distributed_joblib.joblib_funcname


@pytest.fixture(params=['joblib', 'sk_joblib'])
def joblib(request):
    if request.param == 'joblib':
        try:
            this_joblib = importlib.import_module('joblib')
        except ImportError:
            pytest.skip("joblib not available")
    else:
        try:
            this_joblib = importlib.import_module("sklearn.externals.joblib")
        except ImportError:
            pytest.skip("sklearn.externals.joblib not available")

    return this_joblib


def slow_raise_value_error(condition, duration=0.05):
    sleep(duration)
    if condition:
        raise ValueError("condition evaluated to True")


def test_simple(loop, joblib):
    Parallel = joblib.Parallel
    delayed = joblib.delayed
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask') as (ba, _):
                seq = Parallel()(delayed(inc)(i) for i in range(10))
                assert seq == [inc(i) for i in range(10)]

                with pytest.raises(ValueError):
                    Parallel()(delayed(slow_raise_value_error)(i == 3)
                               for i in range(10))

                seq = Parallel()(delayed(inc)(i) for i in range(10))
                assert seq == [inc(i) for i in range(10)]


def random2():
    return random()


def test_dont_assume_function_purity(loop, joblib):
    Parallel = joblib.Parallel
    delayed = joblib.delayed
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask') as (ba, _):
                x, y = Parallel()(delayed(random2)() for i in range(2))
                assert x != y


def test_joblib_funcname(joblib):
    BatchedCalls = joblib.parallel.BatchedCalls
    if LooseVersion(joblib.__version__) <= "0.11.0":
        func = BatchedCalls([(random2,), (random2,)])
    else:
        func = BatchedCalls([(random2,), (random2,)], None)
    assert joblib_funcname(func) == 'random2'
    assert joblib_funcname(random2) == 'random2'


def test_joblib_backend_subclass(joblib):
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


def test_joblib_scatter(loop, joblib):
    Parallel = joblib.Parallel
    delayed = joblib.delayed

    x = CountSerialized(1)
    y = CountSerialized(2)
    z = CountSerialized(3)

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask', scatter=[x, y]) as (ba, _):
                f = delayed(add5)
                tasks = [f(x, y, z, d=4, e=5),
                         f(x, z, y, d=5, e=4),
                         f(y, x, z, d=x, e=5),
                         f(z, z, x, d=z, e=y)]
                sols = [func(*args, **kwargs) for func, args, kwargs in tasks]
                results = Parallel()(tasks)

            # Scatter must take a list/tuple
            with pytest.raises(TypeError):
                with joblib.parallel_backend('dask', loop=loop,
                                             scatter=1):
                    pass

    for l, r in zip(sols, results):
        assert l == r

    # Scattered variables only serialized once
    assert x.count == 1
    assert y.count == 1
    assert z.count == 4


def test_nested_backend_context_manager(loop, joblib):
    if LooseVersion(joblib.__version__) <= "0.11.0":
        pytest.skip("Joblib >= 0.11.1 required for nested parallelism.")
    Parallel = joblib.Parallel
    delayed = joblib.delayed

    def get_nested_pids():
        pids = set(Parallel(n_jobs=2)(delayed(os.getpid)() for _ in range(2)))
        pids |= set(Parallel(n_jobs=2)(delayed(os.getpid)() for _ in range(2)))
        return pids

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask') as (ba, _):
                pid_groups = Parallel(n_jobs=2)(
                    delayed(get_nested_pids, check_pickle=False)()
                    for _ in range(10)
                )
                for pid_group in pid_groups:
                    assert len(set(pid_group)) <= 2

        # No deadlocks
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask') as (ba, _):
                pid_groups = Parallel(n_jobs=2)(
                    delayed(get_nested_pids, check_pickle=False)()
                    for _ in range(10)
                )
                for pid_group in pid_groups:
                    assert len(set(pid_group)) <= 2


def test_errors(loop, joblib):
    with pytest.raises(ValueError) as info:
        with joblib.parallel_backend('dask'):
            pass

    assert "create a dask client" in str(info.value).lower()


def test_secede_with_no_processes(loop, joblib):
    # https://github.com/dask/distributed/issues/1775

    def f(x):
        return x

    with Client(loop=loop, processes=False, set_as_default=True):
        with joblib.parallel_backend('dask'):
            joblib.Parallel(n_jobs=4)(joblib.delayed(f)(i) for i in range(2))

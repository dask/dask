from __future__ import print_function, division, absolute_import
import os
import importlib
from distutils.version import LooseVersion

import pytest
from random import random
from time import sleep

from distributed import Client
from distributed.metrics import time
from distributed.utils_test import cluster, inc
from distributed.utils_test import loop # noqa F401
from toolz import identity

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


def test_joblib_funcname(loop, joblib):
    Parallel = joblib.Parallel
    delayed = joblib.delayed
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask') as (ba, _):
                x, y = Parallel()(delayed(inc)(i) for i in range(2))

            def f(dask_scheduler):
                return list(dask_scheduler.transition_log)
            log = client.run_on_scheduler(f)
            assert all(tup[0].startswith('inc-batch') for tup in log)


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


def test_correct_nested_backend(loop, joblib):
    if LooseVersion(joblib.__version__) <= LooseVersion("0.11.0"):
        pytest.skip("Requires nested parallelism")

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            # No requirement, should be us
            with joblib.parallel_backend('dask') as (ba, _):
                result = joblib.Parallel(n_jobs=2)(joblib.delayed(outer)(
                    joblib, nested_require=None) for _ in range(1))
                assert isinstance(result[0][0][0],
                                  distributed_joblib.DaskDistributedBackend)

            # Require threads, should be threading
            with joblib.parallel_backend('dask') as (ba, _):
                result = joblib.Parallel(n_jobs=2)(joblib.delayed(outer)(
                    joblib, nested_require='sharedmem') for _ in range(1))
                assert isinstance(result[0][0][0],
                                  joblib.parallel.ThreadingBackend)


def outer(joblib, nested_require):
    return joblib.Parallel(n_jobs=2, prefer='threads')(
        joblib.delayed(middle)(joblib, nested_require) for _ in range(1)
    )


def middle(joblib, require):
    return joblib.Parallel(n_jobs=2, require=require)(
        joblib.delayed(inner)(joblib) for _ in range(1)
    )


def inner(joblib):
    return joblib.parallel.Parallel()._backend


def test_secede_with_no_processes(loop, joblib):
    # https://github.com/dask/distributed/issues/1775

    with Client(loop=loop, processes=False, set_as_default=True):
        with joblib.parallel_backend('dask'):
            joblib.Parallel(n_jobs=4)(joblib.delayed(identity)(i) for i in range(2))


def _test_keywords_f(_):
    from distributed import get_worker
    return get_worker().address


def test_keywords(loop, joblib):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask', workers=a['address']) as (ba, _):
                seq = joblib.Parallel()(joblib.delayed(_test_keywords_f)(i) for i in range(10))
                assert seq == [a['address']] * 10

            with joblib.parallel_backend('dask', workers=b['address']) as (ba, _):
                seq = joblib.Parallel()(joblib.delayed(_test_keywords_f)(i) for i in range(10))
                assert seq == [b['address']] * 10


def test_cleanup(loop, joblib):
    with Client(processes=False, loop=loop) as client:
        with joblib.parallel_backend('dask'):
            joblib.Parallel()(joblib.delayed(inc)(i) for i in range(10))

        start = time()
        while client.cluster.scheduler.tasks:
            sleep(0.01)
            assert time() < start + 5

        assert not client.futures


def test_auto_scatter(loop, joblib):
    base_type = joblib._parallel_backends.ParallelBackendBase
    if not hasattr(base_type, 'start_call'):
        raise pytest.skip('joblib version does not support backend callbacks')

    np = pytest.importorskip('numpy')
    data = np.ones(int(1e7), dtype=np.uint8)

    Parallel = joblib.Parallel
    delayed = joblib.delayed

    def noop(*args, **kwargs):
        pass

    def count_events(event_name, client):
        worker_events = client.run(lambda dask_worker: dask_worker.log)
        event_counts = {}
        for w, events in worker_events.items():
            event_counts[w] = len([event for event in list(events)
                                   if event[1] == event_name])
        return event_counts

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask') as (ba, _):
                # Passing the same data as arg and kwarg triggers a single
                # scatter operation whose result is reused.
                Parallel()(delayed(noop)(data, data, i, opt=data)
                           for i in range(5))
            # By default large array are automatically scattered with
            # broadcast=3 which means that each worker can directly receive
            # the data from the scatter operation once.
            counts = count_events('receive-from-scatter', client)
            assert counts[a['address']] == 1
            assert counts[b['address']] == 1

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            with joblib.parallel_backend('dask') as (ba, _):
                Parallel()(delayed(noop)(data[:3], i) for i in range(5))
            # Small arrays are passed within the task definition without going
            # through a scatter operation.
            counts = count_events('receive-from-scatter', client)
            assert counts[a['address']] == 0
            assert counts[b['address']] == 0

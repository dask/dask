"""Tests on traceback shortening heuristics

See Also
--------
distributed/tests/test_client.py::test_short_tracebacks
distributed/tests/test_client.py::test_short_tracebacks_async
"""
from __future__ import annotations

import traceback
from contextlib import contextmanager

import pytest

import dask
from dask.utils import shorten_traceback


@contextmanager
def assert_tb_levels(expect):
    with pytest.raises(ZeroDivisionError) as e:
        yield
    frames = list(traceback.walk_tb(e.tb))
    frame_names = [frame[0].f_code.co_name for frame in frames]
    assert frame_names[0] == "assert_tb_levels", frame_names
    assert frame_names[1:] == expect, frame_names


def f1():
    return 1 / 0


def f2():
    return f1() + 5


def f3():
    return f2() + 1


@pytest.mark.parametrize(
    "regexes,expect",
    [
        (None, ["test_shorten_traceback", "f3", "f2", "f1"]),  # Disabled
        ([], ["test_shorten_traceback", "f3", "f2", "f1"]),  # Disabled
        (["nomatch"], ["test_shorten_traceback", "f3", "f2", "f1"]),
        # Remove absolutely everything, but keep the first and last frame
        # Python <= 3.11 can't remove the first frame of a traceback from a __exit__
        # function
        ([".*"], ["test_shorten_traceback", "f1"]),
        (["tests"], ["test_shorten_traceback", "f1"]),
    ],
)
def test_shorten_traceback(regexes, expect):
    """
    See also
    --------
    test_distributed.py::test_shorten_traceback_excepthook
    test_distributed.py::test_shorten_traceback_ipython
    """
    with dask.config.set({"admin.traceback.shorten": regexes}):
        with assert_tb_levels(expect):
            with shorten_traceback():
                f3()


try:
    import tblib
except ImportError:
    tblib = None


@pytest.mark.parametrize("scheduler", ["threads", "processes", "sync"])
def test_compute_shorten_traceback(scheduler):
    d = dask.delayed(f3)()

    TEST_NAME = "test_compute_shorten_traceback"
    if scheduler == "processes" and not tblib:
        remote_stack = ["reraise"]
    else:
        remote_stack = ["f3", "f2", "f1"]

    expect = [TEST_NAME, "compute", *remote_stack]
    with assert_tb_levels(expect):
        dask.compute(d(), scheduler=scheduler)

    expect = [TEST_NAME, "compute", "compute", *remote_stack]
    with assert_tb_levels(expect):
        d.compute(scheduler=scheduler)


@pytest.mark.parametrize("scheduler", ["threads", "processes", "sync"])
def test_persist_shorten_traceback(scheduler):
    d = dask.delayed(f3)()

    TEST_NAME = "test_persist_shorten_traceback"
    if scheduler == "processes" and not tblib:
        remote_stack = ["reraise"]
    else:
        remote_stack = ["f3", "f2", "f1"]

    expect = [TEST_NAME, "persist", *remote_stack]
    with assert_tb_levels(expect):
        dask.persist(d(), scheduler=scheduler)

    expect = [TEST_NAME, "persist", "persist", *remote_stack]
    with assert_tb_levels(expect):
        d.persist(scheduler=scheduler)


def test_distributed_shorten_traceback():
    distributed = pytest.importorskip("distributed")
    with distributed.Client(processes=False, dashboard_address=":0"):
        d = dask.delayed(f3)()

        (dp1,) = dask.persist(d)
        dp2 = d.persist()

        TEST_NAME = "test_distributed_shorten_traceback"
        expect = [TEST_NAME, "compute", "f3", "f2", "f1"]
        with assert_tb_levels(expect):
            dask.compute(d())

        expect = [TEST_NAME, "compute", "compute", "f3", "f2", "f1"]
        with assert_tb_levels(expect):
            d.compute()

        expect = [TEST_NAME, "compute", "f3", "f2", "f1"]
        with assert_tb_levels(expect):
            dask.compute(dp1)

        expect = [TEST_NAME, "compute", "compute", "f3", "f2", "f1"]
        with assert_tb_levels(expect):
            dp2.compute()

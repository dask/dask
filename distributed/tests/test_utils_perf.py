from __future__ import print_function, division, absolute_import

import contextlib
import gc
import itertools
import random
import re

import pytest

from distributed.compatibility import PY2
from distributed.metrics import thread_time
from distributed.utils_perf import FractionalTimer, GCDiagnosis, disable_gc_diagnosis
from distributed.utils_test import captured_logger, run_for


class RandomTimer(object):
    """
    A mock timer producing random (but monotonic) values.
    """

    def __init__(self):
        self.last = 0.0
        self.timings = []
        self.durations = ([], [])
        self.i_durations = itertools.cycle((0, 1))
        self.random = random.Random(42)

    def __call__(self):
        dt = self.random.expovariate(1.0)
        self.last += dt
        self.timings.append(self.last)
        self.durations[next(self.i_durations)].append(dt)
        return self.last


def test_fractional_timer():
    N = 10

    def check_fraction(timer, ft):
        # The running fraction should be approximately equal to the
        # sum of last N "measurement" intervals over the sum of last
        # 2N intervals (not 2N - 1 or 2N + 1)
        actual = ft.running_fraction
        expected = sum(timer.durations[1][-N:]) / (
            sum(timer.durations[0][-N:] + timer.durations[1][-N:])
        )
        assert actual == pytest.approx(expected)

    timer = RandomTimer()
    ft = FractionalTimer(n_samples=N, timer=timer)
    for i in range(N):
        ft.start_timing()
        ft.stop_timing()
    assert len(timer.timings) == N * 2
    assert ft.running_fraction is None

    ft.start_timing()
    ft.stop_timing()
    assert len(timer.timings) == (N + 1) * 2
    assert ft.running_fraction is not None
    check_fraction(timer, ft)

    for i in range(N * 10):
        ft.start_timing()
        ft.stop_timing()
        check_fraction(timer, ft)


@contextlib.contextmanager
def enable_gc_diagnosis_and_log(diag, level="INFO"):
    disable_gc_diagnosis(force=True)  # just in case
    if gc.callbacks:
        print("Unexpected gc.callbacks", gc.callbacks)

    with captured_logger("distributed.utils_perf", level=level, propagate=False) as sio:
        gc.disable()
        gc.collect()  # drain any leftover from previous tests
        diag.enable()
        try:
            yield sio
        finally:
            diag.disable()
            gc.enable()


@pytest.mark.skipif(PY2, reason="requires Python 3")
def test_gc_diagnosis_cpu_time():
    diag = GCDiagnosis(warn_over_frac=0.75)
    diag.N_SAMPLES = 3  # shorten tests

    with enable_gc_diagnosis_and_log(diag, level="WARN") as sio:
        # Spend some CPU time doing only full GCs
        for i in range(diag.N_SAMPLES):
            gc.collect()
        assert not sio.getvalue()
        gc.collect()
        lines = sio.getvalue().splitlines()
        assert len(lines) == 1
        # Between 80% and 100%
        assert re.match(
            r"full garbage collections took (100|[89][0-9])% " r"CPU time recently",
            lines[0],
        )

    with enable_gc_diagnosis_and_log(diag, level="WARN") as sio:
        # Spend half the CPU time doing full GCs
        for i in range(diag.N_SAMPLES + 1):
            t1 = thread_time()
            gc.collect()
            dt = thread_time() - t1
            run_for(dt, timer=thread_time)
        # Less than 75% so nothing printed
        assert not sio.getvalue()


@pytest.mark.xfail(reason="unknown")
@pytest.mark.skipif(PY2, reason="requires Python 3")
def test_gc_diagnosis_rss_win():
    diag = GCDiagnosis(info_over_rss_win=10e6)

    def make_refcycle(nbytes):
        l = [b"x" * nbytes]
        l.append(l)
        return

    with enable_gc_diagnosis_and_log(diag) as sio:
        make_refcycle(100 * 1024)
        gc.collect()
        # Too small, nothing printed
        assert not sio.getvalue()

        # NOTE: need to allocate a very large value to make sure RSS
        # really shrinks (depending on the system memory allocator,
        # "small" memory deallocations may keep the memory in the pool)
        make_refcycle(200 * 1024 * 1024)
        gc.collect()
        lines = sio.getvalue().splitlines()
        assert len(lines) == 1
        # Several MB released, and at least 1 reference cycles
        assert re.match(
            r"full garbage collection released [\d\.]+ MB "
            r"from [1-9]\d* reference cycles",
            lines[0],
        )

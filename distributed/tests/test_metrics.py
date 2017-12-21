from __future__ import print_function, division, absolute_import

import sys
import threading
import time

from distributed import metrics
from distributed.compatibility import PY3
from distributed.utils_test import run_for


def test_wall_clock():
    for i in range(3):
        time.sleep(0.01)
        t = time.time()
        samples = [metrics.time() for j in range(50)]
        # Resolution
        deltas = [samples[j + 1] - samples[j] for j in range(len(samples) - 1)]
        assert min(deltas) >= 0.0, deltas
        assert max(deltas) <= 1.0, deltas
        assert any(lambda d: 0.0 < d < 0.0001 for d in deltas), deltas
        # Close to time.time()
        assert t - 0.5 < samples[0] < t + 0.5


def test_process_time():
    start = metrics.process_time()
    run_for(0.05)
    dt = metrics.process_time() - start
    assert 0.03 <= dt <= 0.2

    # All threads counted
    t = threading.Thread(target=run_for, args=(0.1,))
    start = metrics.process_time()
    t.start()
    t.join()
    dt = metrics.process_time() - start
    assert dt >= 0.08

    if PY3:
        # Sleep time not counted
        start = metrics.process_time()
        time.sleep(0.1)
        dt = metrics.process_time() - start
        assert dt <= 0.05


def test_thread_time():
    start = metrics.thread_time()
    run_for(0.05)
    dt = metrics.thread_time() - start
    assert 0.03 <= dt <= 0.2

    if PY3:
        # Sleep time not counted
        start = metrics.thread_time()
        time.sleep(0.1)
        dt = metrics.thread_time() - start
        assert dt <= 0.05

        if sys.platform == 'linux':
            # Always per-thread on Linux
            t = threading.Thread(target=run_for, args=(0.1,))
            start = metrics.thread_time()
            t.start()
            t.join()
            dt = metrics.thread_time() - start
            assert dt <= 0.05

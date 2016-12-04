from __future__ import print_function, division, absolute_import

import time

import pytest

from distributed import metrics


def test_wall_clock():
    for i in range(3):
        time.sleep(0.01)
        t = time.time()
        samples = [metrics.time() for j in range(50)]
        # Resolution
        deltas = [samples[j+1] - samples[j] for j in range(len(samples) - 1)]
        assert min(deltas) >= 0.0, deltas
        assert max(deltas) <= 1.0, deltas
        assert any(lambda d: 0.0 < d < 0.0001 for d in deltas), deltas
        # Close to time.time()
        assert t - 0.5 < samples[0] < t + 0.5

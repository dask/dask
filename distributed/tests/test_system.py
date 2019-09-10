import os

import psutil
import pytest

from distributed.system import cpu_count, memory_limit


def test_cpu_count():
    count = cpu_count()
    assert isinstance(count, int)
    assert count <= os.cpu_count()
    assert count >= 1


def test_memory_limit():
    limit = memory_limit()
    assert isinstance(limit, int)
    assert limit <= psutil.virtual_memory().total
    assert limit >= 1


def test_rlimit():
    resource = pytest.importorskip("resource")

    # decrease memory limit by one byte
    new_limit = memory_limit() - 1
    try:
        resource.setrlimit(resource.RLIMIT_RSS, (new_limit, new_limit))
        assert memory_limit() == new_limit
    except OSError:
        pytest.skip("resource could not set the RSS limit")

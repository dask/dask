import builtins
import io
import sys

import psutil
import pytest

from distributed.system import memory_limit


def test_memory_limit():
    limit = memory_limit()
    assert isinstance(limit, int)
    assert limit <= psutil.virtual_memory().total
    assert limit >= 1


def test_memory_limit_cgroups(monkeypatch):
    builtin_open = builtins.open

    def myopen(path, *args, **kwargs):
        if path == "/sys/fs/cgroup/memory/memory.limit_in_bytes":
            # Absurdly low, unlikely to match real value
            return io.StringIO("20")
        return builtin_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", myopen)
    monkeypatch.setattr(sys, "platform", "linux")

    limit = memory_limit()
    assert limit == 20


def test_rlimit():
    resource = pytest.importorskip("resource")

    # decrease memory limit by one byte
    new_limit = memory_limit() - 1
    try:
        resource.setrlimit(resource.RLIMIT_RSS, (new_limit, new_limit))
        assert memory_limit() == new_limit
    except OSError:
        pytest.skip("resource could not set the RSS limit")

import builtins
import io
import os
import sys

import psutil
import pytest

from distributed.system import cpu_count, memory_limit


def test_cpu_count():
    count = cpu_count()
    assert isinstance(count, int)
    assert count <= os.cpu_count()
    assert count >= 1


@pytest.mark.parametrize("dirname", ["cpuacct,cpu", "cpu,cpuacct", None])
def test_cpu_count_cgroups(dirname, monkeypatch):
    def mycpu_count():
        # Absurdly high, unlikely to match real value
        return 250

    monkeypatch.setattr(os, "cpu_count", mycpu_count)

    class MyProcess(object):
        def cpu_affinity(self):
            # No affinity set
            return []

    monkeypatch.setattr(psutil, "Process", MyProcess)

    if dirname:
        paths = {
            "/sys/fs/cgroup/%s/cpu.cfs_quota_us" % dirname: io.StringIO("2005"),
            "/sys/fs/cgroup/%s/cpu.cfs_period_us" % dirname: io.StringIO("10"),
        }
        builtin_open = builtins.open

        def myopen(path, *args, **kwargs):
            if path in paths:
                return paths.get(path)
            return builtin_open(path, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", myopen)
        monkeypatch.setattr(sys, "platform", "linux")

    count = cpu_count()
    if dirname:
        # Rounds up
        assert count == 201
    else:
        assert count == 250


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

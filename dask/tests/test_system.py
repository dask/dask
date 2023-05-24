from __future__ import annotations

import builtins
import io
import os
import sys

import pytest

from dask.system import cpu_count

psutil = pytest.importorskip("psutil")


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

    class MyProcess:
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

import math
import os
import sys

import psutil

__all__ = ("memory_limit", "cpu_count", "MEMORY_LIMIT", "CPU_COUNT")


def memory_limit():
    """Get the memory limit (in bytes) for this system.

    Takes the minimum value from the following locations:

    - Total system host memory
    - Cgroups limit (if set)
    - RSS rlimit (if set)
    """
    limit = psutil.virtual_memory().total

    # Check cgroups if available
    if sys.platform == "linux":
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
                cgroups_limit = int(f.read())
            if cgroups_limit > 0:
                limit = min(limit, cgroups_limit)
        except Exception:
            pass

    # Check rlimit if available
    try:
        import resource

        hard_limit = resource.getrlimit(resource.RLIMIT_RSS)[1]
        if hard_limit > 0:
            limit = min(limit, hard_limit)
    except (ImportError, OSError):
        pass

    return limit


def cpu_count():
    """Get the available CPU count for this system.

    Takes the minimum value from the following locations:

    - Total system cpus available on the host.
    - CPU Affinity (if set)
    - Cgroups limit (if set)
    """
    count = os.cpu_count()

    # Check CPU affinity if available
    try:
        affinity_count = len(psutil.Process().cpu_affinity())
        if affinity_count > 0:
            count = min(count, affinity_count)
    except Exception:
        pass

    # Check cgroups if available
    if sys.platform == "linux":
        # The directory name isn't standardized across linux distros, check both
        for dirname in ["cpuacct,cpu", "cpu,cpuacct"]:
            try:
                with open("/sys/fs/cgroup/%s/cpu.cfs_quota_us" % dirname) as f:
                    quota = int(f.read())
                with open("/sys/fs/cgroup/%s/cpu.cfs_period_us" % dirname) as f:
                    period = int(f.read())
                # We round up on fractional CPUs
                cgroups_count = math.ceil(quota / period)
                if cgroups_count > 0:
                    count = min(count, cgroups_count)
                break
            except Exception:
                pass

    return count


MEMORY_LIMIT = memory_limit()
CPU_COUNT = cpu_count()

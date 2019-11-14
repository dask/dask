import sys

import psutil

__all__ = ("memory_limit", "MEMORY_LIMIT")


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


MEMORY_LIMIT = memory_limit()

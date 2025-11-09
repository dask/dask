from __future__ import annotations

import logging
import sys

import psutil

__all__ = ("memory_limit", "MEMORY_LIMIT")


logger = logging.getLogger(__name__)


def memory_limit() -> int:
    """Get the memory limit (in bytes) for this system.

    Takes the minimum value from the following locations:

    - Total system host memory
    - Cgroups limit (if set)
    - RSS rlimit (if set)
    """
    limit = psutil.virtual_memory().total

    # Check cgroups if available
    # Note: can't use LINUX and WINDOWS constants as they upset mypy
    if sys.platform == "linux":
        path_used = None
        for path in [
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",  # cgroups v1 hard limit
            "/sys/fs/cgroup/memory/memory.soft_limit_in_bytes",  # cgroups v1 soft limit
            "/sys/fs/cgroup/memory.max",  # cgroups v2 hard limit
            "/sys/fs/cgroup/memory.high",  # cgroups v2 soft limit
            "/sys/fs/cgroup/memory.low",  # cgroups v2 softest limit
        ]:
            try:
                with open(path) as f:
                    cgroups_limit = int(f.read())
                if cgroups_limit > 0:
                    path_used = path
                    limit = min(limit, cgroups_limit)
            except Exception:
                pass
        if path_used:
            logger.debug(
                "Setting system memory limit based on cgroup value defined in %s",
                path_used,
            )

    # Check rlimit if available
    if sys.platform != "win32":
        try:
            import resource

            hard_limit = resource.getrlimit(resource.RLIMIT_RSS)[1]
            if 0 < hard_limit < limit:
                logger.debug(
                    "Limiting system memory based on RLIMIT_RSS to %s", hard_limit
                )
                limit = hard_limit
        except (ImportError, OSError):
            pass

    return limit


MEMORY_LIMIT = memory_limit()

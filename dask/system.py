import math
import os
import sys

try:
    import psutil
except ImportError:
    psutil = None

__all__ = ("cpu_count", "CPU_COUNT")


def cpu_count():
    """Get the available CPU count for this system.

    Takes the minimum value from the following locations:

    - Total system cpus available on the host.
    - CPU Affinity (if set)
    - Cgroups limit (if set)
    """
    count = os.cpu_count()

    # Check CPU affinity if available
    if psutil is not None:
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


CPU_COUNT = cpu_count()

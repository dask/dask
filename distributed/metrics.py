from __future__ import print_function, division, absolute_import

import collections
from functools import wraps
import sys
import time as timemod


_empty_namedtuple = collections.namedtuple("_empty_namedtuple", ())


def _psutil_caller(method_name, default=_empty_namedtuple):
    try:
        import psutil
    except ImportError:
        return default

    meth = getattr(psutil, method_name)
    @wraps(meth)
    def wrapper():
        try:
            return meth()
        except RuntimeError:
            # This can happen on some systems (e.g. no physical disk in worker)
            return default()

    return wrapper


disk_io_counters = _psutil_caller("disk_io_counters")

net_io_counters = _psutil_caller("net_io_counters")


class _WindowsTime(object):
    """
    Combine time.time() and time.perf_counter() to get an absolute clock
    with fine resolution.
    """

    # Resync every N seconds, to avoid drifting
    RESYNC_EVERY = 600

    def __init__(self):
        self.delta = None
        self.last_resync = float("-inf")

    if sys.version_info >= (3,):
        perf_counter = timemod.perf_counter
    else:
        perf_counter = timemod.clock

    def time(self):
        delta = self.delta
        cur = self.perf_counter()
        if cur - self.last_resync >= self.RESYNC_EVERY:
            delta = self.resync()
            self.last_resync = cur
        return delta + cur

    def resync(self):
        _time = timemod.time
        _perf_counter = self.perf_counter
        min_samples = 5
        while True:
            times = [(_time(), _perf_counter()) for i in range(min_samples * 2)]
            abs_times = collections.Counter(t[0] for t in times)
            first, nfirst = abs_times.most_common()[0]
            if nfirst < min_samples:
                # System too noisy? Start again
                continue
            else:
                perf_times = [t[1] for t in times if t[0] == first][:-1]
                assert len(perf_times) >= min_samples - 1, perf_times
                self.delta = first - sum(perf_times) / len(perf_times)
                return self.delta


# A high-resolution wall clock timer measuring the seconds since Unix epoch
if sys.platform.startswith('win'):
    time = _WindowsTime().time
else:
    # Under modern Unices, time.time() should be good enough
    time = timemod.time

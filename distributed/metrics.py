from __future__ import print_function, division, absolute_import

import collections
from functools import wraps


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


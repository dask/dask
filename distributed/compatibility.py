from __future__ import print_function, division, absolute_import

import sys

if sys.version_info[0] == 2:
    from Queue import Queue, Empty
    reload = reload
    unicode = unicode

    def isqueue(o):
        return (hasattr(o, 'queue') and
                hasattr(o, '__module__') and
                o.__module__ == 'Queue')


if sys.version_info[0] == 3:
    from queue import Queue, Empty
    from importlib import reload
    unicode = str

    def isqueue(o):
        return isinstance(o, Queue)


try:
    from functools import singledispatch
except ImportError:
    from singledispatch import singledispatch

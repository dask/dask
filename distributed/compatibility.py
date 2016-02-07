from __future__ import print_function, division, absolute_import

import sys

if sys.version_info[0] == 2:
    from Queue import Queue, Empty
    reload = reload

if sys.version_info[0] == 3:
    from queue import Queue, Empty
    from importlib import reload


try:
    from functools import singledispatch
except ImportError:
    from singledispatch import singledispatch

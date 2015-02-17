from __future__ import absolute_import, division, print_function

import sys

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

if PY3:
    import builtins
    from queue import Queue
    unicode = str
    long = int
else:
    import __builtin__ as builtins
    from Queue import Queue
    import operator
    unicode = unicode
    long = long

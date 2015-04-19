from __future__ import absolute_import, division, print_function

import sys

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

if PY3:
    import builtins
    from queue import Queue
    from itertools import zip_longest
    unicode = str
    long = int
    def apply(func, args):
        return func(*args)
else:
    import __builtin__ as builtins
    from Queue import Queue
    import operator
    from itertools import izip_longest as zip_longest
    unicode = unicode
    long = long
    apply = apply


def skip(func):
    return

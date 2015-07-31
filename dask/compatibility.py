from __future__ import absolute_import, division, print_function

import sys

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

if PY3:
    import builtins
    from queue import Queue, Empty
    from itertools import zip_longest
    from io import StringIO, BytesIO
    from urllib.request import urlopen
    from urllib.parse import urlparse
    from urllib.parse import quote, unquote
    unicode = str
    long = int
    def apply(func, args, kwargs=None):
        if kwargs:
            return func(*args, **kwargs)
        else:
            return func(*args)
    range = range
else:
    import __builtin__ as builtins
    from Queue import Queue, Empty
    import operator
    from itertools import izip_longest as zip_longest
    from StringIO import StringIO
    from io import BytesIO
    from urllib2 import urlopen
    from urlparse import urlparse
    from urllib import quote, unquote
    unicode = unicode
    long = long
    apply = apply
    range = xrange


def skip(func):
    return

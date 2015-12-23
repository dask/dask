from __future__ import absolute_import, division, print_function

import functools
import inspect
import operator
import sys
import types

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

if PY3:
    import builtins
    from queue import Queue, Empty
    from itertools import zip_longest
    from io import StringIO, BytesIO
    from bz2 import BZ2File
    from gzip import GzipFile
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
    operator_div = operator.truediv

    def _getargspec(func):
        return inspect.getfullargspec(func)

else:
    import __builtin__ as builtins
    from Queue import Queue, Empty
    from itertools import izip_longest as zip_longest
    from StringIO import StringIO
    from io import BytesIO, BufferedIOBase
    import bz2
    import gzip
    from urllib2 import urlopen
    from urlparse import urlparse
    from urllib import quote, unquote
    unicode = unicode
    long = long
    apply = apply
    range = xrange
    operator_div = operator.div

    def _getargspec(func):
        return inspect.getargspec(func)

    if sys.version_info[1] <= 6:
        class BZ2File(bz2.BZ2File, BufferedIOBase):
            def __init__(self, *args, **kwargs):
                super(BZ2File, self).__init__(*args, **kwargs)

            def readable(self):
                return 'r' in self.mode

            def writable(self):
                return 'w' in self.mode

            def seekable(self):
                return self.readable()

            def read1(self, size=-1):
                return self.read(size)

        class GzipFile(gzip.GzipFile, BufferedIOBase):
            def __init__(self, *args, **kwargs):
                super(GzipFile, self).__init__(*args, **kwargs)

            def readable(self):
                return self.mode == gzip.READ

            def writable(self):
                return self.mode == gzip.WRITE

            def seekable(self):
                # TODO: Can we really hard-code True here?
                return True

            def read1(self, size=-1):
                return self.read(size)
    else:
        BZ2File = bz2.BZ2File
        GzipFile = gzip.GzipFile


def getargspec(func):
    """Version of inspect.getargspec that works for functools.partial objects"""
    if isinstance(func, functools.partial):
        return _getargspec(func.func)
    else:
        if isinstance(func, type):
            return _getargspec(func.__init__)
        else:
            return _getargspec(func)

def skip(func):
    return


def bind_method(cls, name, func):
    """Bind a method to class

    Parameters
    ----------

    cls : type
        class to receive bound method
    name : basestring
        name of method on class instance
    func : function
        function to be bound as method

    Returns
    -------
    None
    """
    # only python 2 has bound/unbound method issue
    if not PY3:
        setattr(cls, name, types.MethodType(func, None, cls))
    else:
        setattr(cls, name, func)

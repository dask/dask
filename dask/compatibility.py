from __future__ import absolute_import, division, print_function

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


_basic_operators = [operator.abs, operator.add, operator.and_,
                    operator.eq, operator.gt, operator.ge, operator.inv,
                    operator.lt, operator.le, operator.mod,
                    operator.mul, operator.ne, operator.neg,
                    operator.or_, operator.pow, operator.sub,
                    operator.truediv, operator.floordiv, operator.xor]

if PY2:
    _basic_operators += [operator.div]
"""
Control global computation context
"""
from __future__ import absolute_import, division, print_function

import threading
from functools import partial
import warnings
from . import config

_globals = config.config


thread_state = threading.local()


_warned_set_options = [False]


def set_options(*args, **kwargs):
    """ Deprecated: see dask.config.set instead """
    if not _warned_set_options[0]:
        warnings.warn("The dask.set_options function has been deprecated. "
                      "Please use dask.config.set instead")
        _warned_set_options[0] = True
    return config.set(*args, **kwargs)


def globalmethod(default=None, key=None, falsey=None):
    """ Allow function to be taken over by globals

    This modifies a method so that occurrences of it may be taken over by
    functions registered in the global options. Can be used as a decorator or a
    function.

    Parameters
    ----------
    default : callable
        The default callable to use.
    key : str
        Key under which we register this function in the global parameters
    falsey : callable, None, optional
        A function to use if the option is falsey. If not provided, the default
        is used instead.

    Examples
    --------
    >>> import dask
    >>> class Foo(object):
    ...     @globalmethod(key='bar', falsey=lambda: 3)
    ...     def bar():
    ...         return 1
    >>> f = Foo()
    >>> f.bar()
    1
    >>> with dask.set_options(bar=lambda: 2):
    ...     print(f.bar())
    2
    >>> with dask.set_options(bar=False):
    ...     print(f.bar())
    3
    """
    if default is None:
        return partial(globalmethod, key=key, falsey=falsey)
    return GlobalMethod(default=default, key=key, falsey=falsey)


class GlobalMethod(object):
    def __init__(self, default, key, falsey=None):
        self._default = default
        self._key = key
        self._falsey = falsey

    def __get__(self, instance, owner=None):
        if self._key in _globals:
            if _globals[self._key]:
                return _globals[self._key]
            elif self._falsey is not None:
                return self._falsey
        return self._default

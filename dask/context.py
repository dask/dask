"""
Control global computation context
"""
from __future__ import absolute_import, division, print_function

from collections import defaultdict
import functools

_globals = defaultdict(lambda: None)
_globals['callbacks'] = set()


class set_options(object):
    """ Set global state within controlled context

    This lets you specify various global settings in a tightly controlled
    ``with`` block.

    Valid keyword arguments currently include the following::

        get - the scheduler to use
        pool - a thread or process pool
        cache - Cache to use for intermediate results
        func_loads/func_dumps - loads/dumps functions for serialization of data
            likely to contain functions.  Defaults to
            cloudpickle.loads/cloudpickle.dumps
        optimizations - List of additional optimizations to run

    Examples
    --------
    >>> with set_options(get=dask.get):  # doctest: +SKIP
    ...     x = np.array(x)  # uses dask.get internally
    """
    def __init__(self, **kwargs):
        self.old = _globals.copy()
        _globals.update(kwargs)

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        _globals.clear()
        _globals.update(self.old)


def defer_to_globals(name, falsey=None):
    """ Allow function to be taken over by globals

    This modifies a function so that occurrences of it may be taken over by
    functions registered in the global options.  It is commonly used as a
    decorator.

    Parameters
    ----------
    name: str
        name under which we register this function in the global parameters
    falsey: callable, None, optional
        A function to use if the option is falsey

    Examples
    --------

    >>> import dask
    >>> @defer_to_globals('foo')
    ... def f():
    ...     return 1

    >>> f()
    1

    >>> with dask.set_options(foo=lambda: 2):
    ...     print(f())
    2
    """
    def _(func):
        @functools.wraps(func)
        def f(*args, **kwargs):
            if name in _globals:
                if _globals[name]:
                    return _globals[name](*args, **kwargs)
                elif falsey:
                    return falsey(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        return f
    return _

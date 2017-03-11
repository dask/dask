"""
Control global computation context
"""
from __future__ import absolute_import, division, print_function

from collections import defaultdict

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

"""
Control global computation context
"""

from contextlib import contextmanager
from collections import defaultdict

_globals = defaultdict(lambda: None)


class set_options(object):
    """ Set global state within controled context

    This lets you specify various global settings in a tightly controlled with
    block

    Valid keyword arguments currently include:

        get - the scheduler to use

    Example
    -------

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

"""
Control global computation context
"""

from contextlib import contextmanager
from collections import defaultdict

_globals = defaultdict(lambda: None)


@contextmanager
def context(**kwargs):
    """ Set global state within controled context

    This lets you specify various global settings in a tightly controlled with
    block

    Valid keyword arguments currently include:

        get - the scheduler to use

    Example
    -------

    >>> with context(get=dask.get):
    ...     x = np.array(x)  # uses dask.get internally
    """
    old = _globals.copy()

    _globals.update(kwargs)

    try:
        yield
    finally:
        _globals.clear()
        _globals.update(old)

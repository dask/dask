from contextlib import contextmanager
from collections import namedtuple

from .context import _globals

Callback = namedtuple('Callback', ['start', 'pretask', 'posttask', 'finish'])


def unpack_callbacks(cbs):
    """Take a callback tuple or iterable of callback tuples, and return a list
    of each callback."""
    if cbs:
        if isinstance(cbs, tuple):
            return [list(i) for i in cbs if i]
        else:
            return [[i for i in f if i] for f in zip(*cbs)]
    else:
        return [(), (), (), ()]


@contextmanager
def add_callbacks(*args):
    """Context manager for callbacks.

    Takes several callback tuples and applies them only in the enclosed
    context.

    Examples
    --------
    >>> def pretask(key, dsk, state):
    ...     print("Now running {0}").format(key)
    >>> callbacks = (None, pretask, None, None)
    >>> with add_callbacks(callbacks):    # doctest: +SKIP
    ...     res.compute()
    """
    old = _globals['callbacks'].copy()
    _globals['callbacks'].update(args)
    yield
    _globals['callbacks'] = old

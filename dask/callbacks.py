from contextlib import contextmanager
from collections import namedtuple

from .context import _globals

callbacks = namedtuple('callbacks', ['start', 'pretask', 'posttask', 'finish'])
default_callbacks = callbacks(None, None, None, None)


def combine(funcs):
    """Build a function that applies all functions in `funcs` iteratively on
    the same input."""
    funcs = list(funcs)
    if funcs:
        def _inner(*args, **kwargs):
            for f in funcs:
                f(*args, **kwargs)
        return _inner


def setup_callbacks(cbs):
    """Take a callback or iterable of callbacks, and return a single callback
    object."""
    if cbs:
        if isinstance(cbs, callbacks):
            return cbs
        elif isinstance(cbs, tuple):
            return callbacks(*cbs)
        else:
            return callbacks(*(combine(filter(None, f)) for f in zip(*cbs)))
    else:
        return default_callbacks


@contextmanager
def add_callbacks(*args):
    old = _globals['callbacks'].copy()
    _globals['callbacks'].update(args)
    yield
    _globals['callbacks'] = old

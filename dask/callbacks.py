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
    old = _globals['callbacks'].copy()
    _globals['callbacks'].update(args)
    yield
    _globals['callbacks'] = old

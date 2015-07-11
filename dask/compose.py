from uuid import uuid4
import operator
from functools import partial, wraps

from toolz import merge

from .optimize import cull
from .context import _globals
from . import threaded


def get_name_dasks(v):
    if isinstance(v, Value):
        return v._name, v._dasks
    else:
        return v, []


def applyfunc(func, *args, **kwargs):
    if kwargs:
        func = partial(func, **kwargs)
    names, dsks = zip(*map(get_name_dasks, args))
    name = uuid4().hex
    dsks = sum(dsks, [{name: (func,) + names}])
    return Value(name, dsks)


def daskify(func):
    """Wraps func so that it outputs a ``Value``"""
    @wraps(func)
    def _dfunc(*args, **kwargs):
        return applyfunc(func, *args, **kwargs)
    return _dfunc


def get(dsk, keys, get=None, **kwargs):
    """Specialized get function"""
    get = get or _globals['get'] or threaded.get
    dsk2 = cull(dsk, keys)
    return get(dsk2, keys, **kwargs)


class Value(object):
    """Represents a value to be computed by dask.

    Equivalent to the output from a single key in a dask graph.
    """
    def __init__(self, name, dasks):
        self._name = name
        self._dasks = dasks

    __add__ = daskify(operator.add)
    __sub__ = daskify(operator.sub)
    __mul__ = daskify(operator.mul)
    __div__ = daskify(operator.div)
    __getitem__ = daskify(operator.getitem)

    def compute(self, **kwargs):
        dask1 = merge(*self._dasks)
        dask2 = cull(dask1, self._name)
        return get(dask2, self._name, **kwargs)

    @property
    def dask(self):
        return merge(*self._dasks)

    def visualize(self):
        from dask.dot import dot_graph
        dot_graph(self.dask)


def value(val, name=None):
    """Create a value from a python object"""
    name = name or uuid4().hex
    return Value(name, [{name: val}])

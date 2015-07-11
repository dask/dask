from uuid import uuid4
import operator
from functools import partial, wraps

from toolz import merge

from .optimize import cull
from .context import _globals
from . import threaded


def get_uuid_dasks(v):
    if isinstance(v, Value):
        return v._uuid, v._dasks
    else:
        return v, []


def applyfunc(func, *args, **kwargs):
    if kwargs:
        func = partial(func, **kwargs)
    uuids, dsks = zip(*map(get_uuid_dasks, args))
    uuid = uuid4().hex
    dsks = sum(dsks, [{uuid: (func,) + uuids}])
    return Value(uuid, dsks)


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
    def __init__(self, uuid, dasks):
        self._uuid = uuid
        self._dasks = dasks

    __add__ = daskify(operator.add)
    __sub__ = daskify(operator.sub)
    __mul__ = daskify(operator.mul)
    __div__ = daskify(operator.div)
    __getitem__ = daskify(operator.getitem)

    def compute(self, **kwargs):
        dask1 = merge(*self._dasks)
        dask2 = cull(dask1, self._uuid)
        return get(dask2, self._uuid, **kwargs)

    @property
    def dask(self):
        return merge(*self._dasks)


def value(val):
    """Create a value from a python object"""
    uuid = uuid4().hex
    return Value(uuid, [{uuid: val}])

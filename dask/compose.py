import operator
from functools import partial, wraps
from itertools import chain

from toolz import merge

from .core import preorder_traversal, istask
from .optimize import cull
from .context import _globals
from . import threaded


def get_dasks(task):
    dsks = chain.from_iterable(t._dasks for t in preorder_traversal(task)
                               if isinstance(t, Value))
    return list(dict((id(d), d) for d in dsks).values())


def insert_lists(task):
    if isinstance(task, list):
        return (list, [insert_lists(i) for i in task])
    elif istask(task):
        return (task[0],) + tuple(insert_lists(i) for i in task[1:])
    else:
        return task


def tokenize(v):
    try:
        return str(hash(v))
    except TypeError:
        pass
    return str(hash(str(v)))


def applyfunc(func, *args, **kwargs):
    if kwargs:
        func = partial(func, **kwargs)
    task = insert_lists((func,) + args)
    dasks = get_dasks(task)
    name = tokenize((func, args, frozenset(kwargs.items())))
    new_dsk = {}
    dasks.append(new_dsk)
    res = Value(name, dasks)
    new_dsk[res] = task
    return res


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

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return "Value({0})".format(repr(self._name))

    __lt__ = daskify(operator.lt)
    __le__ = daskify(operator.le)
    __eq__ = daskify(operator.eq)
    __ne__ = daskify(operator.ne)
    __ge__ = daskify(operator.ge)
    __gt__ = daskify(operator.gt)
    __not__ = daskify(operator.not_)
    __abs__ = daskify(operator.abs)
    __add__ = daskify(operator.add)
    __and__ = daskify(operator.and_)
    __div__ = daskify(operator.floordiv)
    __floordiv__ = daskify(operator.floordiv)
    __index__ = daskify(operator.index)
    __inv__ = daskify(operator.inv)
    __invert__ = daskify(operator.invert)
    __lshift__ = daskify(operator.lshift)
    __mod__ = daskify(operator.mod)
    __mul__ = daskify(operator.mul)
    __neg__ = daskify(operator.neg)
    __or__ = daskify(operator.or_)
    __pos__ = daskify(operator.pos)
    __pow__ = daskify(operator.pow)
    __rshift__ = daskify(operator.rshift)
    __sub__ = daskify(operator.sub)
    __truediv__ = daskify(operator.truediv)
    __xor__ = daskify(operator.xor)
    __concat__ = daskify(operator.concat)
    __contains__ = daskify(operator.contains)
    __delitem__ = daskify(operator.delitem)
    __getitem__ = daskify(operator.getitem)
    __setitem__ = daskify(operator.setitem)

    def compute(self, **kwargs):
        dask1 = merge(*self._dasks)
        dask2 = cull(dask1, self)
        return get(dask2, self, **kwargs)

    @property
    def dask(self):
        return merge(*self._dasks)

    def visualize(self):
        from dask.dot import dot_graph
        dot_graph(self.dask)


def value(val, name=None):
    """Create a value from a python object"""
    name = name or tokenize(val)
    dsk = {}
    res = Value(name, [dsk])
    dsk[res] = val
    return res

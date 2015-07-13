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


def methodcaller(self, method, *args, **kwargs):
    return getattr(self, method)(*args, **kwargs)


def right(method):
    def _inner(self, other):
        return method(other, self)
    return _inner


class Value(object):
    """Represents a value to be computed by dask.

    Equivalent to the output from a single key in a dask graph.
    """
    __slots__ = ('_name', '_dasks')

    def __init__(self, name, dasks):
        object.__setattr__(self, '_name', name)
        object.__setattr__(self, '_dasks', dasks)

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

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return "Value({0})".format(repr(self._name))

    def __dir__(self):
        return list(self.__dict__.keys())

    def __getattr__(self, attr):
        if not attr.startswith('__'):
            def _inner(*args, **kwargs):
                return daskify(methodcaller)(self, attr, *args, **kwargs)
            return _inner
        else:
            raise AttributeError("Attribute {0} not found".format(attr))

    def __setattr__(self, attr, val):
        raise TypeError("Value objects are immutable")

    def __iter__(self):
        raise TypeError("Value objects are not iterable")

    __abs__ = daskify(operator.abs)
    __add__ = daskify(operator.add)
    __and__ = daskify(operator.and_)
    __concat__ = daskify(operator.concat)
    __contains__ = daskify(operator.contains)
    __delitem__ = daskify(operator.delitem)
    __div__ = daskify(operator.floordiv)
    __eq__ = daskify(operator.eq)
    __floordiv__ = daskify(operator.floordiv)
    __ge__ = daskify(operator.ge)
    __getitem__ = daskify(operator.getitem)
    __gt__ = daskify(operator.gt)
    __index__ = daskify(operator.index)
    __inv__ = daskify(operator.inv)
    __invert__ = daskify(operator.invert)
    __le__ = daskify(operator.le)
    __lshift__ = daskify(operator.lshift)
    __lt__ = daskify(operator.lt)
    __mod__ = daskify(operator.mod)
    __mul__ = daskify(operator.mul)
    __ne__ = daskify(operator.ne)
    __neg__ = daskify(operator.neg)
    __not__ = daskify(operator.not_)
    __or__ = daskify(operator.or_)
    __pos__ = daskify(operator.pos)
    __pow__ = daskify(operator.pow)
    __radd__ = daskify(right(operator.add))
    __rand__ = daskify(right(operator.and_))
    __rdiv__ = daskify(right(operator.floordiv))
    __rfloordiv__ = daskify(right(operator.floordiv))
    __rlshift__ = daskify(right(operator.lshift))
    __rmod__ = daskify(right(operator.mod))
    __rmul__ = daskify(right(operator.mul))
    __ror__ = daskify(right(operator.or_))
    __rpow__ = daskify(right(operator.pow))
    __rrshift__ = daskify(right(operator.rshift))
    __rshift__ = daskify(operator.rshift)
    __rsub__ = daskify(right(operator.sub))
    __rtruediv__ = daskify(right(operator.truediv))
    __rxor__ = daskify(right(operator.xor))
    __setitem__ = daskify(operator.setitem)
    __sub__ = daskify(operator.sub)
    __truediv__ = daskify(operator.truediv)
    __xor__ = daskify(operator.xor)


def value(val, name=None):
    """Create a value from a python object"""
    name = name or tokenize(val)
    dsk = {}
    res = Value(name, [dsk])
    dsk[res] = val
    return res

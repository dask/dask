import operator
from functools import partial, wraps
from itertools import chain

from toolz import merge

from .core import preorder_traversal, istask
from .optimize import cull
from .context import _globals
from . import threaded

__all__ = ['do', 'value', 'Value']


def get_dasks(task):
    """Given a task, return a list of all unique dasks for any `Value` object
    it's composed of"""
    dsks = chain.from_iterable(t._dasks for t in preorder_traversal(task)
                               if isinstance(t, Value))
    return list(dict((id(d), d) for d in dsks).values())


def insert_lists(task):
    """Insert calls to `list` in a task around all literal lists"""
    if isinstance(task, list):
        return (list, [insert_lists(i) for i in task])
    elif istask(task):
        return (task[0],) + tuple(insert_lists(i) for i in task[1:])
    else:
        return task


def tokenize(v):
    """Mapping function from task -> consistent name"""
    # TODO: May have hash collisions...
    try:
        return str(hash(v))
    except TypeError:
        pass
    return str(hash(str(v)))


def applyfunc(func, *args, **kwargs):
    """Create a Value by applying a function to args.

    Given a function and arguments, return a Value that represents the result
    of that computation."""

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


def do(func):
    """Wraps a function so that it outputs a ``Value``.

    Examples
    --------
    Can be used as a decorator:

    >>> @do
    ... def add(a, b):
    ...     return a + b
    >>> res = add(1, 2)
    >>> type(res)
    Value
    >>> res.compute()
    3

    For other cases, it may be cleaner to call ``do`` on a function at call
    time:

    >>> res2 = do(sum)([res, 2, 3])
    >>> type(res2)
    Value
    >>> res2.compute()
    8
    """
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
    """Calls method on self with args/kwargs"""
    return getattr(self, method)(*args, **kwargs)


def right(method):
    """Wrapper to create 'right' version of operator given left version"""
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
        """Compute the result."""
        dask1 = merge(*self._dasks)
        dask2 = cull(dask1, self)
        return get(dask2, self, **kwargs)

    @property
    def dask(self):
        return merge(*self._dasks)

    def visualize(self, **kwargs):
        """Visualize the dask as a graph"""
        from dask.dot import dot_graph
        dot_graph(self.dask, **kwargs)

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return "Value({0})".format(repr(self._name))

    def __dir__(self):
        return list(self.__dict__.keys())

    def __getattr__(self, attr):
        if not attr.startswith('_'):
            def _inner(*args, **kwargs):
                return do(methodcaller)(self, attr, *args, **kwargs)
            return _inner
        else:
            raise AttributeError("Attribute {0} not found".format(attr))

    def __setattr__(self, attr, val):
        raise TypeError("Value objects are immutable")

    def __iter__(self):
        raise TypeError("Value objects are not iterable")

    __abs__ = do(operator.abs)
    __add__ = do(operator.add)
    __and__ = do(operator.and_)
    __concat__ = do(operator.concat)
    __delitem__ = do(operator.delitem)
    __div__ = do(operator.floordiv)
    __eq__ = do(operator.eq)
    __floordiv__ = do(operator.floordiv)
    __ge__ = do(operator.ge)
    __getitem__ = do(operator.getitem)
    __gt__ = do(operator.gt)
    __index__ = do(operator.index)
    __inv__ = do(operator.inv)
    __invert__ = do(operator.invert)
    __le__ = do(operator.le)
    __lshift__ = do(operator.lshift)
    __lt__ = do(operator.lt)
    __mod__ = do(operator.mod)
    __mul__ = do(operator.mul)
    __ne__ = do(operator.ne)
    __neg__ = do(operator.neg)
    __not__ = do(operator.not_)
    __or__ = do(operator.or_)
    __pos__ = do(operator.pos)
    __pow__ = do(operator.pow)
    __radd__ = do(right(operator.add))
    __rand__ = do(right(operator.and_))
    __rdiv__ = do(right(operator.floordiv))
    __rfloordiv__ = do(right(operator.floordiv))
    __rlshift__ = do(right(operator.lshift))
    __rmod__ = do(right(operator.mod))
    __rmul__ = do(right(operator.mul))
    __ror__ = do(right(operator.or_))
    __rpow__ = do(right(operator.pow))
    __rrshift__ = do(right(operator.rshift))
    __rshift__ = do(operator.rshift)
    __rsub__ = do(right(operator.sub))
    __rtruediv__ = do(right(operator.truediv))
    __rxor__ = do(right(operator.xor))
    __setitem__ = do(operator.setitem)
    __sub__ = do(operator.sub)
    __truediv__ = do(operator.truediv)
    __xor__ = do(operator.xor)


def value(val, name=None):
    """Create a ``Value`` from a python object.

    Parameters
    ----------
    val : object
        Object to be wrapped.
    name : string, optional
        Name to be used in the resulting dask.

    Examples
    --------
    >>> a = value([1, 2, 3])
    >>> a.compute()
    [1, 2, 3]

    Values can act as a proxy to the underlying object. Many operators are
    supported:

    >>> (a + [1, 2]).compute()
    [1, 2, 3, 1, 2]
    >>> a[1].compute()
    2

    Method axis also works:

    >>> a.count(2).compute()
    1

    Note that if a method doesn't exist, no error will be thrown until runtime:

    >>> res = a.not_a_real_method()
    >>> res.compute()  # doctest: +SKIP
    AttributeError("'list' object has no attribute 'not_a_real_method'")
    """

    name = name or tokenize(val)
    dsk = {}
    res = Value(name, [dsk])
    dsk[res] = val
    return res

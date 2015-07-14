import operator
from functools import partial, wraps
from itertools import chain
from collections import Iterator

from toolz import merge, unique

from .core import istask
from .optimize import cull, fuse
from .context import _globals
from .compatibility import apply
from . import threaded

__all__ = ['do', 'value', 'Value']


def flat_unique(ls):
    """Flatten ``ls``, filter by unique id, and return a list"""
    return list(unique(chain.from_iterable(ls), key=id))


def unzip(ls, nout):
    """Unzip a list of lists into ``nout`` outputs."""
    out = list(zip(*ls))
    if not out:
        out = [()] * nout
    return out


def to_task_dasks(task):
    """Normalize a task and extract all sub-dasks.

    - Replace ``Values`` with their keys
    - Convert literals to things the schedulers can handle
    - Extract dasks from all enclosed values

    Returns
    -------
    task : normalized task to be run
    dasks : list of dasks that form the dag for this task
    """
    if istask(task) and not isinstance(task[0], Value):
        args, dasks = unzip(map(to_task_dasks, task[1:]), 2)
        return (task[0],) + tuple(args), flat_unique(dasks)
    elif isinstance(task, Value):
        return task.key, task._dasks
    elif isinstance(task, (Iterator, list, tuple, set)):
        args, dasks = unzip(map(to_task_dasks, task), 2)
        args = list(args)
        dasks = flat_unique(dasks)
        # Ensure output type matches input type
        if isinstance(task, (list, tuple, set)):
            return (type(task), args), dasks
        else:
            return args, dasks
    elif isinstance(task, dict):
        args, dasks = to_task_dasks(tuple((k, v) for k, v in task.items()))
        return (dict, args), dasks
    else:
        return task, []


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
    task, dasks = to_task_dasks((func,) + args)
    name = tokenize((func, args, frozenset(kwargs.items())))
    dasks.append({name: task})
    return Value(name, dasks)


def do(func):
    """Wraps a function so that it outputs a ``Value``.

    Examples
    --------
    Can be used as a decorator:

    >>> @do
    ... def add(a, b):
    ...     return a + b
    >>> res = add(1, 2)
    >>> type(res) == Value
    True
    >>> res.compute()
    3

    For other cases, it may be cleaner to call ``do`` on a function at call
    time:

    >>> res2 = do(sum)([res, 2, 3])
    >>> res2.compute()
    8
    """
    @wraps(func)
    def _dfunc(*args, **kwargs):
        return applyfunc(func, *args, **kwargs)
    return _dfunc


def optimize(dsk, keys):
    dsk2 = cull(dsk, keys)
    return fuse(dsk2)


def get(dsk, keys, get=None, **kwargs):
    """Specialized get function"""
    get = get or _globals['get'] or threaded.get
    dsk2 = optimize(dsk, keys)
    return get(dsk2, keys, **kwargs)


def right(method):
    """Wrapper to create 'right' version of operator given left version"""
    def _inner(self, other):
        return method(other, self)
    return _inner


class Value(object):
    """Represents a value to be computed by dask.

    Equivalent to the output from a single key in a dask graph.
    """
    __slots__ = ('_key', '_dasks')

    def __init__(self, name, dasks):
        object.__setattr__(self, '_key', name)
        object.__setattr__(self, '_dasks', dasks)

    def compute(self, **kwargs):
        """Compute the result."""
        dask1 = merge(*self._dasks)
        dask2 = cull(dask1, self.key)
        return get(dask2, self.key, **kwargs)

    @property
    def dask(self):
        return merge(*self._dasks)

    @property
    def key(self):
        return self._key

    def visualize(self, optimize_graph=False, **kwargs):
        """Visualize the dask as a graph"""
        from dask.dot import dot_graph
        if optimize_graph:
            dot_graph(optimize(self.dask, self.key), **kwargs)
        else:
            dot_graph(self.dask, **kwargs)

    def __repr__(self):
        return "Value({0})".format(repr(self._key))

    def __hash__(self):
        return hash(self.key)

    def __dir__(self):
        return list(self.__dict__.keys())

    def __getattr__(self, attr):
        if not attr.startswith('_'):
            return do(getattr)(self, attr)
        else:
            raise AttributeError("Attribute {0} not found".format(attr))

    def __setattr__(self, attr, val):
        raise TypeError("Value objects are immutable")

    def __iter__(self):
        raise TypeError("Value objects are not iterable")

    def __call__(self, *args, **kwargs):
        return do(apply)(self, args, kwargs)

    def __bool__(self):
        raise TypeError("Truth of Value objects is not supported")

    __nonzero__ = __bool__

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

    Method and attribute access also works:

    >>> a.count(2).compute()
    1

    Note that if a method doesn't exist, no error will be thrown until runtime:

    >>> res = a.not_a_real_method()
    >>> res.compute()  # doctest: +SKIP
    AttributeError("'list' object has no attribute 'not_a_real_method'")
    """

    name = name or tokenize(val)
    task, dasks = to_task_dasks(val)
    dasks.append({name: task})
    return Value(name, dasks)

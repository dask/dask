from __future__ import absolute_import, division, print_function

from collections import Iterator
from functools import wraps
from itertools import chain, count
import operator
import uuid

from toolz import merge, unique, curry, first

from .optimize import cull, fuse
from .utils import concrete, funcname, ignoring
from . import base
from .compatibility import apply
from . import threaded

__all__ = ['compute', 'do', 'value', 'Value']


def flat_unique(ls):
    """Flatten ``ls``, filter by unique id, and return a list"""
    return list(unique(chain.from_iterable(ls), key=id))


def unzip(ls, nout):
    """Unzip a list of lists into ``nout`` outputs."""
    out = list(zip(*ls))
    if not out:
        out = [()] * nout
    return out


def to_task_dasks(expr):
    """Normalize a python object and extract all sub-dasks.

    - Replace ``Values`` with their keys
    - Convert literals to things the schedulers can handle
    - Extract dasks from all enclosed values

    Parameters
    ----------
    expr : object
        The object to be normalized. This function knows how to handle
        ``Value``s, as well as most builtin python types.

    Returns
    -------
    task : normalized task to be run
    dasks : list of dasks that form the dag for this task

    Examples
    --------

    >>> a = value(1, 'a')
    >>> b = value(2, 'b')
    >>> task, dasks = to_task_dasks([a, b, 3])
    >>> task # doctest: +SKIP
    ['a', 'b', 3]
    >>> dasks # doctest: +SKIP
    [{'a': 1}, {'b': 2}]

    >>> task, dasks = to_task_dasks({a: 1, b: 2})
    >>> task # doctest: +SKIP
    (dict, [['a', 1], ['b', 2]])
    >>> dasks # doctest: +SKIP
    [{'a': 1}, {'b': 2}]
    """
    if isinstance(expr, Value):
        return expr.key, expr._dasks
    if isinstance(expr, base.Base):
        name = tokenize(expr, pure=True)
        keys = expr._keys()
        dsk = expr._optimize(expr.dask, keys)
        dsk[name] = (expr._finalize, (concrete, keys))
        return name, [dsk]
    if isinstance(expr, tuple) and type(expr) != tuple:
        return expr, []
    if isinstance(expr, (Iterator, list, tuple, set)):
        args, dasks = unzip(map(to_task_dasks, expr), 2)
        args = list(args)
        dasks = flat_unique(dasks)
        # Ensure output type matches input type
        if isinstance(expr, (tuple, set)):
            return (type(expr), args), dasks
        else:
            return args, dasks
    if isinstance(expr, dict):
        args, dasks = to_task_dasks([[k, v] for k, v in expr.items()])
        return (dict, args), dasks
    return expr, []


def tokenize(*args, **kwargs):
    """Mapping function from task -> consistent name.

    Parameters
    ----------
    args : object
        Python objects that summarize the task.
    pure : boolean, optional
        If True, a consistent hash function is tried on the input. If this
        fails, then a unique identifier is used. If False (default), then a
        unique identifier is always used.
    """
    if kwargs.pop('pure', False):
        return base.tokenize(*args)
    else:
        return str(uuid.uuid4())


def applyfunc(func, args, kwargs, pure=False):
    """Create a Value by applying a function to args.

    Given a function and arguments, return a Value that represents the result
    of that computation."""

    args, dasks = unzip(map(to_task_dasks, args), 2)
    if kwargs:
        dask_kwargs, dasks2 = to_task_dasks(kwargs)
        dasks = dasks + (dasks2,)
        task = (apply, func, list(args), dask_kwargs)
    else:
        task = (func,) + args
    name = funcname(func) + '-' + tokenize(*task, pure=pure)
    dasks = flat_unique(dasks)
    dasks.append({name: task})
    return Value(name, dasks)


@curry
def delayed(func, pure=False):
    """Wraps a function so that it outputs a ``Value``.

    Examples
    --------
    Can be used as a decorator:

    >>> @delayed
    ... def add(a, b):
    ...     return a + b
    >>> res = add(1, 2)
    >>> type(res) == Value
    True
    >>> res.compute()
    3

    For other cases, it may be cleaner to call ``do`` on a function at call
    time:

    >>> res2 = delayed(sum)([res, 2, 3])
    >>> res2.compute()
    8

    ``do`` also accepts an optional keyword ``pure``. If False (default), then
    subsequent calls will always produce a different ``Value``. This is useful
    for non-pure functions (such as ``time`` or ``random``).

    >>> from random import random
    >>> out1 = delayed(random)()
    >>> out2 = delayed(random)()
    >>> out1.key == out2.key
    False

    If you know a function is pure (output only depends on the input, with no
    global state), then you can set ``pure=True``. This will attempt to apply a
    consistent name to the output, but will fallback on the same behavior of
    ``pure=False`` if this fails.

    >>> @delayed(pure=True)
    ... def add(a, b):
    ...     return a + b
    >>> out1 = add(1, 2)
    >>> out2 = add(1, 2)
    >>> out1.key == out2.key
    True
    """
    def _dfunc(*args, **kwargs):
        return applyfunc(func, args, kwargs, pure=pure)

    with ignoring(AttributeError):
        _dfunc = wraps(func)(_dfunc)

    return _dfunc


do = delayed


def compute(*args, **kwargs):
    """Evaluate more than one ``Value`` at once.

    Note that the only difference between this function and
    ``dask.base.compute`` is that this implicitly wraps python objects in
    ``Value``, allowing for collections of dask objects to be computed.

    Examples
    --------
    >>> a = value(1)
    >>> b = a + 2
    >>> c = a + 3
    >>> compute(b, c)  # Compute both simultaneously
    (3, 4)
    >>> compute(a, [b, c])  # Works for lists of Values
    (1, [3, 4])
    """
    args = [value(a) for a in args]
    return base.compute(*args, **kwargs)


def right(method):
    """Wrapper to create 'right' version of operator given left version"""
    def _inner(self, other):
        return method(other, self)
    return _inner


class Value(base.Base):
    """Represents a value to be computed by dask.

    Equivalent to the output from a single key in a dask graph.
    """
    __slots__ = ('_key', '_dasks')
    _optimize = staticmethod(lambda dsk, keys, **kwargs: dsk)
    _finalize = staticmethod(first)
    _default_get = staticmethod(threaded.get)

    def __init__(self, name, dasks):
        object.__setattr__(self, '_key', name)
        object.__setattr__(self, '_dasks', dasks)

    def __setstate__(self, state):
        self.__init__(*state)
        return self

    def __getstate__(self):
        return (self._key, self._dasks)

    @property
    def dask(self):
        return merge(*self._dasks)

    @property
    def key(self):
        return self._key

    def _keys(self):
        return [self.key]

    def __repr__(self):
        return "Value({0})".format(repr(self.key))

    def __hash__(self):
        return hash(self.key)

    def __dir__(self):
        return dir(type(self))

    def __getattr__(self, attr):
        if not attr.startswith('_'):
            return do(getattr, pure=True)(self, attr)
        else:
            raise AttributeError("Attribute {0} not found".format(attr))

    def __setattr__(self, attr, val):
        raise TypeError("Value objects are immutable")

    def __setitem__(self, index, val):
        raise TypeError("Value objects are immutable")

    def __iter__(self):
        raise TypeError("Value objects are not iterable")

    def __call__(self, *args, **kwargs):
        return do(apply, kwargs.pop('pure', False))(self, args, kwargs)

    def __bool__(self):
        raise TypeError("Truth of Value objects is not supported")

    __nonzero__ = __bool__

    __abs__ = do(operator.abs, True)
    __add__ = do(operator.add, True)
    __and__ = do(operator.and_, True)
    __div__ = do(operator.floordiv, True)
    __eq__ = do(operator.eq, True)
    __floordiv__ = do(operator.floordiv, True)
    __ge__ = do(operator.ge, True)
    __getitem__ = do(operator.getitem, True)
    __gt__ = do(operator.gt, True)
    __index__ = do(operator.index, True)
    __invert__ = do(operator.invert, True)
    __le__ = do(operator.le, True)
    __lshift__ = do(operator.lshift, True)
    __lt__ = do(operator.lt, True)
    __mod__ = do(operator.mod, True)
    __mul__ = do(operator.mul, True)
    __ne__ = do(operator.ne, True)
    __neg__ = do(operator.neg, True)
    __or__ = do(operator.or_, True)
    __pos__ = do(operator.pos, True)
    __pow__ = do(operator.pow, True)
    __radd__ = do(right(operator.add), True)
    __rand__ = do(right(operator.and_), True)
    __rdiv__ = do(right(operator.floordiv), True)
    __rfloordiv__ = do(right(operator.floordiv), True)
    __rlshift__ = do(right(operator.lshift), True)
    __rmod__ = do(right(operator.mod), True)
    __rmul__ = do(right(operator.mul), True)
    __ror__ = do(right(operator.or_), True)
    __rpow__ = do(right(operator.pow), True)
    __rrshift__ = do(right(operator.rshift), True)
    __rshift__ = do(operator.rshift, True)
    __rsub__ = do(right(operator.sub), True)
    __rtruediv__ = do(right(operator.truediv), True)
    __rxor__ = do(right(operator.xor), True)
    __sub__ = do(operator.sub, True)
    __truediv__ = do(operator.truediv, True)
    __xor__ = do(operator.xor, True)


base.normalize_token.register(Value, lambda a: a.key)


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

    Methods are assumed to be impure by default, meaning that subsequent calls
    may return different results. To assume purity, set `pure=True`. This
    allows sharing of any intermediate values.

    >>> a.count(2, pure=True).key == a.count(2, pure=True).key
    True
    """
    if isinstance(val, Value):
        return val
    task, dasks = to_task_dasks(val)
    name = name or (type(val).__name__ + '-' + tokenize(task, pure=True))
    dasks.append({name: task})
    return Value(name, dasks)

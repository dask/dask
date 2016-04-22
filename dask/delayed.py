from __future__ import absolute_import, division, print_function

from collections import Iterator
from functools import wraps
from itertools import chain, count
import operator
import uuid
from warnings import warn

from toolz import merge, unique, curry, first

from .async import get_sync
from .optimize import cull, fuse
from .utils import concrete, funcname, ignoring
from . import base
from .compatibility import apply
from . import threaded

__all__ = ['compute', 'do', 'value', 'Value', 'delayed', 'delayed_value',
           'DelayedValue']


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

    - Replace ``DelayedValue``s with their keys
    - Convert literals to things the schedulers can handle
    - Extract dasks from all enclosed values

    Parameters
    ----------
    expr : object
        The object to be normalized. This function knows how to handle
        ``DelayedValue``s, as well as most builtin python types.

    Returns
    -------
    task : normalized task to be run
    dasks : list of dasks that form the dag for this task

    Examples
    --------

    >>> a = DelayedValue(1, 'a')
    >>> b = DelayedValue(2, 'b')
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
    if isinstance(expr, DelayedValue):
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
    """Create a DelayedValue by applying a function to args.

    Given a function and arguments, return a DelayedValue that represents the
    result of that computation.
    """
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
    return DelayedValue(name, dasks)


class DelayedFunction(base.Base):
    __slots__ = ('function', 'pure')
    _optimize = staticmethod(lambda dsk, keys, **kwargs: dsk)
    _finalize = staticmethod(first)
    _default_get = staticmethod(get_sync)

    def __init__(self, function, pure=False):
        self.function = function
        self.pure = pure

    @property
    def key(self):
        return '%s-%d' % (funcname(self.function), id(self.function))

    def _keys(self):
        return [self.key]

    @property
    def dask(self):
        return {self.key: self.function}

    def __call__(self, *args, **kwargs):
        return applyfunc(self.function, args, kwargs, pure=self.pure)


@curry
def delayed(obj, name=None, pure=False):
    """ Wrap a function or object to produce dask delayed results

    When applied to a function the result generates lazy dask values.

    When applied to an object the result is a lazy dask value.

    Parameters
    ----------
    obj: callable or object
        The function or object to wrap
    name: string or key
        If given an object, the key to use.  Defaults to hashing content.
    pure: bool, optional (False)
        If given a function, whether or not the function is pure

    Examples
    --------
    Operates on functions to delay execution:

    >>> def inc(x):
    ...     return x + 1

    >>> inc(10)
    11

    >>> x = delayed(inc, pure=True)(10)
    >>> type(x).__name__
    'DelayedValue'

    >>> x.compute()
    11

    Can be used as a decorator:

    >>> @delayed(pure=True)
    ... def add(a, b):
    ...     return a + b

    >>> add(1, 2)

    ``delayed`` also accepts an optional keyword ``pure``. If False (default),
    then subsequent calls will always produce a different ``DelayedValue``.
    This is useful for non-pure functions (such as ``time`` or ``random``).

    >>> from random import random
    >>> out1 = delayed(random, pure=False)()
    >>> out2 = delayed(random, pure=False)()
    >>> out1.key == out2.key
    False

    If you know a function is pure (output only depends on the input, with no
    global state or randomness), then you can set ``pure=True``. This will
    attempt to apply a consistent name to the output, but will fallback on the
    same behavior of ``pure=False`` if this fails.

    >>> @delayed(pure=True)
    ... def add(a, b):
    ...     return a + b
    >>> out1 = add(1, 2)
    >>> out2 = add(1, 2)
    >>> out1.key == out2.key
    True

    Delayed may operate on objects themselves, not just functions:

    >>> a = delayed([1, 2, 3])
    >>> a.compute()
    [1, 2, 3]

    Delayed results act as a proxy to the underlying object.
    Many operators are supported:

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
    if isinstance(obj, DelayedValue):
        return obj
    elif callable(obj):
        return DelayedFunction(obj, pure=pure)
    else:
        task, dasks = to_task_dasks(obj)
        name = name or (type(obj).__name__ + '-' + tokenize(task, pure=True))
        dasks.append({name: task})
        return DelayedValue(name, dasks)


do = delayed


def compute(*args, **kwargs):
    """Evaluate more than one ``DelayedValue`` at once.

    Note that the only difference between this function and
    ``dask.base.compute`` is that this implicitly wraps python objects in
    ``DelayedValue``, allowing for collections of dask objects to be computed.

    Examples
    --------
    >>> a = delayed(1)
    >>> b = a + 2
    >>> c = a + 3
    >>> compute(b, c)  # Compute both simultaneously
    (3, 4)
    >>> compute(a, [b, c])  # Works for lists of DelayedValues
    (1, [3, 4])
    """
    args = [delayed(a) for a in args]
    return base.compute(*args, **kwargs)


def right(method):
    """Wrapper to create 'right' version of operator given left version"""
    def _inner(self, other):
        return method(other, self)
    return _inner


class DelayedValue(base.Base):
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
        return "DelayedValue({0})".format(repr(self.key))

    def __hash__(self):
        return hash(self.key)

    def __dir__(self):
        return dir(type(self))

    def __getattr__(self, attr):
        if not attr.startswith('_'):
            return delayed(getattr, pure=True)(self, attr)
        else:
            raise AttributeError("Attribute {0} not found".format(attr))

    def __setattr__(self, attr, val):
        raise TypeError("DelayedValue objects are immutable")

    def __setitem__(self, index, val):
        raise TypeError("DelayedValue objects are immutable")

    def __iter__(self):
        raise TypeError("DelayedValue objects are not iterable")

    def __call__(self, *args, **kwargs):
        pure = kwargs.pop('pure', False)
        return delayed(apply, pure=pure)(self, args, kwargs)

    def __bool__(self):
        raise TypeError("Truth of DelayedValue objects is not supported")

    __nonzero__ = __bool__

    def __abs__(self):
        return delayed(operator.abs, pure=True)(self)

    def __add__(self, other):
        return delayed(operator.add, pure=True)(self, other)

    def __and__(self, other):
        return delayed(operator.and_, pure=True)(self, other)

    def __div__(self, other):
        return delayed(operator.floordiv, pure=True)(self, other)

    def __eq__(self, other):
        return delayed(operator.eq, pure=True)(self, other)

    def __floordiv__(self, other):
        return delayed(operator.floordiv, pure=True)(self, other)

    def __ge__(self, other):
        return delayed(operator.ge, pure=True)(self, other)

    def __getitem__(self, other):
        return delayed(operator.getitem, pure=True)(self, other)

    def __gt__(self, other):
        return delayed(operator.gt, pure=True)(self, other)

    def __index__(self):
        return delayed(operator.index, pure=True)(self)

    def __invert__(self, other):
        return delayed(operator.invert, pure=True)(self, other)

    def __le__(self, other):
        return delayed(operator.le, pure=True)(self, other)

    def __lshift__(self, other):
        return delayed(operator.lshift, pure=True)(self, other)

    def __lt__(self, other):
        return delayed(operator.lt, pure=True)(self, other)

    def __mod__(self, other):
        return delayed(operator.mod, pure=True)(self, other)

    def __mul__(self, other):
        return delayed(operator.mul, pure=True)(self, other)

    def __ne__(self, other):
        return delayed(operator.ne, pure=True)(self, other)

    def __neg__(self):
        return delayed(operator.neg, pure=True)(self)

    def __or__(self, other):
        return delayed(operator.or_, pure=True)(self, other)

    def __pos__(self):
        return delayed(operator.pos, pure=True)(self)

    def __pow__(self, other):
        return delayed(operator.pow, pure=True)(self, other)

    def __radd__(self, other):
        return delayed(right(operator.add), pure=True)(self, other)

    def __rand__(self, other):
        return delayed(right(operator.and_), pure=True)(self, other)

    def __rdiv__(self, other):
        return delayed(right(operator.floordiv), pure=True)(self, other)

    def __rfloordiv__(self, other):
        return delayed(right(operator.floordiv), pure=True)(self, other)

    def __rlshift__(self, other):
        return delayed(right(operator.lshift), pure=True)(self, other)

    def __rmod__(self, other):
        return delayed(right(operator.mod), pure=True)(self, other)

    def __rmul__(self, other):
        return delayed(right(operator.mul), pure=True)(self, other)

    def __ror__(self, other):
        return delayed(right(operator.or_), pure=True)(self, other)

    def __rpow__(self, other):
        return delayed(right(operator.pow), pure=True)(self, other)

    def __rrshift__(self, other):
        return delayed(right(operator.rshift), pure=True)(self, other)

    def __rshift__(self, other):
        return delayed(operator.rshift, pure=True)(self, other)

    def __rsub__(self, other):
        return delayed(right(operator.sub), pure=True)(self, other)

    def __rtruediv__(self, other):
        return delayed(right(operator.truediv), pure=True)(self, other)

    def __rxor__(self, other):
        return delayed(right(operator.xor), pure=True)(self, other)

    def __sub__(self, other):
        return delayed(operator.sub, pure=True)(self, other)

    def __truediv__(self, other):
        return delayed(operator.truediv, pure=True)(self, other)

    def __xor__(self, other):
        return delayed(operator.xor, pure=True)(self, other)



Value = DelayedValue
base.normalize_token.register(DelayedValue, lambda a: a.key)


def value(val, name=None):
    warn("Deprcation warning: moved to delayed")
    return delayed(val, name=name)

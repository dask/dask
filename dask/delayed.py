from __future__ import absolute_import, division, print_function

from collections import Iterator
from itertools import chain
from warnings import warn
import operator
import uuid

from toolz import merge, unique, curry, first

from .utils import concrete, funcname
from . import base
from .compatibility import apply
from . import threaded
from .optimize import inline_functions
from .core import flatten

__all__ = ['compute', 'do', 'Delayed', 'delayed']


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

    - Replace ``Delayed`` with their keys
    - Convert literals to things the schedulers can handle
    - Extract dasks from all enclosed values

    Parameters
    ----------
    expr : object
        The object to be normalized. This function knows how to handle
        ``Delayed``s, as well as most builtin python types.

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
    if isinstance(expr, Delayed):
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


@curry
def delayed(obj, name=None, pure=False):
    """Wraps a function or object to produce a ``Delayed``.

    ``Delayed`` objects act as proxies for the object they wrap, but all
    operations on them are done lazily by building up a dask graph internally.

    Parameters
    ----------
    obj : object
        The function or object to wrap
    name : string or hashable, optional
        The key to use in the underlying graph. Defaults to hashing content.
    pure : bool, optional
        Indicates whether calling the resulting ``Delayed`` object is a pure
        operation. If True, arguments to the call are hashed to produce
        deterministic keys. Default is False.

    Examples
    --------
    Apply to functions to delay execution:

    >>> def inc(x):
    ...     return x + 1

    >>> inc(10)
    11

    >>> x = delayed(inc, pure=True)(10)
    >>> type(x) == Delayed
    True
    >>> x.compute()
    11

    Can be used as a decorator:

    >>> @delayed(pure=True)
    ... def add(a, b):
    ...     return a + b
    >>> add(1, 2).compute()
    3

    ``delayed`` also accepts an optional keyword ``pure``. If False (default),
    then subsequent calls will always produce a different ``Delayed``. This is
    useful for non-pure functions (such as ``time`` or ``random``).

    >>> from random import random
    >>> out1 = delayed(random, pure=False)()
    >>> out2 = delayed(random, pure=False)()
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

    ``delayed`` can also be applied to objects to make operations on them lazy:

    >>> a = delayed([1, 2, 3])
    >>> type(a) == Delayed
    True
    >>> a.compute()
    [1, 2, 3]

    Delayed results act as a proxy to the underlying object. Many operators
    are supported:

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
    if isinstance(obj, Delayed):
        return obj

    task, dasks = to_task_dasks(obj)

    if not dasks:
        return DelayedLeaf(obj, pure=pure, name=name)
    else:
        name = name or '%s-%s' % (type(obj).__name__, tokenize(task))
        dasks.append({name: task})
        return Delayed(name, dasks)


do = delayed


def compute(*args, **kwargs):
    """Evaluate more than one ``Delayed`` at once.

    Note that the only difference between this function and
    ``dask.base.compute`` is that this implicitly wraps python objects in
    ``Delayed``, allowing for collections of dask objects to be computed.

    Examples
    --------
    >>> a = value(1)
    >>> b = a + 2
    >>> c = a + 3
    >>> compute(b, c)  # Compute both simultaneously
    (3, 4)
    >>> compute(a, [b, c])  # Works for lists of Delayed
    (1, [3, 4])
    """
    args = [delayed(a) for a in args]
    return base.compute(*args, **kwargs)


def right(method):
    """Wrapper to create 'right' version of operator given left version"""
    def _inner(self, other):
        return method(other, self)
    return _inner


class Delayed(base.Base):
    """Represents a value to be computed by dask.

    Equivalent to the output from a single key in a dask graph.
    """
    __slots__ = ('_key', '_dasks')
    _finalize = staticmethod(first)
    _default_get = staticmethod(threaded.get)

    @staticmethod
    def _optimize(dsk, keys, **kwargs):
        return inline_functions(dsk, list(flatten(keys)), [getattr])

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
        return "Delayed({0})".format(repr(self.key))

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
        raise TypeError("Delayed objects are immutable")

    def __setitem__(self, index, val):
        raise TypeError("Delayed objects are immutable")

    def __iter__(self):
        raise TypeError("Delayed objects are not iterable")

    def __call__(self, *args, **kwargs):
        pure = kwargs.pop('pure', False)
        return delayed(apply, pure=pure)(self, args, kwargs)

    def __bool__(self):
        raise TypeError("Truth of Delayed objects is not supported")

    __nonzero__ = __bool__

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        method = delayed(right(op) if inv else op, pure=True)
        return lambda *args, **kwargs: method(*args, **kwargs)

    _get_unary_operator = _get_binary_operator


class DelayedLeaf(Delayed):
    def __init__(self, obj, name=None, pure=False):
        if name is None:
            try:
                name = obj.__name__ + tokenize(obj, pure=pure)
            except AttributeError:
                name = type(obj).__name__ + tokenize(obj, pure=pure)
        object.__setattr__(self, '_dasks', [{name: obj}])
        object.__setattr__(self, 'pure', pure)

    def __setstate__(self, state):
        self.__init__(*state)
        return self

    def __getstate__(self):
        return (self._data, self._key, self.pure)

    @property
    def _key(self):
        return first(self._dasks[0])

    @property
    def _data(self):
        return first(self._dasks[0].values())

    def __call__(self, *args, **kwargs):
        dask_key_name = kwargs.pop('dask_key_name', None)
        pure = kwargs.pop('pure', self.pure)

        if dask_key_name is None:
            name = (funcname(self._data) + '-' +
                    tokenize(self._key, *args, pure=pure, **kwargs))
        else:
            name = dask_key_name

        args, dasks = unzip(map(to_task_dasks, args), 2)
        if kwargs:
            dask_kwargs, dasks2 = to_task_dasks(kwargs)
            dasks = dasks + (dasks2,)
            task = (apply, self._data, list(args), dask_kwargs)
        else:
            task = (self._data,) + args

        dasks = flat_unique(dasks)
        dasks.append({name: task})
        return Delayed(name, dasks)


for op in [operator.abs, operator.neg, operator.pos, operator.invert,
           operator.add, operator.sub, operator.mul, operator.floordiv,
           operator.truediv, operator.mod, operator.pow, operator.and_,
           operator.or_, operator.xor, operator.lshift, operator.rshift,
           operator.eq, operator.ge, operator.gt, operator.ne, operator.le,
           operator.lt, operator.getitem]:
    Delayed._bind_operator(op)


base.normalize_token.register(Delayed, lambda a: a.key)

Value = Delayed


def value(val, name=None):
    warn("``dask.imperative.value`` is renamed to ``delayed``")
    return delayed(val, name=name)

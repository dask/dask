from __future__ import absolute_import, division, print_function

from collections import Iterator
from itertools import chain
import operator
import uuid

from toolz import merge, unique, curry, first

from .utils import concrete, funcname, methodcaller
from . import base
from .compatibility import apply
from . import threaded

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

    >>> a = delayed(1, 'a')
    >>> b = delayed(2, 'b')
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
        name = 'finalize-' + tokenize(expr, pure=True)
        keys = expr._keys()
        dsk = expr._optimize(expr.dask, keys)
        dsk[name] = (expr._finalize, (concrete, keys))
        return name, [dsk]
    if isinstance(expr, tuple) and type(expr) != tuple:
        return expr, []
    if isinstance(expr, (Iterator, list, tuple, set)):
        args, dasks = unzip((to_task_dasks(e) for e in expr), 2)
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
        return base.tokenize(*args, **kwargs)
    else:
        return str(uuid.uuid4())


@curry
def delayed(obj, name=None, pure=False, nout=None):
    """Wraps a function or object to produce a ``Delayed``.

    ``Delayed`` objects act as proxies for the object they wrap, but all
    operations on them are done lazily by building up a dask graph internally.

    Parameters
    ----------
    obj : object
        The function or object to wrap
    name : string or hashable, optional
        The key to use in the underlying graph for the wrapped object. Defaults
        to hashing content.
    pure : bool, optional
        Indicates whether calling the resulting ``Delayed`` object is a pure
        operation. If True, arguments to the call are hashed to produce
        deterministic keys. Default is False.
    nout : int, optional
        The number of outputs returned from calling the resulting ``Delayed``
        object. If provided, the ``Delayed`` output of the call can be iterated
        into ``nout`` objects, allowing for unpacking of results. By default
        iteration over ``Delayed`` objects will error.

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

    The key name of the result of calling a delayed object is determined by
    hashing the arguments by default. To explicitly set the name, you can use
    the ``dask_key_name`` keyword when calling the function:

    >>> add(1, 2)    # doctest: +SKIP
    Delayed('add-3dce7c56edd1ac2614add714086e950f')
    >>> add(1, 2, dask_key_name='three')
    Delayed('three')

    Note that objects with the same key name are assumed to have the same
    result. If you set the names explicitly you should make sure your key names
    are different for different results.

    >>> add(1, 2, dask_key_name='three')
    >>> add(2, 1, dask_key_name='three')
    >>> add(2, 2, dask_key_name='four')

    ``delayed`` can also be applied to objects to make operations on them lazy:

    >>> a = delayed([1, 2, 3])
    >>> isinstance(a, Delayed)
    True
    >>> a.compute()
    [1, 2, 3]

    The key name of a delayed object is hashed by default if ``pure=True`` or
    is generated randomly if ``pure=False`` (default).  To explicitly set the
    name, you can use the ``name`` keyword:

    >>> a = delayed([1, 2, 3], name='mylist')
    >>> a
    Delayed('mylist')

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

    As with function calls, method calls also support the ``dask_key_name``
    keyword:

    >>> a.count(2, dask_key_name="count_2")
    Delayed("count_2")

    """
    if isinstance(obj, Delayed):
        return obj

    task, dasks = to_task_dasks(obj)

    if not dasks:
        if not (nout is None or (type(nout) is int and nout >= 0)):
            raise ValueError("nout must be None or a positive integer,"
                             " got %s" % nout)
        if not name:
            try:
                prefix = obj.__name__
            except AttributeError:
                prefix = type(obj).__name__
            token = tokenize(obj, nout, pure=pure)
            name = '%s-%s' % (prefix, token)
        return DelayedLeaf(obj, name, pure=pure, nout=nout)
    else:
        if not name:
            name = '%s-%s' % (type(obj).__name__, tokenize(task, pure=pure))
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
    >>> a = delayed(1)
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
    __slots__ = ('_key', '_dasks', '_length')
    _finalize = staticmethod(first)
    _default_get = staticmethod(threaded.get)
    _optimize = staticmethod(lambda d, k, **kwds: d)

    def __init__(self, key, dasks, length=None):
        self._key = key
        self._dasks = dasks
        self._length = length

    def __getstate__(self):
        return tuple(getattr(self, i) for i in self.__slots__)

    def __setstate__(self, state):
        for k, v in zip(self.__slots__, state):
            setattr(self, k, v)

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
        if attr.startswith('_'):
            raise AttributeError("Attribute {0} not found".format(attr))
        return DelayedAttr(self, attr, 'getattr-%s' % tokenize(self, attr))

    def __setattr__(self, attr, val):
        if attr in self.__slots__:
            object.__setattr__(self, attr, val)
        else:
            raise TypeError("Delayed objects are immutable")

    def __setitem__(self, index, val):
        raise TypeError("Delayed objects are immutable")

    def __iter__(self):
        if getattr(self, '_length', None) is None:
            raise TypeError("Delayed objects of unspecified length are "
                            "not iterable")
        for i in range(self._length):
            yield self[i]

    def __len__(self):
        if getattr(self, '_length', None) is None:
            raise TypeError("Delayed objects of unspecified length have "
                            "no len()")
        return self._length

    def __call__(self, *args, **kwargs):
        pure = kwargs.pop('pure', False)
        name = kwargs.pop('dask_key_name', None)
        func = delayed(apply, pure=pure)
        if name is not None:
            return func(self, args, kwargs, dask_key_name=name)
        return func(self, args, kwargs)

    def __bool__(self):
        raise TypeError("Truth of Delayed objects is not supported")

    __nonzero__ = __bool__

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        method = delayed(right(op) if inv else op, pure=True)
        return lambda *args, **kwargs: method(*args, **kwargs)

    _get_unary_operator = _get_binary_operator


def call_function(func, args, kwargs, pure=False, nout=None):
    dask_key_name = kwargs.pop('dask_key_name', None)
    pure = kwargs.pop('pure', pure)

    if dask_key_name is None:
        name = '%s-%s' % (funcname(func), tokenize(func, *args,
                                                   pure=pure, **kwargs))
    else:
        name = dask_key_name

    args, dasks = unzip(map(to_task_dasks, args), 2)
    if kwargs:
        dask_kwargs, dasks2 = to_task_dasks(kwargs)
        dasks = dasks + (dasks2,)
        task = (apply, func, list(args), dask_kwargs)
    else:
        task = (func,) + args

    dasks = flat_unique(dasks)
    dasks.append({name: task})
    nout = nout if nout and nout > 1 else None
    return Delayed(name, dasks, length=nout)


class DelayedLeaf(Delayed):
    __slots__ = ('_obj', '_key', '_pure', '_nout')

    def __init__(self, obj, key, pure=False, nout=None):
        self._obj = obj
        self._key = key
        self._pure = pure
        self._nout = nout

    @property
    def dask(self):
        return {self._key: self._obj}

    @property
    def _dasks(self):
        return [self.dask]

    def __call__(self, *args, **kwargs):
        return call_function(self._obj, args, kwargs,
                             pure=self._pure, nout=self._nout)


class DelayedAttr(Delayed):
    __slots__ = ('_obj', '_attr', '_key')

    def __init__(self, obj, attr, key):
        self._obj = obj
        self._attr = attr
        self._key = key

    @property
    def _dasks(self):
        return [{self._key: (getattr, self._obj._key, self._attr)}] + self._obj._dasks

    def __call__(self, *args, **kwargs):
        return call_function(methodcaller(self._attr), (self._obj,) + args, kwargs)


for op in [operator.abs, operator.neg, operator.pos, operator.invert,
           operator.add, operator.sub, operator.mul, operator.floordiv,
           operator.truediv, operator.mod, operator.pow, operator.and_,
           operator.or_, operator.xor, operator.lshift, operator.rshift,
           operator.eq, operator.ge, operator.gt, operator.ne, operator.le,
           operator.lt, operator.getitem]:
    Delayed._bind_operator(op)


base.normalize_token.register(Delayed, lambda a: a.key)

from __future__ import absolute_import, division, print_function

from operator import add
from itertools import chain
from .compatibility import builtins

def inc(x):
    return x + 1

def ishashable(x):
    """ Is x hashable?

    Example
    -------

    >>> ishashable(1)
    True
    >>> ishashable([1])
    False
    """
    try:
        hash(x)
        return True
    except TypeError:
        return False


def istask(x):
    """ Is x a runnable task?

    A task is a tuple with a callable first argument

    Example
    -------

    >>> inc = lambda x: x + 1
    >>> istask((inc, 1))
    True
    >>> istask(1)
    False
    """
    return isinstance(x, tuple) and x and callable(x[0])


def get(d, key, get=None, concrete=True, **kwargs):
    """ Get value from Dask

    Example
    -------

    >>> inc = lambda x: x + 1
    >>> d = {'x': 1, 'y': (inc, 'x')}

    >>> get(d, 'x')
    1
    >>> get(d, 'y')
    2

    See Also
    --------
    set
    """
    get = get or _get
    if isinstance(key, list):
        v = (get(d, k, get=get, concrete=concrete) for k in key)
        if concrete:
            v = list(v)
    elif ishashable(key) and key in d:
        v = d[key]
    elif istask(key):
        v = key
    else:
        return key

    if istask(v):
        func, args = v[0], v[1:]
        args2 = [get(d, arg, get=get, concrete=False) for arg in args]
        return func(*[get(d, arg, get=get) for arg in args2])
    else:
        return v

_get = get


def set(d, key, val, args=[]):
    """ Set value for key in Dask

    Example
    -------

    >>> d = {}
    >>> set(d, 'x', 1)
    >>> d
    {'x': 1}

    >>> inc = lambda x: x + 1
    >>> set(d, 'y', inc, args=['x'])

    >>> get(d, 'y')
    2

    See Also
    --------
    get
    """
    assert key not in d
    if callable(val):
        val = (val,) + tuple(args)
    d[key] = val


def _deps(dsk, arg):
    """ Get dependencies from keys or tasks

    Helper function for get_dependencies.

    >>> dsk = {'x': 1, 'y': 2}

    >>> _deps(dsk, 'x')
    set(['x'])
    >>> _deps(dsk, (add, 'x', 1))
    set(['x'])

    >>> _deps(dsk, (add, 'x', (inc, 'y')))  # doctest: +SKIP
    set(['x', 'y'])
    """
    if istask(arg):
        return builtins.set.union(*[_deps(dsk, a) for a in arg[1:]])
    try:
        if arg not in dsk:
            return builtins.set()
    except TypeError:  # not hashable
        return builtins.set()
    return builtins.set([arg])


def get_dependencies(dsk, task):
    """ Get the immediate tasks on which this task depends

    >>> dsk = {'x': 1,
    ...        'y': (inc, 'x'),
    ...        'z': (add, 'x', 'y'),
    ...        'w': (inc, 'z'),
    ...        'a': (add, (inc, 'x'), 1)}

    >>> get_dependencies(dsk, 'x')
    set([])

    >>> get_dependencies(dsk, 'y')
    set(['x'])

    >>> get_dependencies(dsk, 'z')  # doctest: +SKIP
    set(['x', 'y'])

    >>> get_dependencies(dsk, 'w')  # Only direct dependencies
    set(['z'])

    >>> get_dependencies(dsk, 'a')  # Ignore non-keys
    set(['x'])
    """
    val = dsk[task]
    if not istask(val):
        return builtins.set([])
    return builtins.set.union(*[_deps(dsk, x) for x in flatten(val[1:])])


def flatten(seq):
    """

    >>> list(flatten([1]))
    [1]

    >>> list(flatten([[1, 2], [1, 2]]))
    [1, 2, 1, 2]

    >>> list(flatten([[[1], [2]], [[1], [2]]]))
    [1, 2, 1, 2]

    >>> list(flatten(((1, 2), (1, 2)))) # Don't flatten tuples
    [(1, 2), (1, 2)]
    """
    if not isinstance(next(iter(seq)), list):
        return seq
    else:
        return chain.from_iterable(map(flatten, seq))

def reverse_dict(d):
    """

    >>> a, b, c = 'abc'
    >>> d = {a: [b, c], b: [c]}
    >>> reverse_dict(d)  # doctest: +SKIP
    {'a': set([]), 'b': set(['a']}, 'c': set(['a', 'b'])}
    """
    terms = list(d.keys()) + list(chain.from_iterable(d.values()))
    result = dict((t, builtins.set()) for t in terms)
    for k, vals in d.items():
        for val in vals:
            result[val].add(k)
    return result


def cull(dsk, keys):
    """ Return new dask with only the tasks required to calculate keys.

    In other words, remove unnecessary tasks from dask.
    ``keys`` may be a single key or list of keys.

    Example
    -------

    >>> d = {'x': 1, 'y': (inc, 'x'), 'out': (add, 'x', 10)}
    >>> cull(d, 'out')  # doctest: +SKIP
    {'x': 1, 'out': (add, 'x', 10)}
    """
    if not isinstance(keys, list):
        keys = [keys]
    nxt = builtins.set(keys)
    seen = nxt
    while nxt:
        cur = nxt
        nxt = builtins.set()
        for item in cur:
            for dep in get_dependencies(dsk, item):
                if dep not in seen:
                    nxt.add(dep)
        seen.update(nxt)
    return dict((k, v) for k, v in dsk.items() if k in seen)

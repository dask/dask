from __future__ import absolute_import, division, print_function

from operator import add
from itertools import chain

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
        return set.union(*[_deps(dsk, a) for a in arg[1:]])
    try:
        if arg not in dsk:
            return set()
    except TypeError:  # not hashable
        return set()
    return set([arg])


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
    args = [dsk[task]]
    result = set()
    while args:
        arg = args.pop()
        if istask(arg):
            args.extend(arg[1:])
        elif isinstance(arg, list):
            args.extend(arg)
        else:
            try:
                result.add(arg)
            except TypeError:
                pass
    if not result:
        return set()
    return set.union(*[_deps(dsk, x) for x in result])


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

    >>> list(flatten((1, 2, [3, 4]))) # support heterogeneous
    [1, 2, 3, 4]
    """
    for item in seq:
        if isinstance(item, list):
            for item2 in flatten(item):
                yield item2
        else:
            yield item


def reverse_dict(d):
    """

    >>> a, b, c = 'abc'
    >>> d = {a: [b, c], b: [c]}
    >>> reverse_dict(d)  # doctest: +SKIP
    {'a': set([]), 'b': set(['a']}, 'c': set(['a', 'b'])}
    """
    terms = list(d.keys()) + list(chain.from_iterable(d.values()))
    result = dict((t, set()) for t in terms)
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
    nxt = set(keys)
    seen = nxt
    while nxt:
        cur = nxt
        nxt = set()
        for item in cur:
            for dep in get_dependencies(dsk, item):
                if dep not in seen:
                    nxt.add(dep)
        seen.update(nxt)
    return dict((k, v) for k, v in dsk.items() if k in seen)


def subs(task, key, val):
    newargs = []
    for arg in task[1:]:
        if istask(arg):
            arg = subs(arg, key, val)
        elif isinstance(arg, list):
            arg = [subs(x, key, val) for x in arg]
        elif arg == key:
            arg = val
        newargs.append(arg)
    return task[:1] + tuple(newargs)


def fuse(dsk):
    # locate all members of linear chains
    parents = {}
    deadbeats = set()
    for parent in dsk:
        deps = get_dependencies(dsk, parent)
        for child in deps:
            if child in parents:
                del parents[child]
                deadbeats.add(child)
            elif len(deps) > 1:
                deadbeats.add(child)
            elif child not in deadbeats:
                parents[child] = parent

    # construct the chains from ancestor to descendant
    chains = []
    children = dict(map(reversed, parents.items()))
    while parents:
        ch, pa = parents.popitem()
        chain = [ch, pa]
        while pa in parents:
            pa = parents.pop(pa)
            chain.append(pa)
        chain.reverse()
        while ch in children:
            ch = children.pop(ch)
            del parents[ch]
            chain.append(ch)
        chains.append(chain)

    # create a new dask with fused chains
    rv = {}
    fused = set()
    for chain in chains:
        ch = chain.pop()
        val = dsk[ch]
        while chain:
            pa = chain.pop()
            val = subs(dsk[pa], ch, val)
            fused.add(ch)
            ch = pa
        fused.add(ch)
        rv[ch] = val

    for k, v in dsk.items():
        if k not in fused:
            rv[k] = v
    return rv


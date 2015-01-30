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


def isdag(d, keys):
    """ Does Dask form a directed acyclic graph when calculating keys?

    ``keys`` may be a single key or list of keys.

    Example
    -------

    >>> inc = lambda x: x + 1
    >>> isdag({'x': 0, 'y': (inc, 'x')}, 'y')
    True
    >>> isdag({'x': (inc, 'y'), 'y': (inc, 'x')}, 'y')
    False
    """
    # Stack-based depth-first search traversal.  This is based on Tarjan's
    # method for topological sorting (see wikipedia for pseudocode)
    if not isinstance(keys, list):
        keys = [keys]

    # Nodes whose descendents have been completely explored.
    # These nodes are guaranteed to not be part of a cycle.
    completed = builtins.set()

    # All nodes that have been visited in the current traversal.  Because
    # we are doing depth-first search, going "deeper" should never result
    # in visiting a node that has already been seen.  The `seen` and
    # `completed` sets are mutually exclusive; it is okay to visit a node
    # that has already been added to `completed`.
    seen = builtins.set()

    for key in keys:
        if key in completed:
            continue
        nodes = [key]
        while nodes:
            # Keep current node on the stack until all descendants are visited
            cur = nodes[-1]
            if cur in completed:
                # Already fully traversed descendants of cur
                nodes.pop()
                continue
            seen.add(cur)

            # Add direct descendants of cur to nodes stack
            next_nodes = []
            for nxt in get_dependencies(d, cur):
                if nxt not in completed:
                    if nxt in seen:
                        # Cycle detected!
                        return False
                    next_nodes.append(nxt)

            if next_nodes:
                nodes.extend(next_nodes)
            else:
                # cur has no more descendants to explore, so we're done with it
                completed.add(cur)
                seen.remove(cur)
                nodes.pop()
    return True


def _get_task(d, task, maxdepth=1000):
    # non-recursive.  DAG property is checked upon reaching maxdepth.
    depth = 0
    _iter = lambda *args: iter(args)

    # We construct a nested heirarchy of tuples to mimic the execution stack
    # of frames that Python would maintain for a recursive implementation.
    # A frame is associated with a single task from a Dask.
    # A frame tuple has four elements:
    #    1) The function for the task.
    #    2) The arguments for the task (typically keys in the Dask).
    #       Arguments are stored in reverse order, and elements are popped
    #       as they are evaluated.
    #    3) The calculated results of the arguments from (2).
    #    4) The previous frame.
    frame = (task[0], list(task[:0:-1]), [], None)
    while True:
        func, args, results, prev = frame
        if not args:
            val = func(*results)
            if prev is None:
                return val
            prev[2].append(val)
            frame = prev
            depth -= 1
            continue

        key = args.pop()
        if isinstance(key, list):
            # v = (get(d, k, concrete=False) for k in key)  # recursive
            # Fake being lazy
            frame = (_iter, key[::-1], [], frame)
            depth += 1
            if maxdepth and depth > maxdepth:
                if not isdag(d, list(task[1:])):
                    raise ValueError('Cycle detected in dask!')
                maxdepth = None
            continue
        elif ishashable(key) and key in d:
            v = d[key]
        else:
            v = key

        if istask(v):
            frame = (v[0], list(v[:0:-1]), [], frame)
            depth += 1
            if maxdepth and depth > maxdepth:
                if not isdag(d, list(task[1:])):
                    raise ValueError('Cycle detected in dask!')
                maxdepth = None
        else:
            results.append(v)


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
        if get is _get:
            # use non-recursive method by default
            return _get_task(d, v)
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
    children = flatten(val[1:])
    if not children:
        return builtins.set()
    else:
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
    try:
        if not isinstance(next(iter(seq)), list):
            return seq
        else:
            return chain.from_iterable(map(flatten, seq))
    except StopIteration:
        return []

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

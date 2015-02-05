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


def _get_task(d, task, maxdepth=1000):
    # non-recursive.  DAG property is checked upon reaching maxdepth.
    _iter = lambda *args: iter(args)

    # We construct a nested heirarchy of tuples to mimic the execution stack
    # of frames that Python would maintain for a recursive implementation.
    # A frame is associated with a single task from a Dask.
    # A frame tuple has three elements:
    #    1) The function for the task.
    #    2) The arguments for the task (typically keys in the Dask).
    #       Arguments are stored in reverse order, and elements are popped
    #       as they are evaluated.
    #    3) The calculated results of the arguments from (2).
    stack = [(task[0], list(task[:0:-1]), [])]
    while True:
        func, args, results = stack[-1]
        if not args:
            val = func(*results)
            if len(stack) == 1:
                return val
            stack.pop()
            stack[-1][2].append(val)
            continue
        elif maxdepth and len(stack) > maxdepth:
            cycle = getcycle(d, list(task[1:]))
            if cycle:
                cycle = '->'.join(cycle)
                raise RuntimeError('Cycle detected in Dask: %s' % cycle)
            maxdepth = None

        key = args.pop()
        if isinstance(key, list):
            # v = (get(d, k, concrete=False) for k in key)  # recursive
            # Fake being lazy
            stack.append((_iter, key[::-1], []))
            continue
        elif ishashable(key) and key in d:
            v = d[key]
        else:
            v = key

        if istask(v):
            stack.append((v[0], list(v[:0:-1]), []))
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


def _deps(dsk, arg):
    """ Get dependencies from keys or tasks

    Helper function for get_dependencies.

    >>> dsk = {'x': 1, 'y': 2}

    >>> _deps(dsk, 'x')
    ['x']
    >>> _deps(dsk, (add, 'x', 1))
    ['x']

    >>> _deps(dsk, (add, 'x', (inc, 'y')))  # doctest: +SKIP
    ['x', 'y']
    """
    if istask(arg):
        result = []
        for a in arg[1:]:
            result.extend(_deps(dsk, a))
        return result
    try:
        if arg not in dsk:
            return []
    except TypeError:  # not hashable
            return []
    return [arg]


def get_dependencies(dsk, task, as_list=False):
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
    result = []
    while args:
        arg = args.pop()
        if istask(arg):
            args.extend(arg[1:])
        elif isinstance(arg, list):
            args.extend(arg)
        else:
            try:
                result.append(arg)
            except TypeError:
                pass
    if not result:
        return [] if as_list else set()
    rv = []
    for x in result:
        rv.extend(_deps(dsk, x))
    return rv if as_list else set(rv)


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


def subs(task, key, val):
    """ Perform a substitution on a task

    Example
    -------

    >>> subs((inc, 'x'), 'x', 1)  # doctest: +SKIP
    (inc, 1)
    """
    if not istask(task):
        if task == key:
            return val
        elif isinstance(task, list):
            return [subs(x, key, val) for x in task]
        else:
            return task
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


def _toposort(dsk, keys=None, returncycle=False):
    # Stack-based depth-first search traversal.  This is based on Tarjan's
    # method for topological sorting (see wikipedia for pseudocode)
    if keys is None:
        keys = dsk
    elif not isinstance(keys, list):
        keys = [keys]
    if not returncycle:
        ordered = []

    # Nodes whose descendents have been completely explored.
    # These nodes are guaranteed to not be part of a cycle.
    completed = set()

    # All nodes that have been visited in the current traversal.  Because
    # we are doing depth-first search, going "deeper" should never result
    # in visiting a node that has already been seen.  The `seen` and
    # `completed` sets are mutually exclusive; it is okay to visit a node
    # that has already been added to `completed`.
    seen = set()

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
            for nxt in get_dependencies(dsk, cur):
                if nxt not in completed:
                    if nxt in seen:
                        # Cycle detected!
                        cycle = [nxt]
                        while nodes[-1] != nxt:
                            cycle.append(nodes.pop())
                        cycle.append(nodes.pop())
                        cycle.reverse()
                        if returncycle:
                            return cycle
                        else:
                            cycle = '->'.join(cycle)
                            raise RuntimeError('Cycle detected in Dask: %s' % cycle)
                    next_nodes.append(nxt)

            if next_nodes:
                nodes.extend(next_nodes)
            else:
                # cur has no more descendants to explore, so we're done with it
                if not returncycle:
                    ordered.append(cur)
                completed.add(cur)
                seen.remove(cur)
                nodes.pop()
    if returncycle:
        return []
    return ordered


def toposort(dsk):
    """ Return a list of keys of dask sorted in topological order."""
    return _toposort(dsk)


def getcycle(d, keys):
    """ Return a list of nodes that form a cycle if Dask is not a DAG.

    Returns an empty list if no cycle is found.

    ``keys`` may be a single key or list of keys.

    Example
    -------

    >>> d = {'x': (inc, 'z'), 'y': (inc, 'x'), 'z': (inc, 'y')}
    >>> getcycle(d, 'x')
    ['x', 'z', 'y', 'x']

    See Also
    --------
    isdag
    """
    return _toposort(d, keys=keys, returncycle=True)


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

    See Also
    --------
    getcycle
    """
    return not getcycle(d, keys)


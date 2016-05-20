from __future__ import absolute_import, division, print_function

from operator import add

from itertools import chain

def inc(x):
    return x + 1

def ishashable(x):
    """ Is x hashable?

    Examples
    --------

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

    Examples
    --------

    >>> inc = lambda x: x + 1
    >>> istask((inc, 1))
    True
    >>> istask(1)
    False
    """
    return type(x) is tuple and x and callable(x[0])


def preorder_traversal(task):
    """A generator to preorder-traverse a task."""

    for item in task:
        if istask(item):
            for i in preorder_traversal(item):
                yield i
        elif isinstance(item, list):
            yield list
            for i in preorder_traversal(item):
                yield i
        else:
            yield item


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


def get(d, key, get=None, **kwargs):
    """ Get value from Dask

    Examples
    --------

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
        v = tuple(get(d, k, get=get) for k in key)
    elif istask(key):
        v = key
    elif ishashable(key):
        v = d[key]
    else:
        message = '%s is neither a task or a dask key'
        raise KeyError(message % key)

    if istask(v):
        if get is _get:
            # use non-recursive method by default
            return _get_task(d, v)
        func, args = v[0], v[1:]

        args2 = []
        for arg in args:
            if not istask(arg) and arg not in d:
                args2.append(arg)
            else:
                args2.append(get(d, arg, get=get))
        return func(*args2)
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
    >>> _deps(dsk, ['x', 'y'])
    ['x', 'y']
    >>> _deps(dsk, {'a': 'x'})
    ['x']
    >>> _deps(dsk, (add, 'x', (inc, 'y')))  # doctest: +SKIP
    ['x', 'y']
    """
    if istask(arg):
        result = []
        for a in arg[1:]:
            result.extend(_deps(dsk, a))
        return result
    if type(arg) is list:
        return sum([_deps(dsk, a) for a in arg], [])
    if type(arg) is dict:
        return sum([_deps(dsk, v) for v in arg.values()], [])
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
        elif type(arg) is list:
            args.extend(arg)
        else:
            result.append(arg)
    if not result:
        return [] if as_list else set()
    rv = []
    for x in result:
        rv.extend(_deps(dsk, x))
    return rv if as_list else set(rv)


def get_deps(dsk):
    """ Get dependencies and dependents from dask dask graph

    >>> dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> dependencies
    {'a': set([]), 'c': set(['b']), 'b': set(['a'])}
    >>> dependents
    {'a': set(['b']), 'c': set([]), 'b': set(['c'])}
    """
    dependencies = dict((k, get_dependencies(dsk, k)) for k in dsk)
    dependents = reverse_dict(dependencies)
    return dependencies, dependents


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
    if isinstance(seq, str):
        yield seq
    else:
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

    Examples
    --------

    >>> subs((inc, 'x'), 'x', 1)  # doctest: +SKIP
    (inc, 1)
    """
    if not istask(task):
        try:
            if task == key:
                return val
        except Exception:
            pass
        if isinstance(task, list):
            return [subs(x, key, val) for x in task]
        return task
    newargs = []
    for arg in task[1:]:
        if istask(arg):
            arg = subs(arg, key, val)
        elif isinstance(arg, list):
            arg = [subs(x, key, val) for x in arg]
        elif type(arg) is type(key) and arg == key:
            arg = val
        newargs.append(arg)
    return task[:1] + tuple(newargs)


def _toposort(dsk, keys=None, returncycle=False, dependencies=None):
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

    if dependencies is None:
        dependencies = dict((k, get_dependencies(dsk, k)) for k in dsk)

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
            for nxt in dependencies[cur]:
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


def toposort(dsk, dependencies=None):
    """ Return a list of keys of dask sorted in topological order."""
    return _toposort(dsk, dependencies=dependencies)


def getcycle(d, keys):
    """ Return a list of nodes that form a cycle if Dask is not a DAG.

    Returns an empty list if no cycle is found.

    ``keys`` may be a single key or list of keys.

    Examples
    --------

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

    Examples
    --------

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


def list2(L):
    return list(L)


def quote(x):
    """ Ensure that this value remains this value in a dask graph

    Some values in dask graph take on special meaning.  Lists become iterators,
    tasks get executed.  Sometimes we want to ensure that our data is not
    interpreted but remains literal.

    >>> quote([1, 2, 3])
    [1, 2, 3]

    >>> quote((add, 1, 2))  # doctest: +SKIP
    (tuple, [add, 1, 2])
    """
    if istask(x):
        return (tuple, list(map(quote, x)))
    return x

""" Static order of nodes in dask graph

We can make decisions on what tasks to run next both

*  Dynamically at runtime
*  Statically before runtime

Dask's async scheduler runs dynamically and prefers to run tasks that were just
made available.  However when several tasks become available at the same time
we have an opportunity to break ties in an intelligent way

        d
        |
    b   c
     \ /
      a

E.g. when we run ``a`` we can choose to run either ``b`` or ``c`` next.  In
this case we may choose to start with ``c``, because it has other dependencies.

This is particularly important at the beginning of the computation when we
often dump hundreds of leaf nodes onto the scheduler at once.  The order in
which we start this computation can significantly change performance.


Breaking Ties
-------------

And so we create a total ordering over all nodes to serve as a tie breaker.  We
represent this ordering with a dictionary.  Lower scores have higher priority.

    {'d': 0,
     'c': 1,
     'a': 2,
     'b': 3}

There are several ways in which we might order our keys.  In practice we have
found the following objectives important:

1.  **Finish subtrees before starting new subtrees:** Often our computation
    consists of many independent subtrees (e.g. reductions in an array).  We
    want to work on and finish individual subtrees before moving on to others
    in order to keep a low memory footprint.
2.  **Run heavily depended-on tasks first**: Some tasks produce data that is
    required by many other tasks, either in a deep linear chain (critical path)
    or in a shallow but broad nexus (critical point).  By preferring these we
    allow other computations to flow to completion more easily.


Approach: Depth First Search with Intelligent Tie-Breaking
----------------------------------------------------------

To satisfy concern (1) we perform a depth first search (``dfs``).  To satisfy
concern (2) we prefer to traverse down children in the order of which child has
the descendent on whose result the most tasks depend.
"""
from __future__ import absolute_import, division, print_function

from .core import get_dependencies, reverse_dict, get_deps  # noqa: F401
from .utils_test import add, inc  # noqa: F401


def order(dsk, dependencies=None):
    """ Order nodes in dask graph

    The ordering will be a toposort but will also have other convenient
    properties

    1.  Depth first search
    2.  DFS prefers nodes that enable the most data

    >>> dsk = {'a': 1, 'b': 2, 'c': (inc, 'a'), 'd': (add, 'b', 'c')}
    >>> order(dsk)
    {'a': 2, 'c': 1, 'b': 3, 'd': 0}
    """
    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k) for k in dsk}
    dependents = reverse_dict(dependencies)

    ndeps = ndependents(dependencies, dependents)
    maxes = child_max(dependencies, dependents, ndeps)

    def key(x):
        return -maxes.get(x, 0), str(x)

    return dfs(dependencies, dependents, key=key)


def ndependents(dependencies, dependents):
    """ Number of total data elements that depend on key

    For each key we return the number of data that can only be run after this
    key is run.  The root nodes have value 1 while deep child nodes will have
    larger values.

    Examples
    --------

    >>> dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dependencies, dependents = get_deps(dsk)

    >>> sorted(ndependents(dependencies, dependents).items())
    [('a', 3), ('b', 2), ('c', 1)]
    """
    result = dict()
    num_needed = dict((k, len(v)) for k, v in dependents.items())
    current = set(k for k, v in num_needed.items() if v == 0)
    while current:
        key = current.pop()
        result[key] = 1 + sum(result[parent] for parent in dependents[key])
        for child in dependencies[key]:
            num_needed[child] -= 1
            if num_needed[child] == 0:
                current.add(child)
    return result


def child_max(dependencies, dependents, scores):
    """ Maximum-ish of scores of children

    This takes a dictionary of scores per key and returns a new set of scores
    per key that is the maximum of the scores of all children of that node plus
    its own score.  In some sense this ranks each node by the maximum
    importance of their children plus their own value.

    This is generally fed the result from ``ndependents``

    Examples
    --------

    >>> dsk = {'a': 1, 'b': 2, 'c': (inc, 'a'), 'd': (add, 'b', 'c')}
    >>> scores = {'a': 3, 'b': 2, 'c': 2, 'd': 1}
    >>> dependencies, dependents = get_deps(dsk)

    >>> sorted(child_max(dependencies, dependents, scores).items())
    [('a', 3), ('b', 2), ('c', 5), ('d', 6)]
    """
    result = dict()
    num_needed = dict((k, len(v)) for k, v in dependencies.items())
    current = set(k for k, v in num_needed.items() if v == 0)
    while current:
        key = current.pop()
        score = scores[key]
        children = dependencies[key]
        if children:
            score += max(result[child] for child in children)
        result[key] = score
        for parent in dependents[key]:
            num_needed[parent] -= 1
            if num_needed[parent] == 0:
                current.add(parent)
    return result


def dfs(dependencies, dependents, key=lambda x: x):
    """ Depth First Search of dask graph

    This traverses from root/output nodes down to leaf/input nodes in a depth
    first manner.  At each node it traverses down its immediate children by the
    order determined by maximizing the key function.

    As inputs it takes dependencies and dependents as can be computed from
    ``get_deps(dsk)``.

    Examples
    --------

    >>> dsk = {'a': 1, 'b': 2, 'c': (inc, 'a'), 'd': (add, 'b', 'c')}
    >>> dependencies, dependents = get_deps(dsk)

    >>> sorted(dfs(dependencies, dependents).items())
    [('a', 3), ('b', 1), ('c', 2), ('d', 0)]
    """
    result = dict()
    i = 0

    roots = [k for k, v in dependents.items() if not v]
    stack = sorted(roots, key=key, reverse=True)
    seen = set()

    while stack:
        item = stack.pop()
        if item in seen:
            continue
        seen.add(item)

        result[item] = i
        deps = dependencies[item]
        if deps:
            deps = deps - seen
            deps = sorted(deps, key=key, reverse=True)
            stack.extend(deps)
        i += 1

    return result

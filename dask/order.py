""" Static order of nodes in dask graph

Dask makes decisions on what tasks to prioritize both

*  Dynamically at runtime
*  Statically before runtime

Dynamically we prefer to run tasks that were just made available.  However when
several tasks become available at the same time we have an opportunity to break
ties in an intelligent way

        d
        |
    b   c
     \ /
      a

For example after we finish ``a`` we can choose to run either ``b`` or ``c``
next.  Making small decisions like this can greatly affect our performance,
especially because the order in which we run tasks affects the order in which
we can release memory, which operationally we find to have a large affect on
many computation.  We want to run tasks in such a way that we keep only a small
amount of data in memory at any given time.


Static Ordering
---------------

And so we create a total ordering over all nodes to serve as a tie breaker.  We
represent this ordering with a dictionary mapping keys to integer values.
Lower scores have higher priority.  These scores correspond to the order in
which a sequential scheduler would visit each node.

    {'a': 0,
     'c': 1,
     'd': 2,
     'b': 3}

There are several ways in which we might order our keys.  This is a nuanced
process that has to take into account many different kinds of workflows, and
operate efficiently in linear time.  We strongly recommend that readers look at
the docstrings of tests in dask/tests/test_order.py.  These tests usually have
graph types laid out very carefully to show the kinds of situations that often
arise, and the order we would like to be determined.


Policy
------

Work towards *small goals* with *big steps*.

1.  **Small goals**: prefer tasks whose final dependents have few dependencies.

    We prefer to prioritize those tasks that help branches of computation that
    can terminate quickly.

    With more detail, we compute the total number of dependencies that each
    task depends on (both its own dependencies, and the dependencies of its
    dependencies, and so on), and then we choose those tasks that drive towards
    results with a low number of total dependencies.  We choose to prioritize
    tasks that work towards finishing shorter computations first.

2.  **Big steps**: prefer tasks with many dependents

    However, many tasks work towards the same final dependents.  Among those,
    we choose those tasks with the most work left to do.  We want to finish
    the larger portions of a sub-computation before we start on the smaller
    ones.

3.  **Name comparison**: break ties with key name

    Often graphs are made with regular keynames.  When no other structural
    difference exists between two keys, use the key name to break ties.
    This relies on the regularity of graph constructors like dask.array to be a
    good proxy for ordering.  This is usually a good idea and a sane default.
"""
from __future__ import absolute_import, division, print_function

from .core import get_dependencies, reverse_dict, get_deps  # noqa: F401
from .utils_test import add, inc  # noqa: F401


def order(dsk, dependencies=None):
    """ Order nodes in dask graph

    This produces an ordering over our tasks that we use to break ties when
    executing.  We do this ahead of time to reduce a bit of stress on the
    scheduler and also to assist in static analysis.

    This currently traverses the graph as a single-threaded scheduler would
    traverse it.  It breaks ties in the following ways:

    1.  Start from roots nodes that have the largest subgraphs
    2.  When a node has dependencies that are not yet computed prefer
        dependencies with large subtrees  (start hard things first)
    2.  When we reach a node that can be computed we then traverse up and
        prefer dependents that have small super-trees (few total dependents)
        (finish existing work quickly)

    Examples
    --------
    >>> dsk = {'a': 1, 'b': 2, 'c': (inc, 'a'), 'd': (add, 'b', 'c')}
    >>> order(dsk)
    {'a': 0, 'c': 1, 'b': 2, 'd': 3}
    """
    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k) for k in dsk}

    for k, deps in dependencies.items():
        deps.discard(k)

    dependents = reverse_dict(dependencies)

    total_dependencies = ndependencies(dependencies, dependents)
    total_dependents, min_dependencies = ndependents(dependencies, dependents, total_dependencies)

    waiting = {k: set(v) for k, v in dependencies.items()}

    def dependencies_key(x):
        return total_dependencies.get(x, 0), ReverseStrComparable(x)

    def dependents_key(x):
        return (min_dependencies[x],
                -total_dependents.get(x, 0),
                StrComparable(x))

    result = dict()
    seen = set()  # tasks that should not be added again to the stack
    i = 0

    stack = [k for k, v in dependents.items() if not v]
    if len(stack) < 10000:
        stack = sorted(stack, key=dependencies_key)
    else:
        stack = stack[::-1]

    while stack:
        item = stack.pop()

        if item in result:
            continue

        deps = waiting[item]

        if deps:
            stack.append(item)
            seen.add(item)
            if len(deps) < 1000:
                deps = sorted(deps, key=dependencies_key)
            stack.extend(deps)
            continue

        result[item] = i
        i += 1

        for dep in dependents[item]:
            waiting[dep].discard(item)

        deps = [d for d in dependents[item]
                if d not in result and not (d in seen and len(waiting[d]) > 1)]
        if len(deps) < 1000:
            deps = sorted(deps, key=dependents_key, reverse=True)

        stack.extend(deps)

    return result


def ndependents(dependencies, dependents, total_dependencies):
    """ Number of total data elements that depend on key

    For each key we return the number of keys that can only be run after this
    key is run.  The root nodes have value 1 while deep child nodes will have
    larger values.

    We also return the minimum value of the maximum number of dependencies of
    all final dependencies (see module-level comment for more)

    Examples
    --------
    >>> dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dependencies, dependents = get_deps(dsk)

    >>> total_dependencies = ndependencies(dependencies, dependents)
    >>> total_dependents, min_dependencies = ndependents(dependencies,
    ...                                                  dependents,
    ...                                                  total_dependencies)
    >>> sorted(total_dependents.items())
    [('a', 3), ('b', 2), ('c', 1)]

    Returns
    -------
    total_dependendents: Dict[key, int]
    min_dependencies: Dict[key, int]
    """
    result = dict()
    min_result = dict()
    num_needed = {k: len(v) for k, v in dependents.items()}
    current = {k for k, v in num_needed.items() if v == 0}
    while current:
        key = current.pop()
        result[key] = 1 + sum(result[parent] for parent in dependents[key])
        try:
            min_result[key] = min(min_result[parent] for parent in dependents[key])
        except ValueError:
            min_result[key] = total_dependencies[key]
        for child in dependencies[key]:
            num_needed[child] -= 1
            if num_needed[child] == 0:
                current.add(child)
    return result, min_result


def ndependencies(dependencies, dependents):
    """ Number of total data elements on which this key depends

    For each key we return the number of tasks that must be run for us to run
    this task.

    Examples
    --------
    >>> dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> sorted(ndependencies(dependencies, dependents).items())
    [('a', 1), ('b', 2), ('c', 3)]
    """
    result = dict()
    num_needed = {k: len(v) for k, v in dependencies.items()}
    current = {k for k, v in num_needed.items() if v == 0}
    while current:
        key = current.pop()
        result[key] = 1 + sum(result[child] for child in dependencies[key])
        for parent in dependents[key]:
            num_needed[parent] -= 1
            if num_needed[parent] == 0:
                current.add(parent)
    return result


class StrComparable(object):
    """ Wrap object so that it defaults to string comparison

    When comparing two objects of different types Python fails

    >>> 'a' < 1  # doctest: +SKIP
    Traceback (most recent call last):
        ...
    TypeError: '<' not supported between instances of 'str' and 'int'

    This class wraps the object so that, when this would occur it instead
    compares the string representation

    >>> StrComparable('a') < StrComparable(1)
    False
    """
    __slots__ = ('obj',)

    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
        try:
            return self.obj < other.obj
        except Exception:
            return str(self.obj) < str(other.obj)


class ReverseStrComparable(object):
    """ Wrap object so that it defaults to string comparison

    Used when sorting in reverse direction.  See StrComparable for normal
    documentation.
    """
    __slots__ = ('obj',)

    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
        try:
            return self.obj > other.obj
        except Exception:
            return str(self.obj) > str(other.obj)

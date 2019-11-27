r""" Static order of nodes in dask graph

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
from math import log
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
    num_needed, total_dependencies = ndependencies(dependencies, dependents)
    metrics = ndependents(dependencies, dependents, total_dependencies)

    def initial_stack_key(x):
        """ Choose which task to run at the very beginning

        First prioritize large, tall groups, then prioritize the same as `dependents_key`.
        """
        num_dependents = len(dependents[x])
        total_dependents, min_dependencies, max_dependencies, min_heights, max_heights = metrics[
            x
        ]
        return (
            # at a high-level, work towards a large goal (and prefer tall and narrow)
            -max_dependencies,
            num_dependents - max_heights,
            # tactically, finish small connected jobs first
            min_dependencies,
            num_dependents - min_heights,  # prefer tall and narrow
            -total_dependents,  # take a big step
            # try to be memory efficient
            num_dependents,
            # tie-breaker
            StrComparable(x),
        )

    def dependents_key(x):
        """ Choose a path from our starting task to our tactical goal

        This path is connected to a large goal, but focuses on completing a small goal.
        """
        num_dependents = len(dependents[x])
        total_dependents, min_dependencies, _, min_heights, _ = metrics[x]
        return (
            # tactically, finish small connected jobs first
            min_dependencies,
            num_dependents - min_heights,  # prefer tall and narrow
            -total_dependents,  # take a big step
            # try to be memory efficient
            num_dependents - len(dependencies[x]) + num_needed[x],
            num_dependents,
            # tie-breaker
            StrComparable(x),
        )

    def dependencies_key(x):
        """ Choose which dependency to run as part of a reverse DFS

        This is very similar to both `initial_stack_key` and `dependents_key`.
        """
        num_dependents = len(dependents[x])
        total_dependents, min_dependencies, max_dependencies, min_heights, max_heights = metrics[
            x
        ]
        return (
            # at a high-level, work towards a large goal (and prefer tall and narrow)
            -max_dependencies,
            num_dependents - max_heights,
            # tactically, finish small connected jobs first
            min_dependencies,
            num_dependents - min_heights,  # prefer tall and narrow
            -total_dependents,  # stay where the work is
            # try to be memory efficient
            num_dependents - len(dependencies[x]) + num_needed[x],
            num_dependents,
            # tie-breaker
            StrComparable(x),
        )

    result = {}
    i = 0

    init_stack = {k for k, v in dependencies.items() if not v}
    outer_stack = []
    outer_stack_seeds = []
    outer_stack_seeds_index = 0
    inner_stack = []

    # aliases for speed
    outer_stack_append = outer_stack.append
    outer_stack_pop = outer_stack.pop
    outer_stack_extend = outer_stack.extend
    outer_stack_seeds_append = outer_stack_seeds.append
    inner_stack_append = inner_stack.append
    inner_stack_pop = inner_stack.pop
    inner_stack_extend = inner_stack.extend
    inner_stack_reverse = inner_stack.reverse

    is_init_sorted = False
    item = min(init_stack, key=initial_stack_key)
    while True:
        outer_stack_append(item)
        while outer_stack:
            item = outer_stack_pop()
            if item not in result:
                # Pre-populate the inner stack with a path down to our tactical goal
                inner_stack_append(item)
                deps = dependents[item]
                while deps:
                    item = min(deps, key=dependents_key)
                    inner_stack_append(item)
                    deps = dependents[item]
                inner_stack_reverse()

                while inner_stack:
                    # Perform a DFS along dependencies
                    item = inner_stack_pop()
                    if item in result:
                        continue

                    if num_needed[item]:
                        inner_stack_append(item)
                        deps = dependencies[item].difference(result)
                        if len(deps) < 1000:
                            inner_stack_extend(
                                sorted(deps, key=dependencies_key, reverse=True)
                            )
                        else:
                            inner_stack_extend(deps)
                            inner_stack_append(
                                min(deps, key=dependencies_key)  # one good one
                            )
                        continue

                    result[item] = i
                    i += 1
                    for dep in dependents[item]:
                        num_needed[dep] -= 1

                    # Our next starting point will be "seeded" by a completed task in a FIFO manner.
                    # When our DFS with `inner_stack` is finished--which means we computed our tactical
                    # goal--we will choose our next starting point from the dependents of completed tasks.
                    # However, it is too expensive to consider all dependents of all completed tasks, so
                    # we consider the dependents of tasks in the order they complete.
                    #
                    # Instead of always using a FIFO policy with the seeds, a reasonable variant would be
                    # to use a LIFO policy of seeds collected until `inner_stack` is empty.
                    outer_stack_seeds_append(item)

            while not outer_stack and outer_stack_seeds_index < len(outer_stack_seeds):
                outer_stack_extend(
                    sorted(
                        dependents[
                            outer_stack_seeds[outer_stack_seeds_index]
                        ].difference(result),
                        key=dependents_key,
                        reverse=True,
                    )
                )
                outer_stack_seeds_index += 1

        if len(dependencies) == len(result):
            break  # all done!

        # We just finished computing a connected group.
        # Let's choose the first `item` in the next group to compute.
        # If we have few large groups left, then it's best to find `item` by taking a minimum.
        # If we have many small groups left, then it's best to sort.
        # If we have many tiny groups left, then it's best to simply iterate.
        if not is_init_sorted:
            prev_len = len(init_stack)
            init_stack = init_stack.difference(result)
            N = len(init_stack)
            m = prev_len - N
            # is `min` likely better than `sort`?
            if m >= N or N + (N - m) * log(N - m) < N * log(N):
                item = min(init_stack, key=initial_stack_key)
                continue

            if len(init_stack) < 10000:
                init_stack = sorted(init_stack, key=initial_stack_key, reverse=True)
            else:
                init_stack = list(init_stack)
            init_stack_pop = init_stack.pop
            is_init_sorted = True

        item = init_stack_pop()
        while item in result:
            item = init_stack_pop()

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

    >>> _, total_dependencies = ndependencies(dependencies, dependents)
    >>> total_dependents, _, _, _, _= ndependents(dependencies,
    ...                                           dependents,
    ...                                           total_dependencies)
    >>> sorted(total_dependents.items())
    [('a', 3), ('b', 2), ('c', 1)]

    Returns
    -------
    total_dependents: Dict[key, int]
    min_dependencies: Dict[key, int]
    max_dependencies: Dict[key, int]
    min_heights: Dict[key, int]
    max_heights: Dict[key, int]
    """
    result = {}
    num_needed = {k: len(v) for k, v in dependents.items()}
    current = {k for k, v in num_needed.items() if v == 0}
    current_pop = current.pop
    current_add = current.add
    while current:
        key = current_pop()
        parents = dependents[key]
        if parents:
            total_dependents, min_dependencies, max_dependencies, min_heights, max_heights = zip(
                *(result[parent] for parent in parents)
            )
            result[key] = (
                1 + sum(total_dependents),
                min(min_dependencies),
                max(max_dependencies),
                1 + min(min_heights),
                1 + max(max_heights),
            )
        else:
            val = total_dependencies[key]
            result[key] = (1, val, val, 1, 1)
        for child in dependencies[key]:
            num_needed[child] -= 1
            if num_needed[child] == 0:
                current_add(child)
    return result


def ndependencies(dependencies, dependents):
    """ Number of total data elements on which this key depends

    For each key we return the number of tasks that must be run for us to run
    this task.

    Examples
    --------
    >>> dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> num_dependencies, total_dependencies = ndependencies(dependencies, dependents).items())
    >>> sorted(total_dependencies.items())
    [('a', 1), ('b', 2), ('c', 3)]

    Returns
    -------
    num_dependencies: Dict[key, int]
    total_dependencies: Dict[key, int]
    """
    result = {}
    num_needed = {k: len(v) for k, v in dependencies.items()}
    num_dependencies = num_needed.copy()
    current = {k for k, v in num_needed.items() if v == 0}
    current_pop = current.pop
    current_add = current.add
    while current:
        key = current_pop()
        result[key] = 1 + sum(result[child] for child in dependencies[key])
        for parent in dependents[key]:
            num_needed[parent] -= 1
            if num_needed[parent] == 0:
                current_add(parent)
    return num_dependencies, result


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

    __slots__ = ("obj",)

    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
        try:
            return self.obj < other.obj
        except Exception:
            return str(self.obj) < str(other.obj)

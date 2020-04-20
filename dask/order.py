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
from .core import get_dependencies, reverse_dict, get_deps, getcycle  # noqa: F401
from .utils_test import add, inc  # noqa: F401


def order(dsk, dependencies=None):
    """ Order nodes in dask graph

    This produces an ordering over our tasks that we use to break ties when
    executing.  We do this ahead of time to reduce a bit of stress on the
    scheduler and also to assist in static analysis.

    This currently traverses the graph as a single-threaded scheduler would
    traverse it.  It breaks ties in the following ways:

    1.  Begin at a leaf node that is a dependency of a root node that has the
        largest subgraph (start hard things first)
    2.  Prefer tall branches with few dependents (start hard things first and
        try to avoid memory usage)
    3.  Prefer dependents that are dependencies of root nodes that have
        the smallest subgraph (do small goals that can terminate quickly)

    Examples
    --------
    >>> dsk = {'a': 1, 'b': 2, 'c': (inc, 'a'), 'd': (add, 'b', 'c')}
    >>> order(dsk)
    {'a': 0, 'c': 1, 'b': 2, 'd': 3}
    """
    if not dsk:
        return {}

    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k) for k in dsk}

    dependents = reverse_dict(dependencies)
    num_needed, total_dependencies = ndependencies(dependencies, dependents)
    metrics = graph_metrics(dependencies, dependents, total_dependencies)
    if len(metrics) != len(dsk):
        cycle = getcycle(dsk, None)
        raise RuntimeError(
            "Cycle detected between the following keys:\n  -> %s"
            % "\n  -> ".join(str(x) for x in cycle)
        )

    # Leaf nodes.  We choose one--the initial node--for each weakly connected subgraph.
    # Let's calculate the `initial_stack_key` as we determine `init_stack` set.
    init_stack = {
        # First prioritize large, tall groups, then prioritize the same as ``dependents_key``.
        key: (
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
            StrComparable(key),
        )
        for key, num_dependents, (
            total_dependents,
            min_dependencies,
            max_dependencies,
            min_heights,
            max_heights,
        ) in (
            (key, len(dependents[key]), metrics[key])
            for key, val in dependencies.items()
            if not val
        )
    }
    # `initial_stack_key` chooses which task to run at the very beginning.
    # This value is static, so we pre-compute as the value of this dict.
    initial_stack_key = init_stack.__getitem__

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
            total_dependencies[x],  # already found work, so don't add more
            # tie-breaker
            StrComparable(x),
        )

    def dependencies_key(x):
        """ Choose which dependency to run as part of a reverse DFS

        This is very similar to both ``initial_stack_key`` and ``dependents_key``.
        """
        num_dependents = len(dependents[x])
        (
            total_dependents,
            min_dependencies,
            max_dependencies,
            min_heights,
            max_heights,
        ) = metrics[x]
        # Prefer short and narrow instead of tall in narrow, because we're going in
        # reverse along dependencies.
        return (
            # at a high-level, work towards a large goal (and prefer short and narrow)
            -max_dependencies,
            num_dependents + max_heights,
            # tactically, finish small connected jobs first
            min_dependencies,
            num_dependents + min_heights,  # prefer short and narrow
            -total_dependencies[x],  # go where the work is
            # try to be memory efficient
            num_dependents - len(dependencies[x]) + num_needed[x],
            num_dependents,
            total_dependents,  # already found work, so don't add more
            # tie-breaker
            StrComparable(x),
        )

    result = {}
    i = 0

    # Nodes in the current weakly connected subgraph that are not part of the current
    # DFS along dependencies.  The next DFS will begin from these nodes.  `outer_stack`
    # is populated from `init_stack` and the dependents of `outer_stack_seeds`.
    outer_stack = []

    # Nodes get added to this as they complete.  We will populate `outer_stack` from
    # dependents of these nodes.  We consider nodes using a FIFO policy (yes, we could
    # have used collections.deque, but a list with the current index is faster).
    # Note that this does not include nodes with no dependents!
    outer_stack_seeds = []
    outer_stack_seeds_index = 0

    # Used to perform a DFS along dependencies.  Once emptied (when traversing dependencies),
    # this continues (along dependents) along a path from the initial (leaf) node down to a
    # root node (a tactical goal).  This ensures this root node will get calculated before
    # we consider any other dependents from `outer_stack_seeds`.
    inner_stack = []

    # aliases for speed
    outer_stack_append = outer_stack.append
    outer_stack_pop = outer_stack.pop
    outer_stack_extend = outer_stack.extend
    outer_stack_seeds_append = outer_stack_seeds.append
    inner_stack_append = inner_stack.append
    inner_stack_pop = inner_stack.pop
    inner_stack_extend = inner_stack.extend
    set_difference = set.difference

    is_init_sorted = False
    item = min(init_stack, key=initial_stack_key)
    while True:
        outer_stack_append(item)
        while outer_stack:
            item = outer_stack_pop()
            if item not in result:
                inner_stack_append(item)

                while inner_stack:
                    # Perform a DFS along dependencies until we complete our tactical goal
                    item = inner_stack_pop()
                    if item in result:
                        continue

                    if num_needed[item]:
                        inner_stack_append(item)
                        deps = set_difference(dependencies[item], result)
                        if 1 < len(deps) < 1000:
                            inner_stack_extend(
                                sorted(deps, key=dependencies_key, reverse=True)
                            )
                        else:
                            inner_stack_extend(deps)
                        continue

                    result[item] = i
                    i += 1
                    deps = dependents[item]

                    if metrics[item][3] == 2:  # min_height
                        # Don't leave any dangling single nodes!  Finish all dependents that are
                        # ready and are also root nodes.  Doing this here also lets us continue
                        # down to a different root node.
                        finish_now = {
                            dep
                            for dep in deps
                            if not dependents[dep] and num_needed[dep] == 1
                        }
                        if finish_now:
                            deps -= finish_now
                            if len(finish_now) > 1:
                                finish_now = sorted(finish_now, key=dependents_key)
                            for dep in finish_now:
                                result[dep] = i
                                i += 1

                    if deps:
                        for dep in deps:
                            num_needed[dep] -= 1
                        if not inner_stack:
                            # Continue towards our tactical goal (an easy-to-compute root)
                            if len(deps) == 1:
                                inner_stack_extend(deps)
                            else:
                                # If there are many, many dependents, then calculating
                                # `dependents_key` here could be relatively expensive.
                                # However, this is still probably worth it tactically.
                                inner_stack_append(min(deps, key=dependents_key))
                                outer_stack_seeds_append(item)
                        else:
                            # Our next starting point will be "seeded" by a completed task in a
                            # FIFO manner.  When our DFS with `inner_stack` is finished--which
                            # means we computed our tactical goal--we will choose our next starting
                            # point from the dependents of completed tasks.  However, it is too
                            # expensive to consider all dependents of all completed tasks, so we
                            # consider the dependents of tasks in the order they complete.
                            outer_stack_seeds_append(item)

            while not outer_stack and outer_stack_seeds_index < len(outer_stack_seeds):
                # We wait for as long as possible to consider dependents of completed nodes:
                # 1. some may have already completed, so waiting lets us sort fewer items, and
                # 2. sorting via `dependents_key` depends on `num_needed`, which is dynamic.
                deps = set_difference(
                    dependents[outer_stack_seeds[outer_stack_seeds_index]], result
                )
                if deps:
                    if 1 < len(deps) < 1000:
                        outer_stack_extend(
                            sorted(deps, key=dependents_key, reverse=True)
                        )
                    else:
                        outer_stack_extend(deps)
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
            if type(init_stack) is dict:
                init_stack = set(init_stack)
            init_stack = set_difference(init_stack, result)
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


def graph_metrics(dependencies, dependents, total_dependencies):
    r""" Useful measures of a graph used by ``dask.order.order``

    Example DAG (a1 has no dependencies; b2 and c1 are root nodes):

    c1
    |
    b1  b2
     \  /
      a1

    For each key we return:
    1.  The number of keys that can only be run after this key is run.  The
        root nodes have value 1 while deep child nodes will have larger values.

        1
        |
        2   1
         \ /
          4

    2.  The minimum value of the total number of dependencies of
        all final dependents (see module-level comment for more).
        In other words, the minimum of ``ndependencies`` of root
        nodes connected to the current node.

        3
        |
        3   2
         \ /
          2

    3.  The maximum value of the total number of dependencies of
        all final dependents (see module-level comment for more).
        In other words, the maximum of ``ndependencies`` of root
        nodes connected to the current node.

        3
        |
        3   2
         \ /
          3

    4.  The minimum height from a root node

        1
        |
        2   1
         \ /
          2

    5.  The maximum height from a root node

        1
        |
        2   1
         \ /
          3

    Examples
    --------
    >>> dsk = {'a1': 1, 'b1': (inc, 'a1'), 'b2': (inc, 'a1'), 'c1': (inc, 'b1')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> _, total_dependencies = ndependencies(dependencies, dependents)
    >>> metrics = graph_metrics(dependencies, dependents, total_dependencies)
    >>> sorted(metrics.items())
    [('a1', (4, 2, 3, 2, 3)), ('b1', (2, 3, 3, 2, 2)), ('b2', (1, 2, 2, 1, 1)), ('c1', (1, 3, 3, 1, 1))]

    Returns
    -------
    metrics: Dict[key, Tuple[int, int, int, int, int]]
    """
    result = {}
    num_needed = {k: len(v) for k, v in dependents.items() if v}
    current = []
    current_pop = current.pop
    current_append = current.append
    for key, deps in dependents.items():
        if not deps:
            val = total_dependencies[key]
            result[key] = (1, val, val, 1, 1)
            for child in dependencies[key]:
                num_needed[child] -= 1
                if not num_needed[child]:
                    current_append(child)

    while current:
        key = current_pop()
        parents = dependents[key]
        if len(parents) == 1:
            (parent,) = parents
            (
                total_dependents,
                min_dependencies,
                max_dependencies,
                min_heights,
                max_heights,
            ) = result[parent]
            result[key] = (
                1 + total_dependents,
                min_dependencies,
                max_dependencies,
                1 + min_heights,
                1 + max_heights,
            )
        else:
            (
                total_dependents,
                min_dependencies,
                max_dependencies,
                min_heights,
                max_heights,
            ) = zip(*(result[parent] for parent in dependents[key]))
            result[key] = (
                1 + sum(total_dependents),
                min(min_dependencies),
                max(max_dependencies),
                1 + min(min_heights),
                1 + max(max_heights),
            )
        for child in dependencies[key]:
            num_needed[child] -= 1
            if not num_needed[child]:
                current_append(child)
    return result


def ndependencies(dependencies, dependents):
    """ Number of total data elements on which this key depends

    For each key we return the number of tasks that must be run for us to run
    this task.

    Examples
    --------
    >>> dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> num_dependencies, total_dependencies = ndependencies(dependencies, dependents)
    >>> sorted(total_dependencies.items())
    [('a', 1), ('b', 2), ('c', 3)]

    Returns
    -------
    num_dependencies: Dict[key, int]
    total_dependencies: Dict[key, int]
    """
    num_needed = {k: len(v) for k, v in dependencies.items()}
    num_dependencies = num_needed.copy()
    current = []
    current_pop = current.pop
    current_append = current.append
    result = {k: 1 for k, v in dependencies.items() if not v}
    for key in result:
        for parent in dependents[key]:
            num_needed[parent] -= 1
            if not num_needed[parent]:
                current_append(parent)
    while current:
        key = current_pop()
        result[key] = 1 + sum(result[child] for child in dependencies[key])
        for parent in dependents[key]:
            num_needed[parent] -= 1
            if not num_needed[parent]:
                current_append(parent)
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

from __future__ import annotations

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

1.  **Small goals**: prefer tasks that have few total dependents and whose final
    dependents have few total dependencies.

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
from collections import defaultdict, namedtuple
from collections.abc import Mapping, MutableMapping
from heapq import heappop, heappush, nsmallest
from typing import Any, cast

from dask.core import get_dependencies, get_deps, getcycle, istask, reverse_dict
from dask.typing import Key


def order(
    dsk: MutableMapping[Key, Any],
    dependencies: MutableMapping[Key, set[Key]] | None = None,
) -> dict[Key, int]:
    if not dsk:
        return {}

    dsk = dict(dsk)

    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k) for k in dsk}
    dependents = reverse_dict(dependencies)
    num_needed, total_dependencies = ndependencies(dependencies, dependents)
    metrics = graph_metrics(dependencies, dependents)

    if len(metrics) != len(dsk):
        cycle = getcycle(dsk, None)
        raise RuntimeError(
            "Cycle detected between the following keys:\n  -> %s"
            % "\n  -> ".join(str(x) for x in cycle)
        )

    root_nodes = {k for k, v in dependents.items() if not v}
    if len(root_nodes) > 1:
        # This is also nice because it makes us robust to difference when
        # computing vs persisting collections
        root = cast(Key, object())

        def _f(*args: Any, **kwargs: Any) -> None:
            pass

        dsk[root] = (_f, *root_nodes)
        dependencies[root] = root_nodes
        o = order(dsk, dependencies)
        del o[root]
        return o
    root = list(root_nodes)[0]
    init_stack: dict[Key, tuple] | set[Key] | list[Key]
    # Leaf nodes.  We choose one--the initial node--for each weakly connected subgraph.
    # Let's calculate the `initial_stack_key` as we determine `init_stack` set.
    init_stack = {
        # First prioritize large, tall groups, then prioritize the same as ``dependents_key``.
        key: (
            # at a high-level, work towards a large goal (and prefer tall and narrow)
            num_dependents - max_heights,
            # tactically, finish small connected jobs first
            num_dependents - min_heights,  # prefer tall and narrow
            -total_dependents,  # take a big step
            # try to be memory efficient
            num_dependents,
            # tie-breaker
            StrComparable(key),
        )
        for key, num_dependents, (
            total_dependents,
            min_heights,
            max_heights,
        ) in (
            (key, len(dependents[key]), metrics[key])
            for key, val in dependencies.items()
            if not val
        )
    }
    is_init_sorted = False
    # `initial_stack_key` chooses which task to run at the very beginning.
    # This value is static, so we pre-compute as the value of this dict.
    initial_stack_key = init_stack.__getitem__

    def dependents_key(x: Key) -> tuple:
        """Choose a path from our starting task to our tactical goal

        This path is connected to a large goal, but focuses on completing
        a small goal and being memory efficient.
        """
        assert dependencies is not None

        return (
            # Focus on being memory-efficient
            len(dependents[x]) - len(dependencies[x]) + num_needed[x],
            # Do we favor deep or shallow branches?
            #  -1: deep
            #  +1: shallow
            -metrics[x][1],  # min_heights
            # tie-breaker
            StrComparable(x),
        )

    def dependencies_key(x: Key) -> tuple:
        """Choose which dependency to run as part of a reverse DFS

        This is very similar to both ``initial_stack_key``.
        """
        assert dependencies is not None
        num_dependents = len(dependents[x])
        (
            total_dependents,
            min_heights,
            max_heights,
        ) = metrics[x]
        # Prefer short and narrow instead of tall in narrow, because we're going in
        # reverse along dependencies.
        return (
            num_dependents + max_heights,
            num_dependents + min_heights,  # prefer short and narrow
            -total_dependencies[x],  # go where the work is
            # try to be memory efficient
            num_dependents - len(dependencies[x]) + num_needed[x],
            num_dependents,
            total_dependents,  # already found work, so don't add more
            # tie-breaker
            StrComparable(x),
        )

    seen = set(root_nodes)
    seen_update = seen.update
    root_total_dependencies = total_dependencies[list(root_nodes)[0]]
    partition_keys = {
        key: (
            (root_total_dependencies - total_dependencies[key] + 1),
            (total_dependents - min_heights),
            -max_heights,
        )
        for key, (
            total_dependents,
            min_heights,
            max_heights,
        ) in metrics.items()
    }
    result: dict[Key, int] = {root: len(dsk) - 1}
    i = 0

    inner_stack = [min(init_stack, key=initial_stack_key)]
    inner_stack_pop = inner_stack.pop
    next_nodes: defaultdict[tuple[int, ...], set[Key]] = defaultdict(set)
    min_key_next_nodes: list[tuple[int, ...]] = []
    runnable_by_parent: defaultdict[Key, set[Key]] = defaultdict(set)

    def process_runnables(layers_loaded: int) -> None:
        nonlocal i
        # Sort by number of dependents such that we process parents with few dependents first.
        # This is a performance optimization that allows us to break the for
        # loop early if we find a parent that is not allowed to proceed. This is
        # merely an assumption that is not generally true but has been proven to
        # be effective in practice.
        for parent, runnable_tasks in sorted(
            runnable_by_parent.items(), key=lambda x: len(dependents[x[0]])
        ):
            pkey = partition_keys[parent]
            deps_parent = dependents[parent]
            deps_not_in_result = deps_parent.difference(result)
            # We only want to process nodes that guarantee to release the
            # parent, i.e. len(deps_not_in_result) == 1
            # However, the more aggressively the DFS has to backtrack, the more
            # eagerly we are willing to process other runnable tasks to release
            # as many parents as possible before loading more data (which
            # typically happens when backtracking).
            if len(deps_not_in_result) > 1 + layers_loaded:
                heappush(min_key_next_nodes, pkey)
                next_nodes[pkey].update(runnable_tasks)
                break
            del runnable_by_parent[parent]
            runnable_candidates = runnable_tasks - seen
            runnable_sorted = sorted(
                runnable_candidates, key=partition_keys.__getitem__, reverse=True
            )
            while runnable_sorted:
                task = runnable_sorted.pop()
                result[task] = i
                i += 1
                deps = dependents[task]
                for dep in deps:
                    num_needed[dep] -= 1
                    if not num_needed[dep]:
                        runnable_sorted.append(dep)
                    else:
                        pkey = partition_keys[dep]
                        heappush(min_key_next_nodes, pkey)
                        next_nodes[pkey].add(dep)

    layers_loaded = 0
    dep_pools = defaultdict(set)
    while True:
        while inner_stack:
            item = inner_stack_pop()
            if item in result:
                continue
            if num_needed[item]:
                inner_stack.append(item)
                deps = dependencies[item].difference(result)
                if 1 < len(deps) < 1000:
                    inner_stack.extend(sorted(deps, key=dependencies_key, reverse=True))
                else:
                    inner_stack.extend(deps)
                seen_update(deps)
                if not num_needed[inner_stack[-1]]:
                    process_runnables(layers_loaded)
                layers_loaded += 1
                continue
            result[item] = i
            i += 1
            deps = dependents[item]
            for dep in deps:
                num_needed[dep] -= 1
                if not num_needed[dep]:
                    runnable_by_parent[item].add(dep)

            all_keys = []
            for dep in deps:
                if dep in seen:
                    continue
                pkey = partition_keys[dep]
                dep_pools[pkey].add(dep)
                all_keys.append(pkey)
            all_keys.sort()
            target_key: tuple[int, ...] | None = None
            for pkey in reversed(all_keys):
                if inner_stack:
                    target_key = target_key or partition_keys[inner_stack[0]]
                    if pkey < target_key:
                        next_nodes[target_key].update(inner_stack)
                        heappush(min_key_next_nodes, target_key)
                        inner_stack = sorted(
                            dep_pools[pkey], key=dependents_key, reverse=True
                        )
                        inner_stack_pop = inner_stack.pop
                        seen_update(inner_stack)
                        continue
                next_nodes[pkey].update(dep_pools[pkey])
                heappush(min_key_next_nodes, pkey)

            dep_pools.clear()

        process_runnables(layers_loaded)
        layers_loaded = 0

        if next_nodes and not inner_stack:
            # there may be duplicates on the heap
            min_key = heappop(min_key_next_nodes)
            while min_key not in next_nodes:
                min_key = heappop(min_key_next_nodes)
            next_stack = next_nodes.pop(min_key)
            next_stack = next_stack.difference(result)
            # We have to sort the inner_stack but sorting is
            # on average O(n log n). Particularly with the custom key
            # `dependents_key`, this sorting operation can be quite expensive
            # and dominate the entire ordering.
            # There is also no guarantee that even if we sorted the entire
            # stack, that we can actually process it until the end since there
            # is logic that will switch the stack if a better target is found.
            # Therefore, in case of large stacks, we break it up and take only
            # the best nodes. This runs in linear time and will possibly allow
            # us to release a couple of dangling runnables or find a better
            # target before we come back to process the next batch
            cutoff = 50
            if len(next_stack) > cutoff:
                inner_stack = nsmallest(cutoff, list(next_stack), key=dependents_key)[
                    ::-1
                ]
                next_nodes[min_key].update(next_stack)
                heappush(min_key_next_nodes, min_key)
            else:
                inner_stack = sorted(next_stack, key=dependents_key, reverse=True)
            inner_stack_pop = inner_stack.pop
            seen_update(inner_stack)
            continue

        if inner_stack:
            continue

        if len(result) == len(dsk):
            break

        if not is_init_sorted:
            init_stack = set(init_stack)
            init_stack = init_stack.difference(result)
            if len(init_stack) < 10000:
                init_stack = sorted(init_stack, key=initial_stack_key, reverse=True)
            else:
                init_stack = list(init_stack)
            is_init_sorted = True

        inner_stack = [init_stack.pop()]  # type: ignore[call-overload]
        inner_stack_pop = inner_stack.pop
    return result


def graph_metrics(
    dependencies: Mapping[Key, set[Key]],
    dependents: Mapping[Key, set[Key]],
) -> dict[Key, tuple[int, int, int]]:
    r"""Useful measures of a graph used by ``dask.order.order``

    Example DAG (a1 has no dependencies; b2 and c1 are root nodes):

    c1
    |
    b1  b2
     \  /
      a1

    For each key we return:

    1.  **total_dependents**: The number of keys that can only be run
        after this key is run.
        Note that this is only exact for trees. (undirected) cycles will cause
        double counting of nodes. Therefore, this metric is an upper bound
        approximation.

        1
        |
        2   1
         \ /
          4

    2.  **min_height**: The minimum height from a root node

        0
        |
        1   0
         \ /
          1

    3.  **max_height**: The maximum height from a root node

        0
        |
        1   0
         \ /
          2

    Examples
    --------
    >>> inc = lambda x: x + 1
    >>> dsk = {'a1': 1, 'b1': (inc, 'a1'), 'b2': (inc, 'a1'), 'c1': (inc, 'b1')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> metrics = graph_metrics(dependencies, dependents)
    >>> sorted(metrics.items())
    [('a1', (4, 1, 2)), ('b1', (2, 1, 1)), ('b2', (1, 0, 0)), ('c1', (1, 0, 0))]

    Returns
    -------
    metrics: Dict[key, Tuple[int, int, int, int, int]]
    """
    result = {}
    num_needed = {k: len(v) for k, v in dependents.items() if v}
    current: list[Key] = []
    current_pop = current.pop
    current_append = current.append
    for key, deps in dependents.items():
        if not deps:
            result[key] = (1, 0, 0)
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
                min_heights,
                max_heights,
            ) = result[parent]
            result[key] = (
                1 + total_dependents,
                1 + min_heights,
                1 + max_heights,
            )
        else:
            (
                total_dependents_,
                min_heights_,
                max_heights_,
            ) = zip(*(result[parent] for parent in dependents[key]))
            result[key] = (
                1 + sum(total_dependents_),
                1 + min(min_heights_),
                1 + max(max_heights_),
            )
        for child in dependencies[key]:
            num_needed[child] -= 1
            if not num_needed[child]:
                current_append(child)
    return result


def ndependencies(
    dependencies: Mapping[Key, set[Key]], dependents: Mapping[Key, set[Key]]
) -> tuple[dict[Key, int], dict[Key, int]]:
    """Number of total data elements on which this key depends

    For each key we return the number of tasks that must be run for us to run
    this task.

    Examples
    --------
    >>> inc = lambda x: x + 1
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
    num_needed = {}
    result = {}
    for k, v in dependencies.items():
        num_needed[k] = len(v)
        if not v:
            result[k] = 1

    num_dependencies = num_needed.copy()
    current: list[Key] = []
    current_pop = current.pop
    current_append = current.append

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


class StrComparable:
    """Wrap object so that it defaults to string comparison

    When comparing two objects of different types Python fails

    >>> 'a' < 1
    Traceback (most recent call last):
        ...
    TypeError: '<' not supported between instances of 'str' and 'int'

    This class wraps the object so that, when this would occur it instead
    compares the string representation

    >>> StrComparable('a') < StrComparable(1)
    False
    """

    __slots__ = ("obj",)

    obj: Any

    def __init__(self, obj: Any):
        self.obj = obj

    def __lt__(self, other: Any) -> bool:
        try:
            return self.obj < other.obj
        except Exception:
            return str(self.obj) < str(other.obj)


OrderInfo = namedtuple(
    "OrderInfo",
    (
        "order",
        "age",
        "num_data_when_run",
        "num_data_when_released",
        "num_dependencies_freed",
    ),
)


def diagnostics(
    dsk: MutableMapping[Key, Any],
    o: Mapping[Key, int] | None = None,
    dependencies: MutableMapping[Key, set[Key]] | None = None,
) -> tuple[dict[Key, OrderInfo], list[int]]:
    """Simulate runtime metrics as though running tasks one at a time in order.

    These diagnostics can help reveal behaviors of and issues with ``order``.

    Returns a dict of `namedtuple("OrderInfo")` and a list of the number of outputs held over time.

    OrderInfo fields:
    - order : the order in which the node is run.
    - age : how long the output of a node is held.
    - num_data_when_run : the number of outputs held in memory when a node is run.
    - num_data_when_released : the number of outputs held in memory when the output is released.
    - num_dependencies_freed : the number of dependencies freed by running the node.
    """
    if dependencies is None:
        dependencies, dependents = get_deps(dsk)
    else:
        dependents = reverse_dict(dependencies)
    if o is None:
        o = order(dsk, dependencies=dependencies)

    pressure = []
    num_in_memory = 0
    age = {}
    runpressure = {}
    releasepressure = {}
    freed = {}
    num_needed = {key: len(val) for key, val in dependents.items()}
    for i, key in enumerate(sorted(dsk, key=o.__getitem__)):
        pressure.append(num_in_memory)
        runpressure[key] = num_in_memory
        released = 0
        for dep in dependencies[key]:
            num_needed[dep] -= 1
            if num_needed[dep] == 0:
                age[dep] = i - o[dep]
                releasepressure[dep] = num_in_memory
                released += 1
        freed[key] = released
        if dependents[key]:
            num_in_memory -= released - 1
        else:
            age[key] = 0
            releasepressure[key] = num_in_memory
            num_in_memory -= released

    rv = {
        key: OrderInfo(
            val, age[key], runpressure[key], releasepressure[key], freed[key]
        )
        for key, val in o.items()
    }
    return rv, pressure


def _f() -> None:
    ...


def _convert_task(task: Any) -> Any:
    if istask(task):
        assert callable(task[0])
        new_spec: list[Any] = []
        for el in task[1:]:
            if isinstance(el, (str, int)):
                new_spec.append(el)
            elif isinstance(el, tuple):
                if istask(el):
                    new_spec.append(_convert_task(el))
                else:
                    new_spec.append(el)
            elif isinstance(el, list):
                new_spec.append([_convert_task(e) for e in el])
        return (_f, *new_spec)
    elif isinstance(task, tuple):
        return (_f, task)
    else:
        return (_f, *task)


def sanitize_dsk(dsk: MutableMapping[Key, Any]) -> dict:
    """Take a dask graph and replace callables with a dummy function and remove
    payload data like numpy arrays, dataframes, etc.
    """
    new = {}
    for key, values in dsk.items():
        new_key = key
        new[new_key] = _convert_task(values)
    if get_deps(new) != get_deps(dsk):
        # The switch statement in _convert likely dropped some keys
        raise RuntimeError("Sanitization failed to preserve topology.")
    return new

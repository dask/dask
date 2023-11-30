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
from collections import deque, namedtuple
from collections.abc import Mapping, MutableMapping
from typing import Any

from dask.core import get_dependencies, get_deps, getcycle, istask, reverse_dict
from dask.typing import Key


def order(
    dsk: MutableMapping[Key, Any],
    dependencies: MutableMapping[Key, set[Key]] | None = None,
    validate: bool = False,
) -> dict[Key, int]:
    if not dsk:
        return {}

    dsk = dict(dsk)

    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k) for k in dsk}

    dependents = reverse_dict(dependencies)
    num_needed, total_dependencies = ndependencies(dependencies, dependents)
    num_depending, total_dependents = ndependencies(dependents, dependencies)
    # metrics = graph_metrics(dependencies, dependents, total_dependencies)
    metrics_reverse = graph_metrics(dependents, dependencies, total_dependents)

    if len(metrics_reverse) != len(dsk):
        cycle = getcycle(dsk, None)
        raise RuntimeError(
            "Cycle detected between the following keys:\n  -> %s"
            % "\n  -> ".join(str(x) for x in cycle)
        )
    roots = {k for k, v in dependents.items() if not v}

    # if len(root_nodes) == 1:
    #     root = list(root_nodes)[0]
    #     dsk_no_root = dsk.copy()
    #     dsk_no_root.pop(root)
    #     o = order(dsk_no_root)
    #     o[root] = len(dsk) - 1
    #     return o

    result = {}
    i = 0
    linear_hull = set()
    runnable = []
    connected_subgraph = False
    known_runnable_paths: dict[Key, list[list[Key]]] = {}

    # TODO: Somehow sort this in a smarter way. Seems to do mostly fine, though
    # We may want to process smaller groups first to get the chance to hit
    # connected subgraphs
    def root_key(x: Key) -> tuple:
        return (*metrics_reverse[x], StrComparable(x))

    root_nodes_sorted = sorted(roots, key=root_key)

    def add_to_result(item: Key) -> None:
        assert not num_needed[item]
        linear_hull.discard(item)
        # runnable.discard(item)
        if item in result:
            return
        nonlocal i
        result[item] = i
        i += 1
        for dep in dependents[item]:
            num_needed[dep] -= 1
            if not num_needed[dep]:
                if len(dependents[item]) == 1:
                    # FIXME: I saw this causing recursion depth errors. That
                    # probably happens for veeeeery long linear chains but I
                    # haven't confirmed
                    add_to_result(dep)
                else:
                    runnable.append(dep)

    def process_runnables() -> None:
        candidates = runnable.copy()
        runnable.clear()
        while candidates:
            key = candidates.pop()
            if key in linear_hull or key in result:
                continue
            if key in roots:
                add_to_result(key)
                continue
            path = [key]

            branches = deque([path])
            while branches:
                path = branches.popleft()
                while True:
                    current = path[-1]
                    linear_hull.add(current)
                    deps_downstream = dependents[current]
                    deps_upstream = dependencies[current]  # type: ignore
                    if current in roots:
                        # FIXME: The fact that it is possible for
                        # num_needed[current] == 0 means we're doing some work
                        # twice
                        if num_needed[current] <= 1 or (
                            not branches
                            # FIXME: This is a very magical number
                            and len(path) > 2
                        ):
                            for k in path[:-1]:
                                add_to_result(k)
                            if not num_needed[current]:
                                add_to_result(current)
                    elif current not in roots and (
                        len(path) == 1 or len(deps_upstream) == 1
                    ):
                        if len(deps_downstream) > 1:
                            # TODO: sort this with something meaningful
                            for d in sorted(deps_downstream, key=StrComparable):
                                # This ensure we're only considering splitters
                                # that are genuinely splitting and not
                                # interleaving
                                if len(dependencies[d]) == 1:  # type: ignore
                                    branch = path.copy()
                                    branch.append(d)
                                    branches.append(branch)
                            break
                        linear_hull.update(deps_downstream)
                        # TODO: Should this be sorted?
                        path.extend(deps_downstream)
                        continue
                    elif current in known_runnable_paths:
                        known_runnable_paths[current].append(path)
                        if len(known_runnable_paths[current]) >= num_needed[current]:
                            pruned_branches: deque[list[Key]] = deque()
                            for path in known_runnable_paths.pop(current):
                                if path[-2] not in result:
                                    pruned_branches.append(path)
                            if len(pruned_branches) < num_needed[current]:
                                known_runnable_paths[current] = list(pruned_branches)
                            else:
                                if validate:
                                    nodes_in_branches = set()
                                    for b in pruned_branches:
                                        nodes_in_branches.update(b)
                                    cond = not (
                                        dependencies[current]  # type: ignore
                                        - set(result)
                                        - nodes_in_branches
                                    )
                                    assert cond
                                while pruned_branches:
                                    path = pruned_branches.popleft()
                                    for k in path:
                                        if num_needed[k]:
                                            pruned_branches.append(path)
                                            break
                                        add_to_result(k)
                    else:
                        if len(dependencies[current]) > 1 and num_needed[current] <= 1:  # type: ignore
                            for k in path:
                                add_to_result(k)
                        else:
                            known_runnable_paths[current] = [path]
                    break

    known_critical_paths: dict[Key, list[Key]] = {}
    while len(result) < len(dsk):
        critical_path: list[Key] = []
        while not critical_path:
            if not root_nodes_sorted:
                critical_path = known_critical_paths.popitem()[1]
                break
            next_item = root = root_nodes_sorted.pop()
            next_deps = dependencies[next_item]
            critical_path = [next_item]
            if linear_hull & next_deps:
                connected_subgraph = True
            else:
                while next_deps:
                    item = max(next_deps, key=root_key)
                    critical_path.append(item)
                    next_deps = dependencies[item]
                    # TODO: Is this sorting actually useful?
                    for dep in sorted(next_deps, key=root_key):
                        if not num_needed[dep]:
                            add_to_result(dep)
                    if linear_hull & next_deps:
                        connected_subgraph = True
                        break
            if linear_hull and not connected_subgraph:
                known_critical_paths[root] = critical_path
                critical_path = []

        walked_back = False
        while critical_path:
            item = critical_path.pop()
            if item in result:
                continue
            if num_needed[item]:
                if item in known_runnable_paths:
                    for path in known_runnable_paths.pop(item):
                        critical_path.extend(path[::-1])
                    continue
                critical_path.append(item)
                deps = dependencies[item].difference(result)
                unknown = []
                known = []
                for d in sorted(deps, key=root_key):
                    if d in known_runnable_paths:
                        known.append(d)
                    else:
                        unknown.append(d)
                if len(unknown) > 1:
                    walked_back = True

                for d in unknown:
                    critical_path.append(d)
                for d in known:
                    for path in known_runnable_paths.pop(d):
                        critical_path.extend(path[::-1])

                del deps
                continue
            else:
                if walked_back and len(runnable) < len(critical_path):
                    process_runnables()
                add_to_result(item)
        process_runnables()

    return result


def graph_metrics(
    dependencies: Mapping[Key, set[Key]],
    dependents: Mapping[Key, set[Key]],
    total_dependencies: Mapping[Key, int],
) -> dict[Key, tuple[int, int, int, int, int]]:
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

    2.  **min_dependencies**: The minimum value of the total number of
        dependencies of all final dependents (see module-level comment for more).
        In other words, the minimum of ``ndependencies`` of root
        nodes connected to the current node.

        3
        |
        3   2
         \ /
          2

    3.  **max_dependencies**: The maximum value of the total number of
        dependencies of all final dependents (see module-level comment for more).
        In other words, the maximum of ``ndependencies`` of root
        nodes connected to the current node.

        3
        |
        3   2
         \ /
          3

    4.  **min_height**: The minimum height from a root node

        0
        |
        1   0
         \ /
          1

    5.  **max_height**: The maximum height from a root node

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
    >>> _, total_dependencies = ndependencies(dependencies, dependents)
    >>> metrics = graph_metrics(dependencies, dependents, total_dependencies)
    >>> sorted(metrics.items())
    [('a1', (4, 2, 3, 1, 2)), ('b1', (2, 3, 3, 1, 1)), ('b2', (1, 2, 2, 0, 0)), ('c1', (1, 3, 3, 0, 0))]

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
            val = total_dependencies[key]
            result[key] = (1, val, val, 0, 0)
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
                total_dependents_,
                min_dependencies_,
                max_dependencies_,
                min_heights_,
                max_heights_,
            ) = zip(*(result[parent] for parent in dependents[key]))
            result[key] = (
                1 + sum(total_dependents_),
                min(min_dependencies_),
                max(max_dependencies_),
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

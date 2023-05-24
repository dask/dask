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
from math import log

from dask.core import get_dependencies, get_deps, getcycle, reverse_dict


def order(dsk, dependencies=None):
    """Order nodes in dask graph

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
    >>> inc = lambda x: x + 1
    >>> add = lambda x, y: x + y
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

    # Single root nodes that depend on everything. These cause issues for
    # the current ordering algorithm, since we often hit the root node
    # and fell back to the key tie-breaker to choose which immediate dependency
    # to finish next, rather than finishing off subtrees.
    # So under the special case of a single root node that depends on the entire
    # tree, we skip processing it normally.
    # See https://github.com/dask/dask/issues/6745
    root_nodes = {k for k, v in dependents.items() if not v}
    skip_root_node = len(root_nodes) == 1 and len(dsk) > 1

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
        """Choose a path from our starting task to our tactical goal

        This path is connected to a large goal, but focuses on completing
        a small goal and being memory efficient.
        """
        return (
            # Focus on being memory-efficient
            len(dependents[x]) - len(dependencies[x]) + num_needed[x],
            -metrics[x][3],  # min_heights
            # tie-breaker
            StrComparable(x),
        )

    def dependencies_key(x):
        """Choose which dependency to run as part of a reverse DFS

        This is very similar to both ``initial_stack_key``.
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

    def finish_now_key(x):
        """Determine the order of dependents that are ready to run and be released"""
        return (-len(dependencies[x]), StrComparable(x))

    # Computing this for all keys can sometimes be relatively expensive :(
    partition_keys = {
        key: (
            (min_dependencies - total_dependencies[key] + 1)
            * (total_dependents - min_heights)
        )
        for key, (
            total_dependents,
            min_dependencies,
            _,
            min_heights,
            _,
        ) in metrics.items()
    }

    result = {}
    i = 0

    # `inner_stack` is used to perform a DFS along dependencies.  Once emptied
    # (when traversing dependencies), this continue down a path along dependents
    # until a root node is reached.
    #
    # Sometimes, a better path along a dependent is discovered (i.e., something
    # that is easier to compute and doesn't requiring holding too much in memory).
    # In this case, the current `inner_stack` is appended to `inner_stacks` and
    # we begin a new DFS from the better node.
    #
    # A "better path" is determined by comparing `partition_keys`.
    inner_stack = [min(init_stack, key=initial_stack_key)]
    inner_stack_pop = inner_stack.pop
    inner_stacks = []
    inner_stacks_append = inner_stacks.append
    inner_stacks_extend = inner_stacks.extend
    inner_stacks_pop = inner_stacks.pop

    # Okay, now we get to the data structures used for fancy behavior.
    #
    # As we traverse nodes in the DFS along dependencies, we partition the dependents
    # via `partition_key`.  A dependent goes to:
    #    1) `inner_stack` if it's better than our current target,
    #    2) `next_nodes` if the partition key is lower than it's parent,
    #    3) `later_nodes` otherwise.
    # When the inner stacks are depleted, we process `next_nodes`.  If `next_nodes` is
    # empty (and `outer_stacks` is empty`), then we process `later_nodes` the same way.
    # These dicts use `partition_keys` as keys.  We process them by placing the values
    # in `outer_stack` so that the smallest keys will be processed first.
    next_nodes = defaultdict(list)
    later_nodes = defaultdict(list)

    # `outer_stack` is used to populate `inner_stacks`.  From the time we partition the
    # dependents of a node, we group them: one list per partition key per parent node.
    # This likely results in many small lists.  We do this to avoid sorting many larger
    # lists (i.e., to avoid n*log(n) behavior).  So, we have many small lists that we
    # partitioned, and we keep them in the order that we saw them (we will process them
    # in a FIFO manner).  By delaying sorting for as long as we can, we can first filter
    # out nodes that have already been computed.  All this complexity is worth it!
    outer_stack = []
    outer_stack_extend = outer_stack.extend
    outer_stack_pop = outer_stack.pop

    # Keep track of nodes that are in `inner_stack` or `inner_stacks` so we don't
    # process them again.
    if skip_root_node:
        seen = set(root_nodes)
    else:
        seen = set()  # seen in an inner_stack (and has dependencies)
    seen_update = seen.update
    seen_add = seen.add

    # "singles" are tasks that are available to run, and when run may free a dependency.
    # Although running a task to free a dependency may seem like a wash (net zero), it
    # can be beneficial by providing more opportunities for a later task to free even
    # more data.  So, it costs us little in the short term to more eagerly compute
    # chains of tasks that keep the same number of data in memory, and the longer term
    # rewards are potentially high.  I would expect a dynamic scheduler to have similar
    # behavior, so I think it makes sense to do the same thing here in `dask.order`.
    #
    # When we gather tasks in `singles`, we do so optimistically: running the task *may*
    # free the parent, but it also may not, because other dependents of the parent may
    # be in the inner stacks.  When we process `singles`, we run tasks that *will* free
    # the parent, otherwise we move the task to `later_singles`.  `later_singles` is run
    # when there are no inner stacks, so it is safe to run all of them (because no other
    # dependents will be hiding in the inner stacks to keep hold of the parent).
    # `singles` is processed when the current item on the stack needs to compute
    # dependencies before it can be run.
    #
    # Processing singles is meant to be a detour.  Doing so should not change our
    # tactical goal in most cases.  Hence, we set `add_to_inner_stack = False`.
    #
    # In reality, this is a pretty limited strategy for running a task to free a
    # dependency.  A thorough strategy would be to check whether running a dependent
    # with `num_needed[dep] == 0` would free *any* of its dependencies.  This isn't
    # what we do.  This data isn't readily or cheaply available.  We only check whether
    # it will free its last dependency that was computed (the current `item`).  This is
    # probably okay.  In general, our tactics and strategies for ordering try to be
    # memory efficient, so we shouldn't try too hard to work around what we already do.
    # However, sometimes the DFS nature of it leaves "easy-to-compute" stragglers behind.
    # The current approach is very fast to compute, can be beneficial, and is generally
    # low-risk.  There could be more we could do here though.  Our static scheduling
    # here primarily looks at "what dependent should we run next?" instead of "what
    # dependency should we try to free?"  Two sides to the same question, but a dynamic
    # scheduler is much better able to answer the latter one, because it knows the size
    # of data and can react to current state.  Does adding a little more dynamic-like
    # behavior to `dask.order` add any tension to running with an actual dynamic
    # scheduler?  Should we defer to dynamic schedulers and let them behave like this
    # if they so choose?  Maybe.  However, I'm sensitive to the multithreaded scheduler,
    # which is heavily dependent on the ordering obtained here.
    singles = {}
    singles_items = singles.items()
    singles_clear = singles.clear
    later_singles = []
    later_singles_append = later_singles.append
    later_singles_clear = later_singles.clear

    # Priority of being processed
    #   1. inner_stack
    #   2. singles (may be moved to later_singles)
    #   3. inner_stacks
    #   4. later_singles
    #   5. next_nodes
    #   6. outer_stack
    #   7. later_nodes
    #   8. init_stack

    # alias for speed
    set_difference = set.difference

    is_init_sorted = False
    while True:
        while True:
            # Perform a DFS along dependencies until we complete our tactical goal
            if inner_stack:
                item = inner_stack_pop()
                if item in result:
                    continue
                if num_needed[item]:
                    if not skip_root_node or item not in root_nodes:
                        inner_stack.append(item)
                        deps = set_difference(dependencies[item], result)
                        if 1 < len(deps) < 1000:
                            inner_stack.extend(
                                sorted(deps, key=dependencies_key, reverse=True)
                            )
                        else:
                            inner_stack.extend(deps)
                        seen_update(deps)
                    if not singles:
                        continue
                    process_singles = True
                else:
                    result[item] = i
                    i += 1
                    deps = dependents[item]
                    add_to_inner_stack = True

                    if metrics[item][3] == 1:  # min_height
                        # Don't leave any dangling single nodes!  Finish all dependents that are
                        # ready and are also root nodes.
                        finish_now = {
                            dep
                            for dep in deps
                            if not dependents[dep] and num_needed[dep] == 1
                        }
                        if finish_now:
                            deps -= finish_now  # Safe to mutate
                            if len(finish_now) > 1:
                                finish_now = sorted(finish_now, key=finish_now_key)
                            for dep in finish_now:
                                result[dep] = i
                                i += 1
                            add_to_inner_stack = False
                        elif skip_root_node:
                            for dep in root_nodes:
                                num_needed[dep] -= 1
                                # Use remove here to complain loudly if our assumptions change
                                deps.remove(dep)  # Safe to mutate

                    if deps:
                        for dep in deps:
                            num_needed[dep] -= 1
                        process_singles = False
                    else:
                        continue
            elif inner_stacks:
                inner_stack = inner_stacks_pop()
                inner_stack_pop = inner_stack.pop
                continue
            elif singles:
                process_singles = True
            elif later_singles:
                # No need to be optimistic: all nodes in `later_singles` will free a dependency
                # when run, so no need to check whether dependents are in `seen`.
                deps = set()
                for single in later_singles:
                    if single in result:
                        continue
                    while True:
                        dep2 = dependents[single]
                        result[single] = i
                        i += 1
                        if metrics[single][3] == 1:  # min_height
                            # Don't leave any dangling single nodes!  Finish all dependents that are
                            # ready and are also root nodes.
                            finish_now = {
                                dep
                                for dep in dep2
                                if not dependents[dep] and num_needed[dep] == 1
                            }
                            if finish_now:
                                dep2 -= finish_now  # Safe to mutate
                                if len(finish_now) > 1:
                                    finish_now = sorted(finish_now, key=finish_now_key)
                                for dep in finish_now:
                                    result[dep] = i
                                    i += 1
                            elif skip_root_node:
                                for dep in root_nodes:
                                    num_needed[dep] -= 1
                                    # Use remove here to complain loudly if our assumptions change
                                    dep2.remove(dep)  # Safe to mutate
                        if dep2:
                            for dep in dep2:
                                num_needed[dep] -= 1
                            if len(dep2) == 1:
                                # Fast path!  We trim down `dep2` above hoping to reach here.
                                (single,) = dep2
                                if not num_needed[single]:
                                    # Keep it going!
                                    dep2 = dependents[single]
                                    continue
                            deps |= dep2
                        break
                later_singles_clear()
                deps = set_difference(deps, result)
                if not deps:
                    continue
                add_to_inner_stack = False
                process_singles = True
            else:
                break

            if process_singles and singles:
                # We gather all dependents of all singles into `deps`, which we then process below.
                # A lingering question is: what should we use for `item`?  `item_key` is used to
                # determine whether each dependent goes to `next_nodes` or `later_nodes`.  Currently,
                # we use the last value of `item` (i.e., we don't do anything).
                deps = set()
                add_to_inner_stack = True if inner_stack or inner_stacks else False
                for single, parent in singles_items:
                    if single in result:
                        continue
                    if (
                        add_to_inner_stack
                        and len(set_difference(dependents[parent], result)) > 1
                    ):
                        later_singles_append(single)
                        continue

                    while True:
                        dep2 = dependents[single]
                        result[single] = i
                        i += 1
                        if metrics[single][3] == 1:  # min_height
                            # Don't leave any dangling single nodes!  Finish all dependents that are
                            # ready and are also root nodes.
                            finish_now = {
                                dep
                                for dep in dep2
                                if not dependents[dep] and num_needed[dep] == 1
                            }
                            if finish_now:
                                dep2 -= finish_now  # Safe to mutate
                                if len(finish_now) > 1:
                                    finish_now = sorted(finish_now, key=finish_now_key)
                                for dep in finish_now:
                                    result[dep] = i
                                    i += 1
                            elif skip_root_node:
                                for dep in root_nodes:
                                    num_needed[dep] -= 1
                                    # Use remove here to complain loudly if our assumptions change
                                    dep2.remove(dep)  # Safe to mutate
                        if dep2:
                            for dep in dep2:
                                num_needed[dep] -= 1
                            if add_to_inner_stack:
                                already_seen = dep2 & seen
                                if already_seen:
                                    if len(dep2) == len(already_seen):
                                        if len(already_seen) == 1:
                                            (single,) = already_seen
                                            if not num_needed[single]:
                                                dep2 = dependents[single]
                                                continue
                                        break
                                    dep2 = dep2 - already_seen
                            else:
                                already_seen = False
                            if len(dep2) == 1:
                                # Fast path!  We trim down `dep2` above hoping to reach here.
                                (single,) = dep2
                                if not num_needed[single]:
                                    if not already_seen:
                                        # Keep it going!
                                        dep2 = dependents[single]
                                        continue
                                    later_singles_append(single)
                                    break
                            deps |= dep2
                        break

                deps = set_difference(deps, result)
                singles_clear()
                if not deps:
                    continue
                add_to_inner_stack = False

            # If inner_stack is empty, then we typically add the best dependent to it.
            # However, we don't add to it if we complete a node early via "finish_now" above
            # or if a dependent is already on an inner_stack.  In this case, we add the
            # dependents (not in an inner_stack) to next_nodes or later_nodes to handle later.
            # This serves three purposes:
            #   1. shrink `deps` so that it can be processed faster,
            #   2. make sure we don't process the same dependency repeatedly, and
            #   3. make sure we don't accidentally continue down an expensive-to-compute path.
            already_seen = deps & seen
            if already_seen:
                if len(deps) == len(already_seen):
                    if len(already_seen) == 1:
                        (dep,) = already_seen
                        if not num_needed[dep]:
                            singles[dep] = item
                    continue
                add_to_inner_stack = False
                deps = deps - already_seen

            if len(deps) == 1:
                # Fast path!  We trim down `deps` above hoping to reach here.
                (dep,) = deps
                if not inner_stack:
                    if add_to_inner_stack:
                        inner_stack = [dep]
                        inner_stack_pop = inner_stack.pop
                        seen_add(dep)
                        continue
                    key = partition_keys[dep]
                else:
                    key = partition_keys[dep]
                    if key < partition_keys[inner_stack[0]]:
                        # Run before `inner_stack` (change tactical goal!)
                        inner_stacks_append(inner_stack)
                        inner_stack = [dep]
                        inner_stack_pop = inner_stack.pop
                        seen_add(dep)
                        continue
                if not num_needed[dep]:
                    # We didn't put the single dependency on the stack, but we should still
                    # run it soon, because doing so may free its parent.
                    singles[dep] = item
                elif key < partition_keys[item]:
                    next_nodes[key].append(deps)
                else:
                    later_nodes[key].append(deps)
            elif len(deps) == 2:
                # We special-case when len(deps) == 2 so that we may place a dep on singles.
                # Otherwise, the logic here is the same as when `len(deps) > 2` below.
                #
                # Let me explain why this is a special case.  If we put the better dependent
                # onto the inner stack, then it's guaranteed to run next.  After it's run,
                # then running the other dependent *may* allow their parent to be freed.
                dep, dep2 = deps
                key = partition_keys[dep]
                key2 = partition_keys[dep2]
                if (
                    key2 < key
                    or key == key2
                    and dependents_key(dep2) < dependents_key(dep)
                ):
                    dep, dep2 = dep2, dep
                    key, key2 = key2, key
                if inner_stack:
                    prev_key = partition_keys[inner_stack[0]]
                    if key2 < prev_key:
                        inner_stacks_append(inner_stack)
                        inner_stacks_append([dep2])
                        inner_stack = [dep]
                        inner_stack_pop = inner_stack.pop
                        seen_update(deps)
                        if not num_needed[dep2]:
                            if process_singles:
                                later_singles_append(dep2)
                            else:
                                singles[dep2] = item
                    elif key < prev_key:
                        inner_stacks_append(inner_stack)
                        inner_stack = [dep]
                        inner_stack_pop = inner_stack.pop
                        seen_add(dep)
                        if not num_needed[dep2]:
                            if process_singles:
                                later_singles_append(dep2)
                            else:
                                singles[dep2] = item
                        elif key2 < partition_keys[item]:
                            next_nodes[key2].append([dep2])
                        else:
                            later_nodes[key2].append([dep2])
                    else:
                        item_key = partition_keys[item]
                        if key2 < item_key:
                            next_nodes[key].append([dep])
                            next_nodes[key2].append([dep2])
                        elif key < item_key:
                            next_nodes[key].append([dep])
                            later_nodes[key2].append([dep2])
                        else:
                            later_nodes[key].append([dep])
                            later_nodes[key2].append([dep2])
                else:
                    if add_to_inner_stack:
                        if not num_needed[dep2]:
                            inner_stacks_append(inner_stack)
                            inner_stack = [dep]
                            inner_stack_pop = inner_stack.pop
                            seen_add(dep)
                            singles[dep2] = item
                        elif key == key2 and 5 * partition_keys[item] > 22 * key:
                            inner_stacks_append(inner_stack)
                            inner_stacks_append([dep2])
                            inner_stack = [dep]
                            inner_stack_pop = inner_stack.pop
                            seen_update(deps)
                        else:
                            inner_stacks_append(inner_stack)
                            inner_stack = [dep]
                            inner_stack_pop = inner_stack.pop
                            seen_add(dep)
                            if key2 < partition_keys[item]:
                                next_nodes[key2].append([dep2])
                            else:
                                later_nodes[key2].append([dep2])
                    else:
                        item_key = partition_keys[item]
                        if key2 < item_key:
                            next_nodes[key].append([dep])
                            next_nodes[key2].append([dep2])
                        elif key < item_key:
                            next_nodes[key].append([dep])
                            later_nodes[key2].append([dep2])
                        else:
                            later_nodes[key].append([dep])
                            later_nodes[key2].append([dep2])
            else:
                # Slow path :(.  This requires grouping by partition_key.
                dep_pools = defaultdict(list)
                for dep in deps:
                    dep_pools[partition_keys[dep]].append(dep)
                item_key = partition_keys[item]
                if inner_stack:
                    # If we have an inner_stack, we need to look for a "better" path
                    prev_key = partition_keys[inner_stack[0]]
                    now_keys = []  # < inner_stack[0]
                    for key, vals in dep_pools.items():
                        if key < prev_key:
                            now_keys.append(key)
                        elif key < item_key:
                            next_nodes[key].append(vals)
                        else:
                            later_nodes[key].append(vals)
                    if now_keys:
                        # Run before `inner_stack` (change tactical goal!)
                        inner_stacks_append(inner_stack)
                        if 1 < len(now_keys):
                            now_keys.sort(reverse=True)
                        for key in now_keys:
                            pool = dep_pools[key]
                            if 1 < len(pool) < 100:
                                pool.sort(key=dependents_key, reverse=True)
                            inner_stacks_extend([dep] for dep in pool)
                            seen_update(pool)
                        inner_stack = inner_stacks_pop()
                        inner_stack_pop = inner_stack.pop
                else:
                    # If we don't have an inner_stack, then we don't need to look
                    # for a "better" path, but we do need traverse along dependents.
                    if add_to_inner_stack:
                        min_key = min(dep_pools)
                        min_pool = dep_pools.pop(min_key)
                        if len(min_pool) == 1:
                            inner_stack = min_pool
                            seen_update(inner_stack)
                        elif (
                            10 * item_key > 11 * len(min_pool) * len(min_pool) * min_key
                        ):
                            # Put all items in min_pool onto inner_stacks.
                            # I know this is a weird comparison.  Hear me out.
                            # Although it is often beneficial to put all of the items in `min_pool`
                            # onto `inner_stacks` to process next, it is very easy to be overzealous.
                            # Sometimes it is actually better to defer until `next_nodes` is handled.
                            # We should only put items onto `inner_stacks` that we're reasonably
                            # confident about.  The above formula is a best effort heuristic given
                            # what we have easily available.  It is obviously very specific to our
                            # choice of partition_key.  Dask tests take this route about 40%.
                            if len(min_pool) < 100:
                                min_pool.sort(key=dependents_key, reverse=True)
                            inner_stacks_extend([dep] for dep in min_pool)
                            inner_stack = inner_stacks_pop()
                            seen_update(min_pool)
                        else:
                            # Put one item in min_pool onto inner_stack and the rest into next_nodes.
                            if len(min_pool) < 100:
                                inner_stack = [min(min_pool, key=dependents_key)]
                            else:
                                inner_stack = [min_pool.pop()]
                            next_nodes[min_key].append(min_pool)
                            seen_update(inner_stack)

                        inner_stack_pop = inner_stack.pop
                    for key, vals in dep_pools.items():
                        if key < item_key:
                            next_nodes[key].append(vals)
                        else:
                            later_nodes[key].append(vals)

        if len(dependencies) == len(result):
            break  # all done!

        if next_nodes:
            for key in sorted(next_nodes, reverse=True):
                # `outer_stacks` may not be empty here--it has data from previous `next_nodes`.
                # Since we pop things off of it (onto `inner_nodes`), this means we handle
                # multiple `next_nodes` in a LIFO manner.
                outer_stack_extend(reversed(next_nodes[key]))
            next_nodes = defaultdict(list)

        while outer_stack:
            # Try to add a few items to `inner_stacks`
            deps = [x for x in outer_stack_pop() if x not in result]
            if deps:
                if 1 < len(deps) < 100:
                    deps.sort(key=dependents_key, reverse=True)
                inner_stacks_extend([dep] for dep in deps)
                seen_update(deps)
                break

        if inner_stacks:
            continue

        if later_nodes:
            # You know all those dependents with large keys we've been hanging onto to run "later"?
            # Well, "later" has finally come.
            next_nodes, later_nodes = later_nodes, next_nodes
            continue

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

        if skip_root_node and item in root_nodes:
            item = init_stack_pop()

        while item in result:
            item = init_stack_pop()
        inner_stack.append(item)

    return result


def graph_metrics(dependencies, dependents, total_dependencies):
    r"""Useful measures of a graph used by ``dask.order.order``

    Example DAG (a1 has no dependencies; b2 and c1 are root nodes):

    c1
    |
    b1  b2
     \  /
      a1

    For each key we return:

    1.  **total_dependents**: The number of keys that can only be run
        after this key is run.  The root nodes have value 1 while deep child
        nodes will have larger values.

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
    current = []
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
    current = []
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

    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
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


def diagnostics(dsk, o=None, dependencies=None):
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

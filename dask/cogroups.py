from __future__ import annotations

import operator
from typing import Hashable, Iterator, TypeVar

from dask.core import reverse_dict

KT = TypeVar("KT", bound=Hashable)


def cogroup(
    priorities: dict[KT, int],
    dependencies: dict[KT, set[KT]],
) -> Iterator[list[KT]]:
    # NOTE: it's possible there are items in `dependencies.values()` that aren't in `priorities`.
    # This can happen when submitting tasks to the scheduler that depend on existing Futures, for example.
    # If a dependency is missing from `priorities`, we assume it's highest-priority (-1).

    assert priorities.keys() == dependencies.keys(), (
        priorities.keys() - dependencies.keys(),
        dependencies.keys() - priorities.keys(),
    )

    dependents: dict[KT, set[KT]] = reverse_dict(dependencies)
    kps = sorted(priorities.items(), key=operator.itemgetter(1))
    # ^ can't `zip(*sorted...)` because of mypy: https://github.com/python/mypy/issues/5247
    keys = [kp[0] for kp in kps]
    prios = [kp[1] for kp in kps]
    del kps

    # Assume priorities are consecutive, starting from 0.
    # This makes priorities and indices interchangeable: `keys[i]` has priority `i`.
    assert all(p == i for p, i in zip(prios, range(len(keys)))), prios
    del prios

    i = 0
    while i < len(keys):
        start_i = prev_i = i
        key = keys[i]

        # Walk linear chains of consecutive priority, either until we hit a priority jump,
        # or a task with dependencies that are outside of our group.

        while not any(
            # If an input comes from a different cogroup, and it's only
            # used in this group, don't walk past it.
            priorities.get(dk, -1) < start_i and len(dependents[dk]) == 1
            for dk in dependencies[key]
        ) and (downstream := dependents[key]):
            key = min(downstream, key=priorities.__getitem__)
            prev_i = i
            i = priorities[key]

            if i != prev_i + 1:
                # non-consecutive priority jump. this is our max node.

                # Check if we've jumped over a fully disjoint part of the graph.
                # `dask.order` does weird things sometimes; this can happen.

                if keys[i - 1] not in dependencies[key]:
                    # If the key 1 before the max node isn't an input to the max node,
                    # we must have jumped over a disjoint part of the graph.
                    # Don't eat it; roll back and just take the linear chain.
                    i = prev_i
                    break

                # Try to walk up the graph, from the key 1 after where we jumped from.
                # If we can't reach the apex node from there, we've jumped a disjoint subgraph.
                # TODO not a full search, just following the biggest priority leaps assuming they'll
                # get us there faster. This may be wrong??
                di = prev_i + 1
                while dts := dependents[keys[di]]:
                    di = priorities[max(dts, key=priorities.__getitem__)]
                    if di >= i:
                        # TODO `>`?? might also be a bad sign.
                        break
                else:
                    # Reached an output node (no dependents) that's not our apex node.
                    # `[prev_i:i]` includes a disjoint subgraph, so roll back.
                    i = prev_i

                break

            # TODO if we reach the top without a jump, try again from the start with
            # the next-smallest dependent

        # all tasks from the start to the current (inclusive) belong to the cogroup.
        i = i + 1

        # If all inputs are from existing groups, this isn't a real co-group. Note that
        # if there are no dependencies, this will be False. That's ok: if a task has no
        # deps, but we weren't able to traverse past it, it would be a group of size 1.
        if any(priorities.get(dk, -1) >= start_i for dk in dependencies[key]):
            yield keys[start_i:i]

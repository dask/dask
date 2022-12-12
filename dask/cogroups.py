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
                if not _path_exists(
                    prev_i + 1, i, keys, priorities, dependents, seen=set()
                ):
                    # Don't eat disjoint graph; roll back and take the linear chain.
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


def _path_exists(
    start_i: int,
    end_i: int,
    keys: list[KT],
    priorities: dict[KT, int],
    dependents: dict[KT, set[KT]],
    seen: set[int],
):
    # TODO use stack instead of recursion
    assert start_i not in seen, (start_i, seen)
    assert start_i < end_i, (start_i, end_i)

    for dk in dependents[keys[start_i]]:
        di = priorities[dk]
        if di not in seen:
            if di == end_i:
                return True
            if di < end_i and _path_exists(
                di, end_i, keys, priorities, dependents, seen
            ):
                return True

            seen.add(di)

    return False

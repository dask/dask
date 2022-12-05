from __future__ import annotations

import operator
from typing import Hashable, Iterator, TypeVar

from dask.core import reverse_dict

KT = TypeVar("KT", bound=Hashable)


def cogroup(
    priorities: dict[KT, int],
    dependencies: dict[KT, set[KT]],
) -> Iterator[list[KT]]:
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
        while downstream := dependents[key]:
            key = min(downstream, key=priorities.__getitem__)
            i = priorities[key]

            if (
                # linear chain
                i == prev_i + 1
                # If an input comes from a different cogroup, and it's only
                # used in this group, don't walk past it.
                and not any(
                    priorities[dk] < start_i and len(dependents[dk]) == 1
                    for dk in dependencies[key]
                )
            ):
                # walk up the linear chain
                # TODO if we reach the top without a jump, try again from the start with
                # the next-smallest dependent
                prev_i = i
            else:
                # non-consecutive priority jump. this is our max node.

                # check if we've jumped over a fully disjoint part of the graph
                if keys[i - 1] not in dependencies[key]:
                    # If we've jumped over a disjoint subgraph, don't eat it.
                    # Roll back and just take the linear chain.
                    i = prev_i

                break

        # all tasks from the start to the current (inclusive) belong to the cogroup.
        i = i + 1

        # If all inputs are from existing groups, this isn't a real co-group. Note that
        # if there are no dependencies, this will be False. That's ok: if a task has no
        # deps, but we weren't able to traverse past it, it would be a group of size 1.
        if any(priorities[dk] >= start_i for dk in dependencies[key]):
            yield keys[start_i:i]

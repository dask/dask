from __future__ import annotations

import operator
from typing import Hashable, Iterator

from dask.core import reverse_dict


def cogroup(
    priorities: dict[Hashable, int],
    dependencies: dict[Hashable, set[Hashable]],
) -> Iterator[tuple[list[Hashable], bool]]:
    dependents: dict[Hashable, set[Hashable]] = reverse_dict(dependencies)
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
        isolated_cogroup: bool = False

        # Walk linear chains of consecutive priority, either until we hit a priority jump,
        # or a task with dependencies that are outside of our group.
        while downstream := dependents[key]:
            key = min(downstream, key=priorities.__getitem__)
            i = priorities[key]

            if (
                # linear chain
                (was_chain := (i == prev_i + 1))
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
                if keys[i - 1] in dependencies[key]:
                    # Seems connected

                    if not was_chain:
                        # ended up in this branch because `was_chain` was false, not because
                        # inputs belonged to a different cogroup. so this is an isolated cogroup
                        # because it doesn't need to consider the location of any inputs.
                        isolated_cogroup = True
                        assert i > start_i + 1, (
                            i,
                            start_i,
                            key,
                        )
                else:
                    # If we've jumped over a disjoint subgraph, don't eat it.
                    # Roll back and just take the linear chain.
                    i = prev_i

                break

        # all tasks from the start to the current (inclusive) belong to the cogroup.
        i = i + 1
        yield keys[start_i:i], isolated_cogroup

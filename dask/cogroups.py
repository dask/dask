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
                # TODO update this comment:
                # where inputs don't already belong to a different cogroup.
                # If a task has multiple families as inputs, we don't know how to group
                # it yet---that's a `decide_worker` choice based on data size at
                # runtime. This prevents multi-stage fan-in tasks from "eating" the
                # whole graph.
                # TODO can we just more broadly say linear chains have 1 dependency?
                # IDEA: if it depends on exactly 1 other cogroup, then roll this cogroup
                # into that previous cogroup?
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

                break

        # all tasks from the start to the current (inclusive) belong to the cogroup.
        i = i + 1
        yield keys[start_i:i], isolated_cogroup

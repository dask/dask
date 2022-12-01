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

    # Assume priorities are consecutive, starting from 0
    assert all(kp[1] == i for kp, i in zip(kps, range(len(kps)))), kps

    cogroup_start_i = 0
    while cogroup_start_i < len(kps):
        key, current_prio = kps[cogroup_start_i]
        prev_prio = start_prio = current_prio
        assert start_prio == cogroup_start_i, (start_prio, cogroup_start_i)
        # TODO `start_prio` and `cogroup_start_i` are redundant if prios are consecutive from 0
        isolated_cogroup: bool = False

        # Walk linear chains of consecutive priority, either until we hit a priority jump,
        # or a task with dependencies that are outside of our group.
        while downstream := dependents[key]:
            key = min(downstream, key=priorities.__getitem__)
            current_prio = priorities[key]

            if (
                # linear chain
                (was_chain := (current_prio == prev_prio + 1))
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
                    priorities[dk] < start_prio and len(dependents[dk]) == 1
                    for dk in dependencies[key]
                )
            ):
                # TODO if this chain is all linear, try again from the start with the next-smallest dependent
                prev_prio = current_prio
            else:
                # non-consecutive priority jump. this is our max node.

                if not was_chain:
                    # ended up in this branch because `was_chain` was false, not because
                    # inputs belonged to a different cogroup. so this is an isolated cogroup
                    # because it doesn't need to consider the location of any inputs.
                    isolated_cogroup = True
                    assert current_prio > start_prio + 1, (
                        current_prio,
                        start_prio,
                        key,
                    )

                break

        # all tasks from the start to the max (inclusive) belong to the cogroup.
        next_start_i = cogroup_start_i + (current_prio - start_prio) + 1
        yield [kp[0] for kp in kps[cogroup_start_i:next_start_i]], isolated_cogroup
        cogroup_start_i = next_start_i

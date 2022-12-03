from __future__ import annotations

import operator
from bisect import bisect_right
from typing import Hashable, Iterator, NewType, TypeVar

import dask
from dask.core import reverse_dict
from dask.delayed import Delayed
from dask.order import graph_metrics, ndependencies, order

KT = TypeVar("KT", bound=Hashable)


def cogroup(
    priorities: dict[KT, int],
    dependencies: dict[KT, set[KT]],
    max_chain: int | None = None,
) -> Iterator[tuple[list[KT], bool]]:
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
        chain_len = 0
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
                chain_len += 1
                if max_chain is not None and chain_len == max_chain:
                    break
            else:
                # non-consecutive priority jump. this is our max node.

                # check if we've jumped over a fully disjoint part of the graph
                if keys[i - 1] in dependencies[key]:
                    # Seems connected

                    if not was_chain:
                        # ended up in this branch because `was_chain` was false, not because
                        # inputs belonged to a different cogroup or we maxed out the chain.
                        # so this is an isolated cogroup because it doesn't need to consider
                        # the location of any inputs.
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


CogroupID = NewType("CogroupID", int)


def f(*args):
    pass


def _cogroup_recursive(
    *,
    priorities: dict[KT, int],
    dependencies: dict[KT, set[KT]],
    prev_n_groups: int | None,
    min_groups: int | None,
    max_chain: int | None,
    depth: int,
) -> tuple[list[tuple[list[KT], bool]], dict[CogroupID, set[CogroupID]]] | None:

    dsk = {k: (f, *deps) for k, deps in dependencies.items()}
    dask.visualize(
        [Delayed(k, dsk) for k in dsk],
        filename=f"cogroup-l{depth}.png",
        color="cogroup-nonrec",
        optimize_graph=False,
        collapse_outputs=True,
        max_chain=max_chain,
    )

    groups: list[tuple[list[KT], bool]] = []
    group_deps: dict[CogroupID, set[CogroupID]] = {}
    group_end_idxs: list[int] = []

    def cogroup_of(key: KT) -> CogroupID:
        return CogroupID(bisect_right(group_end_idxs, priorities[key]))

    for group_id, (keys, isolated) in enumerate(
        cogroup(priorities, dependencies, max_chain)
    ):
        deps: set[CogroupID]
        group_deps[CogroupID(group_id)] = deps = set()
        groups.append((keys, isolated))
        group_end_idxs.append((group_end_idxs[-1] if group_end_idxs else 0) + len(keys))
        assert group_end_idxs[-1] == priorities[keys[-1]] + 1

        for key in keys:
            deps.update(
                dg for dk in dependencies[key] if (dg := cogroup_of(dk)) != group_id
            )

    # `cogroup` guarantees keys are in continuous priority order from 0
    assert (ks := [k for (g, i) in groups for k in g]) == (
        s := sorted(ks, key=priorities.__getitem__)
    ), (ks, s)
    assert (kps := [priorities[k] for k in ks]) == list(range(len(ks))), kps

    if (prev_n_groups and len(groups) == prev_n_groups) or (
        min_groups and len(groups) <= min_groups
    ):
        # Terminal case: no more change, or we've collapsed too much.
        return None

    if min_groups is None:
        # Calculate the number of output groups (groups with no dependents) at depth 0.
        # We shouldn't consolidate more than this (otherwise unrelated outputs be getting
        # bundled together).
        min_groups = sum(len(d) == 0 for d in reverse_dict(group_deps).values())

    if max_chain is None:
        dependents = reverse_dict(dependencies)
        num_needed, total_dependencies = ndependencies(dependencies, dependents)
        metrics = graph_metrics(dependencies, dependents, total_dependencies)

        # item 5: **max_height**: The maximum height from a root node
        # TODO don't generalize over the whole graph; different sections might have
        # different heights.
        max_chain = max(m[4] for m in metrics.values()) - 1

    # HACK: `order` doesn't actually care if `dsk` is a dask; just uses it to calculate dependencies.
    # if you don't pass them in. We can pass any collection as long as it's the right length.
    new_order = order(group_deps, dependencies=group_deps)

    assert min_groups is not None
    assert max_chain is not None
    if r := _cogroup_recursive(
        priorities=new_order,
        dependencies=group_deps,
        prev_n_groups=len(groups),
        min_groups=min_groups,
        max_chain=max_chain - 1,
        depth=depth + 1,
    ):
        # Recursive case: a subsequent co-grouping reduced the number of groups without collapsing too much.
        # Translate the keys back, and return it.
        # The new groups can only be supersets of the old ones, so this just means grouping the old keys
        # by the new group boundaries
        new_groups, new_group_deps = r

        assert len(new_groups) <= len(groups)

        translated_groups = [
            (
                [k for gid in sorted(group) for k in groups[gid][0]],
                any(groups[gid][1] for gid in group),
            )
            for (group, isolated) in new_groups
        ]

        assert len(translated_groups) == len(new_group_deps)
        # After translating, keys are _not_ guaranteed to be in priority order any more??

        # assert ks == (nks := [k for (g, i) in translated_groups for k in g]), (ks, nks)
        # assert (kps := [priorities[k] for k in ks]) == list(range(len(ks))), kps

        return translated_groups, new_group_deps

    # Recursing went too far. Use what we have.
    return groups, group_deps


def cogroup_recursive(
    priorities: dict[KT, int],
    dependencies: dict[KT, set[KT]],
) -> list[tuple[list[KT], bool]]:
    r = _cogroup_recursive(
        priorities=priorities,
        dependencies=dependencies,
        prev_n_groups=None,
        min_groups=None,
        max_chain=None,
        depth=0,
    )
    assert r
    return r[0]

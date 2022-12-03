from __future__ import annotations

from typing import Hashable, Iterable

import pytest
from tlz import partition_all

import dask
from dask.base import collections_to_dsk, get_dependencies, tokenize
from dask.cogroups import cogroup_recursive
from dask.delayed import Delayed
from dask.order import order


@pytest.fixture(params=["abcde"])
def abcde(request):
    return request.param


def f(*args):
    return None


df = dask.delayed(f, pure=True)


def tsk(name: str, *args):
    "Syntactic sugar for calling dummy delayed function"
    return df(*args, dask_key_name=name)


def tskchain(input, *names):
    token = tokenize(input, names)[:6]
    out = input
    for i, name in enumerate(names):
        if name is _:
            name = (i, token)
        out = tsk(name, out)
    return out


def get_cogroups(
    xs: Delayed | list[Delayed],
) -> tuple[list[tuple[list[Hashable], bool]], dict[Hashable, int]]:
    if not isinstance(xs, list):
        xs = [xs]

    dask.visualize(xs, color="cogroup", optimize_graph=False, collapse_outputs=True)

    dsk = collections_to_dsk(xs, optimize_graph=False)
    dependencies = {k: get_dependencies(dsk, k) for k in dsk}

    priorities: dict[Hashable, int] = order(dsk, dependencies=dependencies)

    cogroups = cogroup_recursive(priorities, dependencies)

    return cogroups, priorities


def cogroup_to_dict(
    cogroups: Iterable[tuple[list[Hashable], bool]]
) -> dict[Hashable, tuple[int, bool]]:
    return {
        k: (i, isolated) for i, (keys, isolated) in enumerate(cogroups) for k in keys
    }


_ = "_"


def assert_cogroup(
    cogroup: tuple[list[Hashable], bool],
    pattern: list[Hashable],
    *,
    isolated: bool,
) -> None:
    keys, is_isolated = cogroup
    assert is_isolated == isolated
    assert len(keys) == len(pattern)
    for k, p in zip(keys, pattern):
        if p != "_":
            assert k == p


def assert_cogroup_priority_range(
    cogroup: tuple[list[Hashable], bool],
    start: int,
    stop_inclusive: int,
    priorities: dict[Hashable, int],
    *,
    isolated: bool,
) -> None:
    keys, is_isolated = cogroup
    assert is_isolated == isolated
    assert [priorities[k] for k in keys] == list(range(start, stop_inclusive + 1))


def test_two_step_reduction(abcde):
    r"""
                  final
              /     |      \
          e         e         e
        /  |      /  |      /  |
      d    |    d    |    d    |
     / \   |   / \   |   / \   |
    a   b  c  a   b  c  a   b  c
    """
    a, b, c, d, e = abcde

    # TODO parametrize to also be widely-shared
    ats = [tsk((a, i)) for i in range(3)]
    bts = [tsk((b, i)) for i in range(3)]
    cts = [tsk((c, i)) for i in range(3)]

    ets = [
        tsk((e, i), tsk((d, i), at, bt), ct)
        for i, (at, bt, ct) in enumerate(zip(ats, bts, cts))
    ]
    final = tsk("final", *ets)

    cogroups, prios = get_cogroups(final)

    assert len(cogroups) == 4

    for i in range(len(ats)):
        assert_cogroup(
            cogroups[i], [(a, i), (b, i), (d, i), (c, i), (e, i)], isolated=True
        )

    assert_cogroup(cogroups[-1], ["final"], isolated=False)


def test_two_step_reduction_linear_chains(abcde):
    r"""
                          final
                     /              \               \
                    /                \               \
           ---------------
           |     s2      |            s2              s2
           |  /      \   |          /     \         /     \
           | /        \  |         |       |       |       |
     -------           | |         |       |       |       |
     |     s1          | |         s1      |       s1      |
     | /   |  \        | |     /   |  \    |   /   |  \    |
     | x   |   |       | |     x   |   |   |   x   |   |   |
     | |   |   |       | |     |   |   |   |   |   |   |   |
     | x   |   x       x |     x   |   x   x   x   |   x   x
     | |   |   |       | |     |   |   |   |   |   |   |   |
     | a   b   c       d |     a   b   c   d   a   b   c   d
     ---------------------    /   /   /  /   /   /    /   /
       \   \   \   \   \  \  /   /   /  /   /   /    /   /
                            r
    """
    a, b, c, d, e = abcde
    root = tsk(name="root")
    ats = [tskchain(root, (a, i), _, _) for i in range(3)]  # 2-chain(z)
    bts = [tskchain(root, (b, i)) for i in range(3)]  # 0-chain
    cts = [tskchain(root, (c, i), _) for i in range(3)]  # 1-chain

    s1ts = [
        tsk(("s1", i), at, bt, ct) for i, (at, bt, ct) in enumerate(zip(ats, bts, cts))
    ]

    dts = [tskchain(root, (d, i), ("dx1", i)) for i in range(3)]  # 1-chain
    s2ts = [tsk(("s2", i), s1t, dt) for i, (s1t, dt) in enumerate(zip(s1ts, dts))]

    final = tsk("final", *s2ts)

    cogroups, prios = get_cogroups(final)

    assert len(cogroups) == 4

    assert_cogroup(
        cogroups[0],
        # TODO ideally `root` wouldn't be in there
        ["root", (a, 0), _, _, (c, 0), _, (b, 0), ("s1", 0), (d, 0), _, ("s2", 0)],
        isolated=True,
    )

    assert_cogroup(
        cogroups[1],
        [(a, 1), _, _, (c, 1), _, (b, 1), ("s1", 1), (d, 1), _, ("s2", 1)],
        isolated=True,
    )

    assert_cogroup(
        cogroups[2],
        [(a, 2), _, _, (c, 2), _, (b, 2), ("s1", 2), (d, 2), _, ("s2", 2)],
        isolated=True,
    )

    assert_cogroup(cogroups[3], ["final"], isolated=False)


@pytest.mark.xfail(reason="widely shared dependencies mess everything up")
def test_cogroup_linear_chains_plus_widely_shared(abcde):
    r"""
      c     c    c
     /|\   /|\   /\
    b b b b b b b b
    |\|\|\|\|/|/|/|
    | | | | s | | |
    a a a a a a a a
    """
    a, b, c, d, e = abcde
    shared = tsk("shared")
    roots = [tsk((a, i)) for i in range(9)]
    bts = [tsk((b, i), r, shared) for i, r in enumerate(roots)]
    cts = [tsk((c, i), axs) for i, axs in enumerate(partition_all(3, bts))]

    cogroups, prios = get_cogroups(cts)
    assert len(cogroups) == 4

    assert_cogroup(cogroups[0], ["shared"], isolated=False)
    assert_cogroup(
        cogroups[1],
        [(a, 0), (b, 0), (a, 1), (b, 1), (a, 2), (b, 2), (c, 0)],
        isolated=True,
    )
    assert_cogroup(
        cogroups[2],
        [(a, 3), (b, 3), (a, 4), (b, 4), (a, 5), (b, 5), (c, 1)],
        isolated=True,
    )
    assert_cogroup(
        cogroups[3],
        [(a, 6), (b, 6), (a, 7), (b, 7), (c, 3)],
        isolated=True,
    )


# # @gen_cluster(nthreads=[], client=True)
# # async def test_cogroup_triangle(c, s):
# #     r"""
# #       z
# #      /|
# #     y |
# #     \ |
# #       x
# #     """
# #     x = dask.delayed(0, name="x")
# #     y = inc(x, dask_key_name="y")
# #     z = add(x, y, dask_key_name="z")

# #     futs = await submit_delayed(c, s, z)

# #     x = s.tasks["x"]
# #     y = s.tasks["y"]
# #     z = s.tasks["z"]

# #     cogroup = cogroup(x, 1000, 1000)
# #     assert cogroup
# #     sibs, downstream = cogroup
# #     assert sibs == set()
# #     assert downstream == {z}  # `y` is just a linear chain, not downstream

# #     cogroup = cogroup(y, 1000, 1000)
# #     assert cogroup
# #     sibs, downstream = cogroup
# #     assert sibs == {x}
# #     assert downstream == {z}


# # @gen_cluster(nthreads=[], client=True)
# # async def test_cogroup_wide_gather_downstream(c, s):
# #     r"""
# #             s
# #      / / / /|\ \ \
# #     i i i i i i i i
# #     | | | | | | | |
# #     r r r r r r r r
# #     """
# #     roots = [dask.delayed(i, name=f"r-{i}") for i in range(8)]
# #     incs = [inc(r, dask_key_name=f"i-{i}") for i, r in enumerate(roots)]
# #     sum = dsum(incs, dask_key_name="sum")

# #     futs = await submit_delayed(c, s, sum)

# #     rts = [s.tasks[r.key] for r in roots]
# #     sts = s.tasks["sum"]

# #     cogroup = cogroup(rts[0], 4, 1000)
# #     assert cogroup
# #     sibs, downstream = cogroup
# #     assert sibs == set()
# #     assert downstream == set()  # `sum` not downstream because it's too large

# #     cogroup = cogroup(rts[0], 1000, 1000)
# #     assert cogroup
# #     sibs, downstream = cogroup
# #     assert sibs == set(rts[1:])
# #     assert downstream == {sts}


# # # TODO test cogroup commutativity. Given any node X in any graph, calculate `cogroup(X)`.
# # # For each sibling S, `cogroup(S)` should give the same cogroup, regardless of the
# # # starting node.
# # # EXECPT THIS ISN'T TRUE


# # @gen_cluster(nthreads=[], client=True)
# # async def test_cogroup_non_commutative(c, s):
# #     roots = [dask.delayed(i, name=f"r-{i}") for i in range(16)]
# #     aggs = [dsum(rs) for rs in partition(4, roots)]
# #     extra = dsum([roots[::4]], dask_key_name="extra")

# #     futs = await submit_delayed(c, s, aggs + [extra])

# #     rts = [s.tasks[r.key] for r in roots]
# #     ats = [s.tasks[a.key] for a in aggs]
# #     ets = s.tasks["extra"]

# #     cogroup = cogroup(rts[0], 1000, 1000)
# #     assert cogroup
# #     sibs, downstream = cogroup
# #     assert sibs == set(rts[1:4]) | {rts[4], rts[8], rts[12]}
# #     assert downstream == {ats[0], ets}

# #     cogroup = cogroup(rts[1], 1000, 1000)
# #     assert cogroup
# #     sibs, downstream = cogroup
# #     assert sibs == {rts[0], rts[2], rts[3]}
# #     assert downstream == {ats[0]}


# # @gen_cluster(nthreads=[], client=True)
# # async def test_reuse(c, s):
# #     r"""
# #     a + (a + 1).mean()
# #     """
# #     roots = [dask.delayed(i, name=f"r-{i}") for i in range(6)]
# #     incs = [inc(r, name=f"i-{i}") for i, r in enumerate(roots)]
# #     mean = dsum([dsum(incs[:3]), dsum(incs[3:])])
# #     deltas = [add(r, mean, dask_key_name=f"d-{i}") for i, r in enumerate(roots)]

# #     futs = await submit_delayed(c, s, deltas)


# # @gen_cluster(nthreads=[], client=True)
# # async def test_common_with_trees(c, s):
# #     r"""
# #      x       x        x      x
# #      /|\    /|\      /|\    /|\
# #     a | b  c | d    e | f  g | h
# #       |      |        |      |
# #        ---------- c ----------
# #     """
# #     pass


# # @gen_cluster(nthreads=[], client=True)
# # async def test_zigzag(c, s):
# #     r"""
# #     x  x  x  x
# #     | /| /| /|
# #     r  r  r  r
# #     """
# #     roots = [dask.delayed(i, name=f"r-{i}") for i in range(4)]
# #     others = [
# #         inc(roots[0]),
# #         add(roots[0], roots[1]),
# #         add(roots[1], roots[2]),
# #         add(roots[2], roots[3]),
# #     ]

# #     futs = await submit_delayed(c, s, others)


# # @gen_cluster(nthreads=[], client=True)
# # async def test_overlap(c, s):
# #     r"""
# #     x  x  x  x
# #     |\/|\/|\/|
# #     |/\|/\|/\|
# #     r  r  r  r
# #     """
# #     roots = [dask.delayed(i, name=f"r-{i}") for i in range(4)]
# #     others = [
# #         add(roots[0], roots[1]),
# #         dsum(roots[0], roots[1], roots[2]),
# #         dsum(roots[1], roots[2], roots[3]),
# #         add(roots[2], roots[3]),
# #     ]

# #     futs = await submit_delayed(c, s, others)


def test_tree_reduce(abcde):
    r"""
                c
          /     |      \
       b        b        b
     / | \    / | \    / | \
    a  a  a  a  a  a  a  a  a
    """
    a, b, c, _, _ = abcde
    a1, a2, a3, a4, a5, a6, a7, a8, a9 = (a + i for i in "123456789")
    b1, b2, b3, b4 = (b + i for i in "1234")
    dsk = {
        a1: (f,),
        a2: (f,),
        a3: (f,),
        b1: (f, a1, a2, a3),
        a4: (f,),
        a5: (f,),
        a6: (f,),
        b2: (f, a4, a5, a6),
        a7: (f,),
        a8: (f,),
        a9: (f,),
        b3: (f, a7, a8, a9),
        c: (f, b1, b2, b3),
    }

    cogroups, prios = get_cogroups(Delayed(c, dsk))

    assert len(cogroups) == 4

    assert_cogroup(cogroups[0], [a1, a2, a3, b1], isolated=True)
    assert_cogroup(cogroups[1], [a4, a5, a6, b2], isolated=True)
    assert_cogroup(cogroups[2], [a7, a8, a9, b3], isolated=True)
    assert_cogroup(cogroups[3], [c], isolated=False)


# TODO not sure what the best behavior here is?
def test_nearest_neighbor(abcde):
    r"""
    a1  a2  a3  a4  a5  a6  a7 a8  a9
     \  |  /  \ |  /  \ |  / \ |  /
        b1      b2      b3     b4

    No co-groups
    """
    a, b, c, _, _ = abcde
    a1, a2, a3, a4, a5, a6, a7, a8, a9 = (a + i for i in "123456789")
    b1, b2, b3, b4 = (b + i for i in "1234")

    dsk = {
        b1: (f,),
        b2: (f,),
        b3: (f,),
        b4: (f,),
        a1: (f, b1),
        a2: (f, b1),
        a3: (f, b1, b2),
        a4: (f, b2),
        a5: (f, b2, b3),
        a6: (f, b3),
        a7: (f, b3, b4),
        a8: (f, b4),
        a9: (f, b4),
    }

    cogroups, prios = get_cogroups([Delayed(k, dsk) for k in dsk])

    # no isolated cogroups
    assert len([f for f in cogroups if f[1]]) == 0


# @gen_cluster(nthreads=[], client=True)
# async def test_deep_bases_win_over_dependents(client, s, abcde):
#     r"""
#     It's not clear who should run first, e or d

#     1.  d is nicer because it exposes parallelism
#     2.  e is nicer (hypothetically) because it will be sooner released
#         (though in this case we need d to run first regardless)

#     Regardless of e or d first, we should run b before c.

#             a
#           / | \   .
#          b  c |
#         / \ | /
#        e    d
#     """
#     a, b, c, d, e = abcde
#     dsk = {a: (f, b, c, d), b: (f, d, e), c: (f, d), d: (f,), e: (f,)}

#     futs = await submit_delayed(client, s, Delayed(a, dsk))
#     cogroups = list(cogroup(s.tasks.values()))

#     assert len([f for f in cogroups if f[1]]) == 1
#     assert_cogroup(cogroups[0], [e, d, b], isolated=True, scheduler=s)


@pytest.mark.xfail(reason="widely-shared dep, plus idk how this should even look")
def test_base_of_reduce_preferred(abcde):
    r"""
             a3
            /|
          a2 |
         /|  |
       a1 |  |
      /|  |  |
    a0 |  |  |
    |  |  |  |
    b0 b1 b2 b3
      \ \ / /
         c

    """
    a, b, c, d, e = abcde
    dsk = {(a, i): (f, (a, i - 1), (b, i)) for i in [1, 2, 3]}
    dsk[(a, 0)] = (f, (b, 0))
    dsk.update({(b, i): (f, c) for i in [0, 1, 2, 3]})
    dsk[c] = (f,)

    cogroups, prios = get_cogroups(Delayed((a, 3), dsk))

    assert len(cogroups) == 2
    assert_cogroup(
        cogroups[0],
        [
            c,
            (b, 0),
            (a, 0),
            (b, 1),
            (a, 1),
        ],
        isolated=True,
    )
    assert_cogroup(
        cogroups[1],
        [
            c,
            (b, 2),
            (a, 2),
            (b, 3),
            (a, 3),
        ],
        isolated=True,
    )


# TODO not sure what the best behavior here is?
def test_map_overlap(abcde):
    r"""
      b1      b3      b5
       |\    / | \  / |
      c1  c2  c3  c4  c5
       |/  | \ | / | \|
      d1  d2  d3  d4  d5
       |       |      |
      e1      e3      e5

    Want to finish b1 before we start on e5
    """
    a, b, c, d, e = abcde
    dsk = {
        (e, 1): (f,),
        (d, 1): (f, (e, 1)),
        (c, 1): (f, (d, 1)),
        (b, 1): (f, (c, 1), (c, 2)),
        (d, 2): (f,),
        (c, 2): (f, (d, 1), (d, 2), (d, 3)),
        (e, 3): (f,),
        (d, 3): (f, (e, 3)),
        (c, 3): (f, (d, 3)),
        (b, 3): (f, (c, 2), (c, 3), (c, 4)),
        (d, 4): (f,),
        (c, 4): (f, (d, 3), (d, 4), (d, 5)),
        (e, 5): (f,),
        (d, 5): (f, (e, 5)),
        (c, 5): (f, (d, 5)),
        (b, 5): (f, (c, 4), (c, 5)),
        # a: (f, (b, 1), (b, 2), (b, 3)),
        # a: (f, (b, 1), (b, 3), (b, 5)),
    }

    cogroups, prios = get_cogroups([Delayed(k, dsk) for k in dsk])

    assert len([f for f in cogroups if f[1]]) == 2

    assert_cogroup_priority_range(cogroups[0], 0, 7, prios, isolated=True)
    assert_cogroup_priority_range(cogroups[1], 8, 13, prios, isolated=True)


@pytest.mark.parametrize(
    "substructure",
    [
        "linear",
        "sibling",
        "tree-sib",
    ],
)
def test_vorticity(abcde, substructure):
    # See https://gist.github.com/TomNicholas/fe9c6b6c415d4fa42523216c87e2fff2
    # https://github.com/dask/distributed/discussions/7128#discussioncomment-3910328
    a, b, c, d, e = abcde
    d1, d2, d3, d4, d5, d6, d7 = (d + i for i in "1234567")
    e1, e2, e3, e4, e5 = (e + i for i in "12345")

    def _gen_branch(ix):
        if substructure == "linear":
            return {
                f"a{ix}": (f,),
                f"b{ix}": (f, f"a{ix}"),
                f"d{ix}": (f, f"b{ix}"),
            }
        elif substructure == "sibling":
            return {
                f"a{ix}": (f,),
                f"b{ix}": (f, f"a{ix}"),
                f"c{ix}": (f, f"a{ix}"),
                f"d{ix}": (f, f"b{ix}", f"c{ix}", f"z{ix}"),
            }
        elif substructure == "tree-sib":
            return {
                f"a{ix}": (f,),
                f"b{ix}": (f, f"a{ix}"),
                f"c{ix}": (f, f"a{ix}"),
                f"z{ix}": (f, f"a{ix}"),
                f"y{ix}": (f, f"b{ix}", f"c{ix}"),
                f"d{ix}": (f, f"y{ix}", f"z{ix}"),
            }
        raise ValueError(substructure)

    dsk = {}
    for ix in range(1, 12):
        dsk.update(_gen_branch(ix))

    dsk.update(
        {
            e1: (f, d1, d2, d3, d4, d5),
            e2: (f, d3, d4, d5, d6, d7),
            e3: (f, d5, d6, d7, "d8", "d9"),
            e4: (f, d7, "d8", "d9", "d10", "d11"),
        }
    )
    cogroups, prios = get_cogroups([Delayed(k, dsk) for k in dsk])
    grouping = cogroup_to_dict(cogroups)

    # Neighboring towers should be joined, since they feed into a common dependency.
    assert grouping["a1"] == grouping["a2"] == grouping["d1"] == grouping["d2"]
    assert grouping["a3"] == grouping["a4"] == grouping["d3"] == grouping["d4"]
    assert grouping["a6"] == grouping["a7"] == grouping["d6"] == grouping["d7"]
    assert grouping["a8"] == grouping["a9"] == grouping["d8"] == grouping["d9"]

    # Debatable:
    # assert not grouping[e1][1]
    # assert not grouping[e2][1]
    # assert not grouping[e3][1]


def test_actual_map_overlap():
    da = pytest.importorskip("dask.array")
    arr = da.zeros(100, chunks=10)
    # overlap = da.map_overlap(lambda x: x, arr, depth=1, boundary='reflect')
    overlap = da.map_overlap(lambda x: x, arr, depth=1, boundary=0)

    cogroups, prios = get_cogroups(overlap)


def test_actual_shuffle():
    dd = pytest.importorskip("dask.dataframe")
    df = dd.demo.make_timeseries()
    dfs = df.shuffle("id", shuffle="tasks")

    cogroups, prios = get_cogroups(dfs)

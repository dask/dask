import pytest

import dask
from dask.core import get_deps
from dask.order import ndependencies, order
from dask.utils_test import add, inc


@pytest.fixture(params=["abcde", "edcba"])
def abcde(request):
    return request.param


def issorted(L, reverse=False):
    return sorted(L, reverse=reverse) == L


def f(*args):
    pass


def test_ordering_keeps_groups_together(abcde):
    a, b, c, d, e = abcde
    d = dict(((a, i), (f,)) for i in range(4))
    d.update({(b, 0): (f, (a, 0), (a, 1)), (b, 1): (f, (a, 2), (a, 3))})
    o = order(d)

    assert abs(o[(a, 0)] - o[(a, 1)]) == 1
    assert abs(o[(a, 2)] - o[(a, 3)]) == 1

    d = dict(((a, i), (f,)) for i in range(4))
    d.update({(b, 0): (f, (a, 0), (a, 2)), (b, 1): (f, (a, 1), (a, 3))})
    o = order(d)

    assert abs(o[(a, 0)] - o[(a, 2)]) == 1
    assert abs(o[(a, 1)] - o[(a, 3)]) == 1


def test_avoid_broker_nodes(abcde):
    r"""

    b0    b1  b2
    |      \  /
    a0      a1

    a0 should be run before a1
    """
    a, b, c, d, e = abcde
    dsk = {
        (a, 0): (f,),
        (a, 1): (f,),
        (b, 0): (f, (a, 0)),
        (b, 1): (f, (a, 1)),
        (b, 2): (f, (a, 1)),
    }
    o = order(dsk)
    assert o[(a, 0)] < o[(a, 1)]

    # Switch name of 0, 1 to ensure that this isn't due to string comparison
    dsk = {
        (a, 1): (f,),
        (a, 0): (f,),
        (b, 0): (f, (a, 1)),
        (b, 1): (f, (a, 0)),
        (b, 2): (f, (a, 0)),
    }
    o = order(dsk)
    assert o[(a, 0)] > o[(a, 1)]

    # Switch name of 0, 1 for "b"s too
    dsk = {
        (a, 0): (f,),
        (a, 1): (f,),
        (b, 1): (f, (a, 0)),
        (b, 0): (f, (a, 1)),
        (b, 2): (f, (a, 1)),
    }
    o = order(dsk)
    assert o[(a, 0)] < o[(a, 1)]


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

    We really want to run b0 quickly
    """
    a, b, c, d, e = abcde
    dsk = {(a, i): (f, (a, i - 1), (b, i)) for i in [1, 2, 3]}
    dsk[(a, 0)] = (f, (b, 0))
    dsk.update({(b, i): (f, c, 1) for i in [0, 1, 2, 3]})
    dsk[c] = 1

    o = order(dsk)

    assert o[(b, 0)] <= 1
    assert o[(b, 1)] <= 3


@pytest.mark.xfail(reason="Can't please 'em all")
def test_avoid_upwards_branching(abcde):
    r"""
         a1
         |
         a2
         |
         a3    d1
        /  \  /
      b1    c1
      |     |
      b2    c2
            |
            c3

    Prefer b1 over c1 because it won't stick around waiting for d1 to complete
    """
    a, b, c, d, e = abcde
    dsk = {
        (a, 1): (f, (a, 2)),
        (a, 2): (f, (a, 3)),
        (a, 3): (f, (b, 1), (c, 1)),
        (b, 1): (f, (b, 2)),
        (c, 1): (f, (c, 2)),
        (c, 2): (f, (c, 3)),
        (d, 1): (f, (c, 1)),
    }

    o = order(dsk)

    assert o[(b, 1)] < o[(c, 1)]


def test_avoid_upwards_branching_complex(abcde):
    r"""
         a1
         |
    e2   a2  d2  d3
    |    |    \  /
    e1   a3    d1
     \  /  \  /
      b1    c1
      |     |
      b2    c2
            |
            c3

    Prefer c1 over b1 because c1 will stay in memory less long while b1
    computes
    """
    a, b, c, d, e = abcde
    dsk = {
        (a, 1): (f, (a, 2)),
        (a, 2): (f, (a, 3)),
        (a, 3): (f, (b, 1), (c, 1)),
        (b, 1): (f, (b, 2)),
        (b, 2): (f,),
        (c, 1): (f, (c, 2)),
        (c, 2): (f, (c, 3)),
        (c, 3): (f,),
        (d, 1): (f, (c, 1)),
        (d, 2): (f, (d, 1)),
        (d, 3): (f, (d, 1)),
        (e, 1): (f, (b, 1)),
        (e, 2): (f, (e, 1)),
    }

    o = order(dsk)
    assert o[(c, 1)] < o[(b, 1)]
    assert abs(o[(d, 2)] - o[(d, 3)]) == 1


def test_deep_bases_win_over_dependents(abcde):
    r"""
    It's not clear who should run first, e or d

    1.  d is nicer because it exposes parallelism
    2.  e is nicer (hypothetically) because it will be sooner released
        (though in this case we need d to run first regardless)

    Regardless of e or d first, we should run b before c.

            a
          / | \   .
         b  c |
        / \ | /
       e    d
    """
    a, b, c, d, e = abcde
    dsk = {a: (f, b, c, d), b: (f, d, e), c: (f, d), d: 1, e: 2}

    o = order(dsk)
    assert o[e] < o[d]  # ambiguous, but this is what we currently expect
    assert o[b] < o[c]


def test_prefer_deep(abcde):
    """
        c
        |
    e   b
    |   |
    d   a

    Prefer longer chains first so we should start with c
    """
    a, b, c, d, e = abcde
    dsk = {a: 1, b: (f, a), c: (f, b), d: 1, e: (f, d)}

    o = order(dsk)
    assert o[a] < o[d]
    assert o[b] < o[d]


def test_stacklimit(abcde):
    dsk = dict(("x%s" % (i + 1), (inc, "x%s" % i)) for i in range(10000))
    dependencies, dependents = get_deps(dsk)
    ndependencies(dependencies, dependents)


def test_break_ties_by_str(abcde):
    a, b, c, d, e = abcde
    dsk = {("x", i): (inc, i) for i in range(10)}
    x_keys = sorted(dsk)
    dsk["y"] = list(x_keys)

    o = order(dsk)
    expected = {"y": 10}
    expected.update({k: i for i, k in enumerate(x_keys)})

    assert o == expected


def test_order_doesnt_fail_on_mixed_type_keys(abcde):
    order({"x": (inc, 1), ("y", 0): (inc, 2), "z": (add, "x", ("y", 0))})


def test_gh_3055():
    da = pytest.importorskip("dask.array")
    A, B = 20, 99
    orig = x = da.random.normal(size=(A, B), chunks=(1, None))
    for _ in range(2):
        y = (x[:, None, :] * x[:, :, None]).cumsum(axis=0)
        x = x.cumsum(axis=0)
    w = (y * x[:, None]).sum(axis=(1, 2))

    dsk = dict(w.__dask_graph__())
    o = order(dsk)
    L = [o[k] for k in w.__dask_keys__()]
    assert sum(x < len(o) / 2 for x in L) > len(L) / 3  # some complete quickly

    L = [o[k] for kk in orig.__dask_keys__() for k in kk]
    assert sum(x > len(o) / 2 for x in L) > len(L) / 3  # some start later

    assert sorted(L) == L  # operate in order


def test_type_comparisions_ok(abcde):
    a, b, c, d, e = abcde
    dsk = {a: 1, (a, 1): 2, (a, b, 1): 3}
    order(dsk)  # this doesn't err


def test_prefer_short_dependents(abcde):
    r"""

         a
         |
      d  b  e
       \ | /
         c

    Prefer to finish d and e before starting b.  That way c can be released
    during the long computations.
    """
    a, b, c, d, e = abcde
    dsk = {c: (f,), d: (f, c), e: (f, c), b: (f, c), a: (f, b)}

    o = order(dsk)
    assert o[d] < o[b]
    assert o[e] < o[b]


@pytest.mark.xfail(reason="This is challenging to do precisely")
def test_run_smaller_sections(abcde):
    r"""
            aa
           / |
      b   d  bb dd
     / \ /|  | /
    a   c e  cc

    Prefer to run acb first because then we can get that out of the way
    """
    a, b, c, d, e = abcde
    aa, bb, cc, dd = [x * 2 for x in [a, b, c, d]]

    expected = [a, c, b, e, d, cc, bb, aa, dd]

    log = []

    def f(x):
        def _(*args):
            log.append(x)

        return _

    dsk = {
        a: (f(a),),
        c: (f(c),),
        e: (f(e),),
        cc: (f(cc),),
        b: (f(b), a, c),
        d: (f(d), c, e),
        bb: (f(bb), cc),
        aa: (f(aa), d, bb),
        dd: (f(dd), cc),
    }

    dask.get(dsk, [aa, b, dd])  # trigger computation

    assert log == expected


def test_local_parents_of_reduction(abcde):
    """

            c1
            |
        b1  c2
        |  /|
    a1  b2  c3
    |  /|
    a2  b3
    |
    a3

    Prefer to finish a1 stack before proceeding to b2
    """
    a, b, c, d, e = abcde
    a1, a2, a3 = [a + i for i in "123"]
    b1, b2, b3 = [b + i for i in "123"]
    c1, c2, c3 = [c + i for i in "123"]

    expected = [a3, b3, c3, a2, a1, b2, b1, c2, c1]

    log = []

    def f(x):
        def _(*args):
            log.append(x)

        return _

    dsk = {
        a3: (f(a3),),
        a2: (f(a2), a3),
        a1: (f(a1), a2),
        b3: (f(b3),),
        b2: (f(b2), b3, a2),
        b1: (f(b1), b2),
        c3: (f(c3),),
        c2: (f(c2), c3, b2),
        c1: (f(c1), c2),
    }

    order(dsk)
    dask.get(dsk, [a1, b1, c1])  # trigger computation

    assert log == expected


def test_nearest_neighbor(abcde):
    r"""

    a1  a2  a3  a4  a5  a6  a7 a8  a9
     \  |  /  \ |  /  \ |  / \ |  /
        b1      b2      b3     b4

    Want to finish off a local group before moving on.
    This is difficult because all groups are connected.
    """
    a, b, c, _, _ = abcde
    a1, a2, a3, a4, a5, a6, a7, a8, a9 = [a + i for i in "123456789"]
    b1, b2, b3, b4 = [b + i for i in "1234"]

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

    o = order(dsk)

    assert 3 < sum(o[a + i] < len(o) / 2 for i in "123456789") < 7
    assert 1 < sum(o[b + i] < len(o) / 2 for i in "1234") < 4
    assert o[min([b1, b2, b3, b4])] == 0


def test_string_ordering():
    """Prefer ordering tasks by name first"""
    dsk = {("a", 1): (f,), ("a", 2): (f,), ("a", 3): (f,)}
    o = order(dsk)
    assert o == {("a", 1): 0, ("a", 2): 1, ("a", 3): 2}


def test_string_ordering_dependents():
    """Prefer ordering tasks by name first even when in dependencies"""
    dsk = {("a", 1): (f, "b"), ("a", 2): (f, "b"), ("a", 3): (f, "b"), "b": (f,)}
    o = order(dsk)
    assert o == {"b": 0, ("a", 1): 1, ("a", 2): 2, ("a", 3): 3}


def test_prefer_short_narrow(abcde):
    # See test_prefer_short_ancestor for a fail case.
    a, b, c, _, _ = abcde
    dsk = {
        (a, 0): 0,
        (b, 0): 0,
        (c, 0): 0,
        (c, 1): (f, (c, 0), (a, 0), (b, 0)),
        (a, 1): 1,
        (b, 1): 1,
        (c, 2): (f, (c, 1), (a, 1), (b, 1)),
    }
    o = order(dsk)
    assert o[(b, 0)] < o[(b, 1)]
    assert o[(b, 0)] < o[(c, 2)]
    assert o[(c, 1)] < o[(c, 2)]


def test_prefer_short_ancestor(abcde):
    r"""
    From https://github.com/dask/dask-ml/issues/206#issuecomment-395869929

    Two cases, one where chunks of an array are independent, and one where the
    chunks of an array have a shared source. We handled the independent one
    "well" earlier.

    Good:

                    c2
                   / \ \
                  /   \ \
                c1     \ \
              / | \     \ \
            c0  a0 b0   a1 b1

    Bad:

                    c2
                   / \ \
                  /   \ \
                c1     \ \
              / | \     \ \
            c0  a0 b0   a1 b1
                   \ \   / /
                    \ \ / /
                      a-b


    The difference is that all the `a` and `b` tasks now have a common
    ancestor.

    We would like to choose c1 *before* a1, and b1 because

    * we can release a0 and b0 once c1 is done
    * we don't need a1 and b1 to compute c1.
    """
    a, b, c, _, _ = abcde
    ab = a + b

    dsk = {
        ab: 0,
        (a, 0): (f, ab, 0, 0),
        (b, 0): (f, ab, 0, 1),
        (c, 0): 0,
        (c, 1): (f, (c, 0), (a, 0), (b, 0)),
        (a, 1): (f, ab, 1, 0),
        (b, 1): (f, ab, 1, 1),
        (c, 2): (f, (c, 1), (a, 1), (b, 1)),
    }
    o = order(dsk)

    assert o[(a, 0)] < o[(a, 1)]
    assert o[(b, 0)] < o[(b, 1)]
    assert o[(b, 0)] < o[(c, 2)]
    assert o[(c, 1)] < o[(c, 2)]
    assert o[(c, 1)] < o[(a, 1)]


def test_map_overlap(abcde):
    r"""
      b1      b3      b5
       |\    / | \  / |
      c1  c2  c3  c4  c5
       |/  | \ | / | \|
      d1  d2  d3  d4  d5
       |       |      |
      e1      e2      e5

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
    }

    o = order(dsk)

    assert o[(b, 1)] < o[(e, 5)] or o[(b, 5)] < o[(e, 1)]


def test_use_structure_not_keys(abcde):
    """See https://github.com/dask/dask/issues/5584#issuecomment-554963958

    We were using key names to infer structure, which could result in funny behavior.
    """
    a, b, _, _, _ = abcde
    dsk = {
        (a, 0): (f,),
        (a, 1): (f,),
        (a, 2): (f,),
        (a, 3): (f,),
        (a, 4): (f,),
        (a, 5): (f,),
        (a, 6): (f,),
        (a, 7): (f,),
        (a, 8): (f,),
        (a, 9): (f,),
        (b, 5): (f, (a, 2)),
        (b, 7): (f, (a, 0), (a, 2)),
        (b, 9): (f, (a, 7), (a, 0), (a, 2)),
        (b, 1): (f, (a, 4), (a, 7), (a, 0)),
        (b, 2): (f, (a, 9), (a, 4), (a, 7)),
        (b, 4): (f, (a, 6), (a, 9), (a, 4)),
        (b, 3): (f, (a, 5), (a, 6), (a, 9)),
        (b, 8): (f, (a, 1), (a, 5), (a, 6)),
        (b, 6): (f, (a, 8), (a, 1), (a, 5)),
        (b, 0): (f, (a, 3), (a, 8), (a, 1)),
    }
    o = order(dsk)
    As = sorted(val for (letter, _), val in o.items() if letter == a)
    Bs = sorted(val for (letter, _), val in o.items() if letter == b)
    assert Bs[0] in {1, 3}
    if Bs[0] == 3:
        assert As == [0, 1, 2, 4, 6, 8, 10, 12, 14, 16]
        assert Bs == [3, 5, 7, 9, 11, 13, 15, 17, 18, 19]
    else:
        assert As == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        assert Bs == [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]


def test_dont_run_all_dependents_too_early(abcde):
    """From https://github.com/dask/dask-ml/issues/206#issuecomment-395873372"""
    a, b, c, d, e = abcde
    depth = 10
    dsk = {(a, 0): 0, (b, 0): 1, (c, 0): 2, (d, 0): (f, (a, 0), (b, 0), (c, 0))}
    for i in range(1, depth):
        dsk[(b, i)] = (f, (b, 0))
        dsk[(c, i)] = (f, (c, 0))
        dsk[(d, i)] = (f, (d, i - 1), (b, i), (c, i))
    o = order(dsk)
    expected = [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]
    actual = sorted(v for (letter, num), v in o.items() if letter == d)
    assert expected == actual


def test_many_branches_use_ndependencies(abcde):
    """From https://github.com/dask/dask/pull/5646#issuecomment-562700533

    Sometimes we need larger or wider DAGs to test behavior.  This test
    ensures we choose the branch with more work twice in successtion.
    This is important, because ``order`` may search along dependencies
    and then along dependents.

    """
    a, b, c, d, e = abcde
    dd = d + d
    ee = e + e
    dsk = {
        (a, 0): 0,
        (a, 1): (f, (a, 0)),
        (a, 2): (f, (a, 1)),
        (b, 1): (f, (a, 0)),
        (b, 2): (f, (b, 1)),
        (c, 1): (f, (a, 0)),  # most short and thin; should go last
        (d, 1): (f, (a, 0)),
        (d, 2): (f, (d, 1)),
        (dd, 1): (f, (a, 0)),
        (dd, 2): (f, (dd, 1)),
        (dd, 3): (f, (d, 2), (dd, 2)),
        (e, 1): (f, (a, 0)),
        (e, 2): (f, (e, 1)),
        (ee, 1): (f, (a, 0)),
        (ee, 2): (f, (ee, 1)),
        (ee, 3): (f, (e, 2), (ee, 2)),
        (a, 3): (f, (a, 2), (b, 2), (c, 1), (dd, 3), (ee, 3)),
    }
    o = order(dsk)
    # run all d's and e's first
    expected = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    actual = sorted(v for (letter, _), v in o.items() if letter in {d, dd, e, ee})
    assert actual == expected
    assert o[(c, 1)] == o[(a, 3)] - 1


def test_order_cycle():
    with pytest.raises(RuntimeError, match="Cycle detected"):
        dask.get({"a": (f, "a")}, "a")  # we encounter this in `get`
    with pytest.raises(RuntimeError, match="Cycle detected"):
        order({"a": (f, "a")})  # trivial self-loop
    with pytest.raises(RuntimeError, match="Cycle detected"):
        order({("a", 0): (f, ("a", 0))})  # non-string
    with pytest.raises(RuntimeError, match="Cycle detected"):
        order({"a": (f, "b"), "b": (f, "c"), "c": (f, "a")})  # non-trivial loop
    with pytest.raises(RuntimeError, match="Cycle detected"):
        order({"a": (f, "b"), "b": (f, "c"), "c": (f, "a", "d"), "d": 1})
    with pytest.raises(RuntimeError, match="Cycle detected"):
        order({"a": (f, "b"), "b": (f, "c"), "c": (f, "a", "d"), "d": (f, "b")})


def test_order_empty():
    assert order({}) == {}


def test_switching_dependents(abcde):
    r"""

    a7 a8  <-- do these last
    | /
    a6                e6
    |                /
    a5   c5    d5  e5
    |    |    /   /
    a4   c4 d4  e4
    |  \ | /   /
    a3   b3---/
    |
    a2
    |
    a1
    |
    a0  <-- start here

    Test that we are able to switch to better dependents.
    In this graph, we expect to start at a0.  To compute a4, we need to compute b3.
    After computing b3, three "better" paths become available.
    Confirm that we take the better paths before continuing down `a` path.

    This test is pretty specific to how `order` is implemented
    and is intended to increase code coverage.
    """
    a, b, c, d, e = abcde
    dsk = {
        (a, 0): 0,
        (a, 1): (f, (a, 0)),
        (a, 2): (f, (a, 1)),
        (a, 3): (f, (a, 2)),
        (a, 4): (f, (a, 3), (b, 3)),
        (a, 5): (f, (a, 4)),
        (a, 6): (f, (a, 5)),
        (a, 7): (f, (a, 6)),
        (a, 8): (f, (a, 6)),
        (b, 3): 1,
        (c, 4): (f, (b, 3)),
        (c, 5): (f, (c, 4)),
        (d, 4): (f, (b, 3)),
        (d, 5): (f, (d, 4)),
        (e, 4): (f, (b, 3)),
        (e, 5): (f, (e, 4)),
        (e, 6): (f, (e, 5)),
    }
    o = order(dsk)

    assert o[(a, 0)] == 0  # probably
    assert o[(a, 5)] > o[(c, 5)]
    assert o[(a, 5)] > o[(d, 5)]
    assert o[(a, 5)] > o[(e, 6)]


def test_order_with_equal_dependents(abcde):
    """From https://github.com/dask/dask/issues/5859#issuecomment-608422198

    See the visualization of `(maxima, argmax)` example from the above comment.

    This DAG has enough structure to exercise more parts of `order`

    """
    a, b, c, d, e = abcde
    dsk = {}
    abc = [a, b, c, d]
    for x in abc:
        dsk.update(
            {
                (x, 0): 0,
                (x, 1): (f, (x, 0)),
                (x, 2, 0): (f, (x, 0)),
                (x, 2, 1): (f, (x, 1)),
            }
        )
        for i, y in enumerate(abc):
            dsk.update(
                {
                    (x, 3, i): (f, (x, 2, 0), (y, 2, 1)),  # cross x and y
                    (x, 4, i): (f, (x, 3, i)),
                    (x, 5, i, 0): (f, (x, 4, i)),
                    (x, 5, i, 1): (f, (x, 4, i)),
                    (x, 6, i, 0): (f, (x, 5, i, 0)),
                    (x, 6, i, 1): (f, (x, 5, i, 1)),
                }
            )
    o = order(dsk)
    total = 0
    for x in abc:
        for i in range(len(abc)):
            val = o[(x, 6, i, 1)] - o[(x, 6, i, 0)]
            assert val > 0  # ideally, val == 2
            total += val
    assert total <= 110  # ideally, this should be 2 * 16 = 32

    # Add one to the end of the nine bundles
    dsk2 = dict(dsk)
    for x in abc:
        for i in range(len(abc)):
            dsk2[(x, 7, i, 0)] = (f, (x, 6, i, 0))
    o = order(dsk2)
    total = 0
    for x in abc:
        for i in range(len(abc)):
            val = o[(x, 7, i, 0)] - o[(x, 6, i, 1)]
            assert val > 0  # ideally, val == 3
            total += val
    assert total <= 138  # ideally, this should be 3 * 16 == 48

    # Remove one from each of the nine bundles
    dsk3 = dict(dsk)
    for x in abc:
        for i in range(len(abc)):
            del dsk3[(x, 6, i, 1)]
    o = order(dsk3)
    total = 0
    for x in abc:
        for i in range(len(abc)):
            val = o[(x, 6, i, 0)] - o[(x, 5, i, 1)]
            assert val > 0  # ideally, val == 2
            total += val
    assert total <= 98  # ideally, this should be 2 * 16 == 32

    # Remove another one from each of the nine bundles
    dsk4 = dict(dsk3)
    for x in abc:
        for i in range(len(abc)):
            del dsk4[(x, 6, i, 0)]
    o = order(dsk4)
    total = 0
    for x in abc:
        for i in range(len(abc)):
            assert o[(x, 5, i, 1)] - o[(x, 5, i, 0)] == 1


def test_terminal_node_backtrack():
    r"""
    https://github.com/dask/dask/issues/6745

    We have

    1. A terminal node that depends on the entire graph ('s')
    2. Some shared dependencies near the roots ('a1', 'a4')
    3. But the left and right halves are disconnected, other
       than the terminal node.

                       s
               /   /       \   \
              /   /         \   \
            s00  s10       s01  s11
             |    |         |    |
            b00  b10       b01  b11
            / \  / \       / \ / \
           a0  a1  a2    a3  a4  a5

    Previously we started at 'a', and worked up to 's00'. We'd like to finish
    's00' completely, so we progress to 's' and work through its dependencies.

    Ideally, we would choose 's10', since we've already computed one of its
    (eventual) dependencies: 'a1'. However, all of 's00' through 's11' had
    equal metrics so we fell back to the name tie-breaker and started on
    's01' (via 'a3', a4', 'b01', ...).
    """
    dsk = {
        # left half
        ("a", 0): (0,),
        ("a", 1): (1,),
        ("a", 2): (2,),
        ("b", 0): (f, ("a", 0), ("a", 1)),
        ("b", 1): (f, ("a", 1), ("a", 2)),
        ("store", 0, 0): ("b", 0),
        ("store", 1, 0): ("b", 1),
        # right half
        ("a", 3): (3,),
        ("a", 4): (4,),
        ("a", 5): (5,),
        ("b", 2): (f, ("a", 3), ("a", 4)),
        ("b", 3): (f, ("a", 4), ("a", 5)),
        ("store", 0, 1): ("b", 2),
        ("store", 1, 1): ("b", 3),
        "store": (
            f,
            ("store", 0, 0),
            ("store", 1, 0),
            ("store", 0, 1),
            ("store", 1, 1),
        ),
    }
    o = order(dsk)
    assert o[("a", 2)] < o[("a", 3)]


def test_array_store_final_order(tmpdir):
    # https://github.com/dask/dask/issues/6745
    # This essentially tests the same thing as test_terminal_node_backtrack,
    # but with the graph actually generated by da.store.
    da = pytest.importorskip("dask.array")
    zarr = pytest.importorskip("zarr")
    import numpy as np

    arrays = [da.from_array(np.ones((110, 4)), chunks=(100, 2)) for i in range(4)]
    x = da.concatenate(arrays, axis=0).rechunk((100, 2))

    store = zarr.DirectoryStore(tmpdir)
    root = zarr.group(store, overwrite=True)
    dest = root.empty_like(name="dest", data=x, chunks=x.chunksize, overwrite=True)
    d = x.store(dest, lock=False, compute=False)
    o = order(d.dask)

    # Find the lowest store. Dask starts here.
    stores = [k for k in o if isinstance(k, tuple) and k[0].startswith("store-")]
    first_store = min(stores, key=lambda k: o[k])
    first_store
    connected_stores = [k for k in stores if k[-1] == first_store[-1]]
    disconnected_stores = [k for k in stores if k[-1] != first_store[-1]]

    connected_max = max([v for k, v in o.items() if k in connected_stores])
    disconnected_min = min([v for k, v in o.items() if k in disconnected_stores])
    assert connected_max < disconnected_min


def test_eager_to_compute_dependent_to_free_parent():
    r"""https://github.com/dask/dask/pull/7929

    This graph begins with many motifs like the following:

    |      |
    c1    c2
      \ /
       b
       |
       a

    We want to compute c2 and c3 pretty close together, because if we choose to
    compute c1, then we should also compute c2 so we can release b.  Being
    greedy here allows us to release memory sooner and be more globally optimal.
    """
    dsk = {
        "join-ac838b48-7eab-4ed5-96b1-42ca515d6e44": (
            f,
            "finalise-3a20f5c2-a931-4205-aa1c-cc5611eb5309",
            "finalise-ecc61ba8-281c-45cc-9d67-9b395f193c02",
        ),
        "lambda-b1813da8-9490-4c8b-8db3-9ba7b6c20e1c": (
            f,
            "lambda-fcc361be-21a0-46c8-821d-87caa95e8da4",
            "lambda-ad489580-1682-4045-954a-caa2c09b9726",
        ),
        "lambda-fcc361be-21a0-46c8-821d-87caa95e8da4": (
            f,
            "lambda-9e3de884-53b2-4cf7-8f4e-5a721ecea05b",
            "lambda-94aa372f-64c5-43c9-9e26-12d058d84352",
        ),
        "lambda-9e3de884-53b2-4cf7-8f4e-5a721ecea05b": (
            f,
            "lambda-3871d17e-9ba9-4b08-bc83-a08dd21bbbdf",
            "lambda-eed31881-0973-4f9e-9738-6696349d9b74",
        ),
        "lambda-3871d17e-9ba9-4b08-bc83-a08dd21bbbdf": (
            f,
            "compute_energy_time-551d382f-2ce1-4337-aa1f-eb06984ea454",
            "compute_energy_time-43aed8f7-d052-403b-a576-ed92cedf3c66",
        ),
        "read_chunk-183213b3-6c97-4c07-b9d0-072113d315b3": (f, f),
        "normalise_channel-a59c27f2-ed8e-4d6d-8ec8-dab6be36ee03": (
            f,
            "read_chunk-183213b3-6c97-4c07-b9d0-072113d315b3",
        ),
        "lambda-231cf4fa-7694-425d-ab6b-d886f57ed40f": (
            f,
            "normalise_channel-a59c27f2-ed8e-4d6d-8ec8-dab6be36ee03",
        ),
        "compute_energy_time-551d382f-2ce1-4337-aa1f-eb06984ea454": (
            f,
            "lambda-231cf4fa-7694-425d-ab6b-d886f57ed40f",
        ),
        "read_chunk-82a4db55-f4a5-43f5-9426-8c0b72ae9e3e": (f, f),
        "normalise_channel-79384943-c14f-4039-bc33-0702ddfe8c5a": (
            f,
            "read_chunk-82a4db55-f4a5-43f5-9426-8c0b72ae9e3e",
        ),
        "lambda-9f21016b-f3a7-4f81-a836-fd092e011f17": (
            f,
            "normalise_channel-79384943-c14f-4039-bc33-0702ddfe8c5a",
        ),
        "compute_energy_time-43aed8f7-d052-403b-a576-ed92cedf3c66": (
            f,
            "lambda-9f21016b-f3a7-4f81-a836-fd092e011f17",
        ),
        "lambda-eed31881-0973-4f9e-9738-6696349d9b74": (
            f,
            "compute_energy_time-722afbb6-1229-41a5-b10d-43fee0b942a5",
            "compute_energy_time-78f3fbf2-017c-446b-affa-0bf53e5d3534",
        ),
        "read_chunk-08ca9f8f-64c6-45f8-88d5-50a4399e1baf": (f, f),
        "normalise_channel-ac877174-b4e5-4464-879e-9af686c9b630": (
            f,
            "read_chunk-08ca9f8f-64c6-45f8-88d5-50a4399e1baf",
        ),
        "lambda-b644a272-fe8c-4fc7-9fed-fb3dae2f3866": (
            f,
            "normalise_channel-ac877174-b4e5-4464-879e-9af686c9b630",
        ),
        "compute_energy_time-722afbb6-1229-41a5-b10d-43fee0b942a5": (
            f,
            "lambda-b644a272-fe8c-4fc7-9fed-fb3dae2f3866",
        ),
        "read_chunk-da8b680f-c1ef-4fb1-8ead-5c8f67aa6bb6": (f, f),
        "normalise_channel-4a422ac8-7adb-47fb-89cd-d96c43400295": (
            f,
            "read_chunk-da8b680f-c1ef-4fb1-8ead-5c8f67aa6bb6",
        ),
        "lambda-a88b2318-f52c-469d-a4af-3f58e219b779": (
            f,
            "normalise_channel-4a422ac8-7adb-47fb-89cd-d96c43400295",
        ),
        "compute_energy_time-78f3fbf2-017c-446b-affa-0bf53e5d3534": (
            f,
            "lambda-a88b2318-f52c-469d-a4af-3f58e219b779",
        ),
        "lambda-94aa372f-64c5-43c9-9e26-12d058d84352": (
            f,
            "lambda-8cd8c88f-6590-46c5-97e0-992e7ae4f491",
            "lambda-9a99b8fd-18a6-43f8-94c2-9e31150998a1",
        ),
        "lambda-8cd8c88f-6590-46c5-97e0-992e7ae4f491": (
            f,
            "compute_energy_time-9162ccaf-bcb9-433e-a1ae-e745d5b72989",
            "compute_energy_time-462a7087-cf10-4765-a817-163a4a3e0627",
        ),
        "read_chunk-e7ac866d-f259-4be8-bff7-af5c36d950f7": (f, f),
        "normalise_channel-4c54379d-b0b2-4c25-9b64-3196815212dc": (
            f,
            "read_chunk-e7ac866d-f259-4be8-bff7-af5c36d950f7",
        ),
        "lambda-9e8229e8-804e-4acd-85cc-5beec4830acb": (
            f,
            "normalise_channel-4c54379d-b0b2-4c25-9b64-3196815212dc",
        ),
        "compute_energy_time-9162ccaf-bcb9-433e-a1ae-e745d5b72989": (
            f,
            "lambda-9e8229e8-804e-4acd-85cc-5beec4830acb",
        ),
        "read_chunk-05c2f1bb-a3fe-442f-8c21-115e73da5519": (f, f),
        "normalise_channel-23711edf-f989-415b-a030-2441d6e205d7": (
            f,
            "read_chunk-05c2f1bb-a3fe-442f-8c21-115e73da5519",
        ),
        "lambda-6353dbee-35c9-4d1c-b0ee-25dee2923f28": (
            f,
            "normalise_channel-23711edf-f989-415b-a030-2441d6e205d7",
        ),
        "compute_energy_time-462a7087-cf10-4765-a817-163a4a3e0627": (
            f,
            "lambda-6353dbee-35c9-4d1c-b0ee-25dee2923f28",
        ),
        "lambda-9a99b8fd-18a6-43f8-94c2-9e31150998a1": (
            f,
            "compute_energy_time-fb0538c4-6b9b-45eb-b9c0-4ba2f9edb417",
            "compute_energy_time-cfb26d70-7a10-4e7b-943e-a7f776105a79",
        ),
        "read_chunk-e1956e03-5b7d-4810-bdb3-e8c947a267c2": (f, f),
        "normalise_channel-bee36443-c2f6-4b62-9705-2b277404d1c8": (
            f,
            "read_chunk-e1956e03-5b7d-4810-bdb3-e8c947a267c2",
        ),
        "lambda-39785e9c-4f70-4531-b87c-3812366507c5": (
            f,
            "normalise_channel-bee36443-c2f6-4b62-9705-2b277404d1c8",
        ),
        "compute_energy_time-fb0538c4-6b9b-45eb-b9c0-4ba2f9edb417": (
            f,
            "lambda-39785e9c-4f70-4531-b87c-3812366507c5",
        ),
        "read_chunk-edbad689-7d65-4898-8af0-602a9b8d1864": (f, f),
        "normalise_channel-7bb2eff7-0ed3-4e7b-87ce-3ec9e8cd9f85": (
            f,
            "read_chunk-edbad689-7d65-4898-8af0-602a9b8d1864",
        ),
        "lambda-c42cad00-6a7c-4dbc-b74e-94e081aad500": (
            f,
            "normalise_channel-7bb2eff7-0ed3-4e7b-87ce-3ec9e8cd9f85",
        ),
        "compute_energy_time-cfb26d70-7a10-4e7b-943e-a7f776105a79": (
            f,
            "lambda-c42cad00-6a7c-4dbc-b74e-94e081aad500",
        ),
        "lambda-ad489580-1682-4045-954a-caa2c09b9726": (
            f,
            "compute_energy_time-ce153b68-fd90-48e9-8178-0d8cc8d4f2a5",
            "compute_energy_time-cbdee506-b3fb-48d4-8064-dbd730bdaf6d",
        ),
        "read_chunk-13f09f14-e43b-42bb-9d60-39e0f70c2c70": (f, f),
        "normalise_channel-a369bdf5-df4e-475c-ad0f-0eff0397d4b7": (
            f,
            "read_chunk-13f09f14-e43b-42bb-9d60-39e0f70c2c70",
        ),
        "lambda-bf488477-b595-4631-b7db-bb88249252dd": (
            f,
            "normalise_channel-a369bdf5-df4e-475c-ad0f-0eff0397d4b7",
        ),
        "compute_energy_time-ce153b68-fd90-48e9-8178-0d8cc8d4f2a5": (
            f,
            "lambda-bf488477-b595-4631-b7db-bb88249252dd",
        ),
        "read_chunk-61c5d4d2-28b2-4424-8e5d-0184a3a460e4": (f, f),
        "normalise_channel-e4f81fc5-4639-46e5-99a4-520de6d7ac9d": (
            f,
            "read_chunk-61c5d4d2-28b2-4424-8e5d-0184a3a460e4",
        ),
        "lambda-7e4519e0-e3f1-4cf2-a2e6-1fd9f79bc325": (
            f,
            "normalise_channel-e4f81fc5-4639-46e5-99a4-520de6d7ac9d",
        ),
        "compute_energy_time-cbdee506-b3fb-48d4-8064-dbd730bdaf6d": (
            f,
            "lambda-7e4519e0-e3f1-4cf2-a2e6-1fd9f79bc325",
        ),
        "fill_hist-559b962b-354e-4790-8ae4-017da796e462": (
            f,
            "lambda-b1813da8-9490-4c8b-8db3-9ba7b6c20e1c",
        ),
        "finalise-3a20f5c2-a931-4205-aa1c-cc5611eb5309": (
            f,
            "fill_hist-559b962b-354e-4790-8ae4-017da796e462",
        ),
        "add-c09239f2-1b9b-4d54-82c6-4d52e874ab04": (
            f,
            "add-0366d937-62e1-4580-bc2a-dbb6d0b58477",
            "add-23d5234f-e5c4-40c2-b7ab-9d602652bd9e",
        ),
        "add-0366d937-62e1-4580-bc2a-dbb6d0b58477": (
            f,
            "add-92b99cf7-1800-4289-80d2-399b77772aea",
            "add-fec67e19-bfd2-4ac6-a785-1dc3f2250378",
        ),
        "add-92b99cf7-1800-4289-80d2-399b77772aea": (
            f,
            "add-62db9502-7e8a-48a7-b414-ce81400c5c32",
            "add-3c8f2bd6-2d6f-4676-bc33-93ce1f5ad3c6",
        ),
        "add-62db9502-7e8a-48a7-b414-ce81400c5c32": (
            f,
            "fill_hist_ptp-e984e1f7-13f2-4741-94e0-f34be3b6ee3a",
            "fill_hist_ptp-1281d98d-7fb3-4d29-a6c0-1a538d47652a",
        ),
        "fill_hist_ptp-e984e1f7-13f2-4741-94e0-f34be3b6ee3a": (
            f,
            "normalise_channel-a59c27f2-ed8e-4d6d-8ec8-dab6be36ee03",
        ),
        "fill_hist_ptp-1281d98d-7fb3-4d29-a6c0-1a538d47652a": (
            f,
            "normalise_channel-79384943-c14f-4039-bc33-0702ddfe8c5a",
        ),
        "add-3c8f2bd6-2d6f-4676-bc33-93ce1f5ad3c6": (
            f,
            "fill_hist_ptp-5acf1198-91fa-4784-9276-caa29c382e15",
            "fill_hist_ptp-25e2e2da-95a2-4658-8db9-e689dd95b479",
        ),
        "fill_hist_ptp-5acf1198-91fa-4784-9276-caa29c382e15": (
            f,
            "normalise_channel-ac877174-b4e5-4464-879e-9af686c9b630",
        ),
        "fill_hist_ptp-25e2e2da-95a2-4658-8db9-e689dd95b479": (
            f,
            "normalise_channel-4a422ac8-7adb-47fb-89cd-d96c43400295",
        ),
        "add-fec67e19-bfd2-4ac6-a785-1dc3f2250378": (
            f,
            "add-6b47ae48-cc0d-4a5b-9eab-85b626d8e3d9",
            "add-4b6365f9-745d-4bd0-a618-d51841436064",
        ),
        "add-6b47ae48-cc0d-4a5b-9eab-85b626d8e3d9": (
            f,
            "fill_hist_ptp-41b37863-5cea-4bc1-a017-f2fc0e878297",
            "fill_hist_ptp-b6fd58ec-1145-4797-b2c5-e697250e2cf4",
        ),
        "fill_hist_ptp-41b37863-5cea-4bc1-a017-f2fc0e878297": (
            f,
            "normalise_channel-4c54379d-b0b2-4c25-9b64-3196815212dc",
        ),
        "fill_hist_ptp-b6fd58ec-1145-4797-b2c5-e697250e2cf4": (
            f,
            "normalise_channel-23711edf-f989-415b-a030-2441d6e205d7",
        ),
        "add-4b6365f9-745d-4bd0-a618-d51841436064": (
            f,
            "fill_hist_ptp-289a9d23-66af-430d-a0e9-8465410a055d",
            "fill_hist_ptp-6ee556d0-6e55-491f-bc76-533ffedd95b7",
        ),
        "fill_hist_ptp-289a9d23-66af-430d-a0e9-8465410a055d": (
            f,
            "normalise_channel-bee36443-c2f6-4b62-9705-2b277404d1c8",
        ),
        "fill_hist_ptp-6ee556d0-6e55-491f-bc76-533ffedd95b7": (
            f,
            "normalise_channel-7bb2eff7-0ed3-4e7b-87ce-3ec9e8cd9f85",
        ),
        "add-23d5234f-e5c4-40c2-b7ab-9d602652bd9e": (
            f,
            "fill_hist_ptp-c589e891-97c6-4cab-b600-7018f4eee3a0",
            "fill_hist_ptp-78d3436c-5417-4828-b9ea-47859ccaa5da",
        ),
        "fill_hist_ptp-c589e891-97c6-4cab-b600-7018f4eee3a0": (
            f,
            "normalise_channel-a369bdf5-df4e-475c-ad0f-0eff0397d4b7",
        ),
        "fill_hist_ptp-78d3436c-5417-4828-b9ea-47859ccaa5da": (
            f,
            "normalise_channel-e4f81fc5-4639-46e5-99a4-520de6d7ac9d",
        ),
        "finalise-ecc61ba8-281c-45cc-9d67-9b395f193c02": (
            f,
            "add-c09239f2-1b9b-4d54-82c6-4d52e874ab04",
        ),
    }
    dependencies, dependents = get_deps(dsk)
    o = order(dsk)
    parents = {deps.pop() for key, deps in dependents.items() if not dependencies[key]}

    def cost(deps):
        a, b = deps
        return abs(o[a] - o[b])

    cost_of_pairs = {key: cost(dependents[key]) for key in parents}
    # Allow one to be bad, b/c this is hard!
    costs = sorted(cost_of_pairs.values())
    assert sum(costs[:-1]) <= 25 or sum(costs) <= 31

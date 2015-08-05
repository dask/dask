from itertools import chain

from dask.order import ndependents, order
from dask.order import get_deps


def issorted(L, reverse=False):
    return sorted(L, reverse=reverse) == L


def test_ordering_prefers_depth_first():
    a, b, c = 'abc'
    f = lambda *args: None
    d = {(a, 0): (f,), (b, 0): 0, (c, 0): (f,),
         (a, 1): (f,), (b, 1): (f, (a, 0), (b, 0), (c, 0)), (c, 1): (f,),
         (a, 2): (f,), (b, 2): (f, (a, 1), (b, 1), (c, 1)), (c, 2): (f,),
         (a, 3): (f,), (b, 3): (f, (a, 2), (b, 2), (c, 2)), (c, 3): (f,)}

    o = order(d)

    assert issorted(list(map(o.get, [(a, i) for i in range(4)])), reverse=True)
    assert issorted(list(map(o.get, [(c, i) for i in range(4)])), reverse=True)


def test_ordering_keeps_groups_together():
    a, b, c = 'abc'
    f = lambda *args: None
    d = dict(((a, i), (f,)) for i in range(4))
    d.update({(b, 0): (f, (a, 0), (a, 1)),
              (b, 1): (f, (a, 2), (a, 3))})
    o = order(d)

    assert abs(o[(a, 0)] - o[(a, 1)]) == 1
    assert abs(o[(a, 2)] - o[(a, 3)]) == 1

    d = dict(((a, i), (f,)) for i in range(4))
    d.update({(b, 0): (f, (a, 0), (a, 2)),
              (b, 1): (f, (a, 1), (a, 3))})
    o = order(d)

    assert abs(o[(a, 0)] - o[(a, 2)]) == 1
    assert abs(o[(a, 1)] - o[(a, 3)]) == 1


def test_ordering_prefers_tasks_that_release_data():
    a, b, c = 'abc'
    f = lambda *args: None
    d = {(a, 0): (f,), (a, 1): (f,),
         (b, 0): (f, (a, 0)), (b, 1): (f, (a, 1)), (b, 2): (f, (a, 1))}

    o = order(d)

    assert o[(a, 1)] > o[(a, 0)]

    d = {(a, 0): (f,), (a, 1): (f,),
         (b, 0): (f, (a, 0)), (b, 1): (f, (a, 1)), (b, 2): (f, (a, 0))}

    o = order(d)

    assert o[(a, 1)] < o[(a, 0)]


def test_ndependents():
    from operator import add
    a, b, c = 'abc'
    dsk = dict(chain((((a, i), i * 2) for i in range(5)),
                     (((b, i), (add, i, (a, i))) for i in range(5)),
                     (((c, i), (add, i, (b, i))) for i in range(5))))
    result = ndependents(*get_deps(dsk))
    expected = dict(chain((((a, i), 3) for i in range(5)),
                          (((b, i), 2) for i in range(5)),
                          (((c, i), 1) for i in range(5))))
    assert result == expected

    dsk = {a: 1, b: 1}
    deps = get_deps(dsk)
    assert ndependents(*deps) == dsk

    dsk = {a: 1, b: (add, a, 1), c: (add, b, a)}
    assert ndependents(*get_deps(dsk)) == {a: 4, b: 2, c: 1}

    dsk = {a: 1, b: a, c: b}
    deps = get_deps(dsk)
    assert ndependents(*deps) == {a: 3, b: 2, c: 1}

from itertools import chain

from dask.order import child_max, ndependents, order
from dask.core import get_deps
from dask.utils_test import add, inc


def issorted(L, reverse=False):
    return sorted(L, reverse=reverse) == L


def f(*args):
    pass


def test_ordering_keeps_groups_together():
    a, b, c = 'abc'
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


def test_prefer_broker_nodes():
    """

    b0    b1  b2
    |      \  /
    a0      a1

    a1 should be run before a0
    """
    a, b, c = 'abc'
    dsk = {(a, 0): (f,), (a, 1): (f,),
           (b, 0): (f, (a, 0)), (b, 1): (f, (a, 1)), (b, 2): (f, (a, 1))}

    o = order(dsk)

    assert o[(a, 1)] < o[(a, 0)]

    # Switch name of 0, 1 to ensure that this isn't due to string comparison
    dsk = {(a, 0): (f,), (a, 1): (f,),
           (b, 0): (f, (a, 0)), (b, 1): (f, (a, 1)), (b, 2): (f, (a, 0))}

    o = order(dsk)

    assert o[(a, 1)] > o[(a, 0)]


def test_base_of_reduce_preferred():
    """
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
    dsk = dict((('a', i), (f, ('a', i - 1), ('b', i))) for i in [1, 2, 3])
    dsk[('a', 0)] = (f, ('b', 0))
    dsk.update(dict((('b', i), (f, 'c', 1)) for i in [0, 1, 2, 3]))
    dsk['c'] = 1

    o = order(dsk)

    assert o == {('a', 3): 0,
                 ('a', 2): 1,
                 ('a', 1): 2,
                 ('a', 0): 3,
                 ('b', 0): 4,
                 'c': 5,
                 ('b', 1): 6,
                 ('b', 2): 7,
                 ('b', 3): 8}

    # ('b', 0) is the most important out of ('b', i)
    assert min([('b', i) for i in [0, 1, 2, 3]], key=o.get) == ('b', 0)


def test_deep_bases_win_over_dependents():
    """
    d should come before e and probably before one of b and c

            a
          / | \   .
         b  c |
        / \ | /
       e    d
    """
    dsk = {'a': (f, 'b', 'c', 'd'), 'b': (f, 'd', 'e'), 'c': (f, 'd'), 'd': 1,
           'e': 2}

    o = order(dsk)
    assert o['d'] < o['e']
    assert o['d'] < o['b'] or o['d'] < o['c']


def test_prefer_deep():
    """
        c
        |
    y   b
    |   |
    x   a

    Prefer longer chains first so we should start with c
    """
    dsk = {'a': 1, 'b': (f, 'a'), 'c': (f, 'b'),
           'x': 1, 'y': (f, 'x')}

    o = order(dsk)
    assert o == {'c': 0, 'b': 1, 'a': 2, 'y': 3, 'x': 4}


def test_stacklimit():
    dsk = dict(('x%s' % (i + 1), (inc, 'x%s' % i)) for i in range(10000))
    dependencies, dependents = get_deps(dsk)
    scores = dict.fromkeys(dsk, 1)
    child_max(dependencies, dependents, scores)
    ndependents(dependencies, dependents)


def test_ndependents():
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


def test_break_ties_by_str():
    dsk = {('x', i): (inc, i) for i in range(10)}
    x_keys = sorted(dsk)
    dsk['y'] = list(x_keys)

    o = order(dsk)
    expected = {'y': 0}
    expected.update({k: i + 1 for i, k in enumerate(x_keys)})

    assert o == expected


def test_order_doesnt_fail_on_mixed_type_keys():
    order({'x': (inc, 1),
           ('y', 0): (inc, 2),
           'z': (add, 'x', ('y', 0))})

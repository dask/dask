from __future__ import absolute_import, division, print_function

from toolz import merge, join, reduceby, pipe
import numpy as np
from dask.bag.core import Bag, lazify_internal, lazify_task, fuse, lazify_top

dsk = {('x', 0): (range, 5),
       ('x', 1): (range, 5),
       ('x', 2): (range, 5)}

L = list(range(5)) * 3

b = Bag(dsk, 'x', 3)

def inc(x):
    return x + 1

def iseven(x):
    return x % 2 == 0

def isodd(x):
    return x % 2 == 1

def add(x, y):
    return x + y


def test_Bag():
    assert b.name == 'x'
    assert b.npartitions == 3


def test_keys():
    assert sorted(b.keys()) == sorted(dsk.keys())


def test_map():
    c = b.map(inc)
    expected = merge(dsk, dict(((c.name, i), (list, (map, inc, (b.name, i))))
                               for i in range(b.npartitions)))
    assert c.dask == expected


def test_filter():
    c = b.filter(iseven)
    expected = merge(dsk, dict(((c.name, i),
                                (list, (filter, iseven, (b.name, i))))
                               for i in range(b.npartitions)))
    assert c.dask == expected


def test_iter():
    assert sorted(list(b)) == sorted(L)
    assert sorted(list(b.map(inc))) == sorted(list(range(1, 6)) * 3)


def test_fold_computation():
    assert int(b.fold(add)) == sum(L)


def test_frequencies():
    assert dict(list(b.frequencies())) == {0: 3, 1: 3, 2: 3, 3: 3, 4: 3}


def test_topk():
    assert list(b.topk(4)) == [4, 4, 4, 3]
    assert list(b.topk(4, key=lambda x: -x)) == [0, 0, 0, 1]


def test_lambdas():
    assert list(b.map(lambda x: x + 1)) == list(b.map(inc))

def test_reductions():
    assert int(b.count()) == 15
    assert int(b.sum()) == 30
    assert int(b.max()) == 4
    assert int(b.min()) == 0
    assert int(b.any()) == True
    assert int(b.all()) == False  # some zeros exist

def test_mean():
    assert float(b.mean()) == np.mean(L)
def test_std():
    assert float(b.std()) == np.std(L)
def test_var():
    assert float(b.var()) == np.var(L)


def test_join():
    assert list(b.join([1, 2, 3], on_self=isodd, on_other=iseven)) == \
            list(join(iseven, [1, 2, 3], isodd, list(b)))
    assert list(b.join([1, 2, 3], isodd)) == \
            list(join(isodd, [1, 2, 3], isodd, list(b)))

def test_foldby():
    c = b.foldby(iseven, lambda acc, x: acc + x, 0, lambda a, b: a + b, 0)
    assert set(c) == set(reduceby(iseven, lambda acc, x: acc + x, L, 0).items())


def test_map_partitions():
    assert list(b.map_partitions(len)) == [5, 5, 5]


def test_lazify_task():
    task = (sum, (list, (map, inc, [1, 2, 3])))
    assert lazify_task(task) == (sum, (map, inc, [1, 2, 3]))

    task = (list, (map, inc, [1, 2, 3]))
    assert lazify_task(task) == task

    a = (list, (map, inc,
                     (list, (filter, iseven, 'y'))))
    b = (list, (map, inc,
                            (filter, iseven, 'y')))
    assert lazify_task(a) == b


f = lambda x: x


def test_lazify_internal():
    a = {'x': (list, (map, inc,
                           (list, (filter, iseven, 'y')))),
         'a': (f, 'x'), 'b': (f, 'x')}
    b = {'x': (list, (map, inc,
                                  (filter, iseven, 'y'))),
         'a': (f, 'x'), 'b': (f, 'x')}
    assert lazify_internal(a) == b


def test_lazify_top():
    a = {'x': (list, (map, inc,
                           (list, (filter, iseven, 'y')))),
         'a': (f, 'x')}
    b = {'x':        (map, inc,
                           (list, (filter, iseven, 'y'))),
         'a': (f, 'x')}
    assert lazify_top(a) == b

    b = Bag(dsk, 'x', 3)
    d = pipe(b.map(inc).filter(iseven).sum().dask, fuse, lazify_internal,
            lazify_top)
    assert 'list' not in str(d)

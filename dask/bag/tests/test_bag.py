from __future__ import absolute_import, division, print_function

from toolz import merge, join
import numpy as np
from dask.bag.core import Bag

dsk = {('x', 0): (range, 5),
       ('x', 1): (range, 5),
       ('x', 2): (range, 5)}

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
    assert sorted(list(b)) == sorted(list(range(5)) * 3)
    assert sorted(list(b.map(inc))) == sorted(list(range(1, 6)) * 3)


def test_fold_computation():
    assert int(b.fold(add)) == sum(range(5)) * 3


def test_frequencies():
    assert dict(list(b.frequencies())) == {0: 3, 1: 3, 2: 3, 3: 3, 4: 3}


def test_topk():
    assert list(b.topk(4)) == [4, 4, 4, 3]


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
    assert float(b.mean()) == np.mean(list(range(5)) * 3)
def test_std():
    assert float(b.std()) == np.std(list(range(5)) * 3)
def test_var():
    assert float(b.var()) == np.var(list(range(5)) * 3)


def test_join():
    assert list(b.join([1, 2, 3], on_self=isodd, on_other=iseven)) == \
            list(join(iseven, [1, 2, 3], isodd, list(b)))

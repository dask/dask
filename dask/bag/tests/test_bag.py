from toolz import merge
from dask.bag.core import Bag

dsk = {('x', i): (range, 5) for i in range(3)}
b = Bag(dsk, 'x', 3)

def inc(x):
    return x + 1

def iseven(x):
    return x % 2 == 0

def add(x, y):
    return x + y


def test_Bag():
    assert b.name == 'x'
    assert b.npartitions == 3


def test_keys():
    assert sorted(b.keys()) == sorted(dsk.keys())


def test_map():
    c = b.map(inc)
    expected = merge(dsk, {(c.name, i): (map, inc, (b.name, i))
                           for i in range(b.npartitions)})
    assert c.dask == expected

def test_filter():
    c = b.filter(iseven)
    expected = merge(dsk, {(c.name, i): (filter, iseven, (b.name, i))
                           for i in range(b.npartitions)})
    assert c.dask == expected

def dont_test_fold():
    c = b.fold(add)
    expected = merge(dsk, {(c.name, i): (reduce, add, (b.name, i))
                           for i in range(b.npartitions)})
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

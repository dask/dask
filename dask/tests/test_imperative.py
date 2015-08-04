from operator import add
from collections import Iterator
from random import random

import pytest

from dask.imperative import value, do, to_task_dasks, compute, Value
from dask.utils import raises


def test_value():
    v = value(1)
    assert v.compute() == 1
    assert 1 in v.dask.values()


def test_to_task_dasks():
    a = value(1, 'a')
    b = value(2, 'b')
    task, dasks = to_task_dasks([a, b, 3])
    assert task == (list, ['a', 'b', 3])
    assert len(dasks) == 2
    assert a.dask in dasks
    assert b.dask in dasks

    task, dasks = to_task_dasks({a: 1, b: 2})
    assert (task == (dict, (list, [(list, ['b', 2]), (list, ['a', 1])]))
            or task == (dict, (list, [(list, ['a', 1]), (list, ['b', 2])])))
    assert len(dasks) == 2
    assert a.dask in dasks
    assert b.dask in dasks


def test_operators():
    a = value([1, 2, 3])
    assert a[0].compute() == 1
    assert (a + a).compute() == [1, 2, 3, 1, 2, 3]

    a = value(10)
    assert (a + 1).compute() == 11
    assert (1 + a).compute() == 11
    assert (a >> 1).compute() == 5
    assert (a > 2).compute()
    assert (a ** 2).compute() == 100


def test_methods():
    a = value("a b c d e")
    assert a.split(' ').compute() == ['a', 'b', 'c', 'd', 'e']
    assert a.upper().replace('B', 'A').split().count('A').compute() == 2


def test_attributes():
    a = value(2 + 1j)
    assert a.real.compute() == 2
    assert a.imag.compute() == 1


def test_value_errors():
    a = value([1, 2, 3])
    # Immutable
    assert raises(TypeError, lambda: setattr(a, 'foo', 1))
    assert raises(TypeError, lambda: setattr(a, '_key', 'test'))
    def setitem(a, ind, val):
        a[ind] = val
    assert raises(TypeError, lambda: setitem(a, 1, 0))
    # Can't iterate, or check if contains
    assert raises(TypeError, lambda: 1 in a)
    assert raises(TypeError, lambda: list(a))
    # No dynamic generation of magic methods
    assert raises(AttributeError, lambda: a.__len__())
    # Truth of values forbidden
    assert raises(TypeError, lambda: bool(a))


def test_do():
    add2 = do(add)
    assert add2(1, 2).compute() == 3
    assert (add2(1, 2) + 3).compute() == 6
    assert add2(add2(1, 2), 3).compute() == 6
    a = value(1)
    b = add2(add2(a, 2), 3)
    assert a.key in b.dask


def test_compute():
    a = value(1) + 5
    b = a + 1
    c = a + 2
    assert compute(b, c) == (7, 8)
    assert compute(b) == (7,)
    assert compute([a, b], c) == ([6, 7], 8)


def test_named_value():
    assert 'X' in value(1, name='X').dask


def test_common_subexpressions():
    a = value([1, 2, 3])
    res = a[0] + a[0]
    assert a[0].key in res.dask
    assert a.key in res.dask
    assert len(res.dask) == 3


def test_lists():
    a = value(1)
    b = value(2)
    c = do(sum)([a, b])
    assert c.compute() == 3


def test_literates():
    a = value(1)
    b = a + 1
    lit = (a, b, 3)
    assert value(lit).compute() == (1, 2, 3)
    lit = set((a, b, 3))
    assert value(lit).compute() == set((1, 2, 3))
    lit = {a: 'a', b: 'b', 3: 'c'}
    assert value(lit).compute() == {1: 'a', 2: 'b', 3: 'c'}
    assert value(lit)[a].compute() == 'a'
    lit = {'a': a, 'b': b, 'c': 3}
    assert value(lit).compute() == {'a': 1, 'b': 2, 'c': 3}
    assert value(lit)['a'].compute() == 1


def test_lists_are_concrete():
    a = value(1)
    b = value(2)
    c = do(max)([[a, 10], [b, 20]], key=lambda x: x[0])[1]

    assert c.compute() == 20


def test_iterators():
    a = value(1)
    b = value(2)
    c = do(sum)(iter([a, b]))

    assert c.compute() == 3

    def f(seq):
        assert isinstance(seq, Iterator)
        return sum(seq)

    c = do(f)(iter([a, b]))
    assert c.compute() == 3


def test_pure():
    v1 = do(add, pure=True)(1, 2)
    v2 = do(add, pure=True)(1, 2)
    assert v1.key == v2.key

    myrand = do(random)
    assert myrand().key != myrand().key


da = pytest.importorskip('dask.array')
import numpy as np


def test_array_imperative():
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5))
    val = do(sum)([arr, darr, 1])
    assert isinstance(val, Value)
    assert np.allclose(val.compute(), arr + arr + 1)
    assert val.sum().compute() == (arr + arr + 1).sum()
    assert val[0, 0].compute() == (arr + arr + 1)[0, 0]

    task, dasks = to_task_dasks(darr)
    assert len(dasks) == 1
    orig = set(darr.dask)
    final = set(dasks[0])
    assert orig.issubset(final)
    diff = final.difference(orig)
    assert len(diff) == 1


db = pytest.importorskip('dask.bag')


def test_array_bag_imperative():
    arr1 = np.arange(100).reshape((10, 10))
    arr2 = arr1.dot(arr1.T)
    darr1 = da.from_array(arr1, chunks=(5, 5))
    darr2 = da.from_array(arr2, chunks=(5, 5))
    b = db.from_sequence([1, 2, 3])
    seq = [arr1, arr2, darr1, darr2, b]
    out = do(sum)([i.sum() for i in seq])
    assert out.compute() == 2*arr1.sum() + 2*arr2.sum() + sum([1, 2, 3])

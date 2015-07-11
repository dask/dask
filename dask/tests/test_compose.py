from dask.compose import value, daskify
from operator import add


def test_value():
    v = value(1)
    assert v.compute() == 1
    assert 1 in v.dask.values()


def test_dask_function():
    add2 = daskify(add)

    a, b, c = map(value, [1, 2, 3])

    d = add2(add2(a, b), c)

    assert d.compute() == 1 + 2 + 3
    assert set(a.dask.keys()).issubset(set(d.dask.keys()))


def test_named_value():
    assert 'X' in value(1, name='X').dask

import dask.array as da
import numpy as np
import dask
from dask.array.ghost import *
from dask.core import get


def eq(a, b):
    if isinstance(a, Array):
        a = a.compute()
    if isinstance(b, Array):
        b = b.compute()
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_fractional_slice():
    assert fractional_slice(('x', 4.9), {0: 2}) == \
            (getitem, ('x', 5), (slice(0, 2),))

    assert fractional_slice(('x', 3, 5.1), {0: 2, 1: 3}) == \
            (getitem, ('x', 3, 5), (slice(None, None, None), slice(-3, None)))

    assert fractional_slice(('x', 2.9, 5.1), {0: 2, 1: 3}) == \
            (getitem, ('x', 3, 5), (slice(0, 2), slice(-3, None)))


def test_arange_2d():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, blockshape=(4, 4))

    g = ghost(d, {0: 2, 1: 1})
    result = g.compute(get=get)

    expected = np.array([
        [ 0,  1,  2,  3,  4,    3,  4,  5,  6,  7],
        [ 8,  9, 10, 11, 12,   11, 12, 13, 14, 15],
        [16, 17, 18, 19, 20,   19, 20, 21, 22, 23],
        [24, 25, 26, 27, 28,   27, 28, 29, 30, 31],
        [32, 33, 34, 35, 36,   35, 36, 37, 38, 39],
        [40, 41, 42, 43, 44,   43, 44, 45, 46, 47],

        [16, 17, 18, 19, 20,   19, 20, 21, 22, 23],
        [24, 25, 26, 27, 28,   27, 28, 29, 30, 31],
        [32, 33, 34, 35, 36,   35, 36, 37, 38, 39],
        [40, 41, 42, 43, 44,   43, 44, 45, 46, 47],
        [48, 49, 50, 51, 52,   51, 52, 53, 54, 55],
        [56, 57, 58, 59, 60,   59, 60, 61, 62, 63]])

    assert eq(result, expected)


def test_internal_trim():
    d = da.ones((40, 60), blockshape=(10, 10))
    e = internal_trim(d, axes={0: 1, 1: 2})

    assert e.blockdims == ((8, 8, 8, 8), (6, 6, 6, 6, 6, 6))


def test_periodic():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, blockshape=(4, 4))

    e = periodic(d, axis=0, depth=2)
    assert e.shape[0] == d.shape[0] + 4
    assert e.shape[1] == d.shape[1]

    assert eq(e[1, :], d[-1, :])
    assert eq(e[0, :], d[-2, :])



def test_constant():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, blockshape=(4, 4))

    e = constant(d, axis=0, depth=2, value=10)
    assert e.shape[0] == d.shape[0] + 4
    assert e.shape[1] == d.shape[1]

    assert eq(e[1, :], 10)
    assert eq(e[-1, :], 10)

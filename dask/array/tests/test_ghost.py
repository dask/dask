import dask.array as da
import numpy as np
import dask
from dask.array.ghost import *
from dask.core import get


def eq(a, b):
    if isinstance(a, Array):
        a = a.compute(get=dask.get)
    if isinstance(b, Array):
        b = b.compute(get=dask.get)
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


def test_ghost_internal():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4))

    g = ghost_internal(d, {0: 2, 1: 1})
    result = g.compute(get=get)
    assert g.chunks == ((6, 6), (5, 5))

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


def test_trim_internal():
    d = da.ones((40, 60), chunks=(10, 10))
    e = trim_internal(d, axes={0: 1, 1: 2})

    assert e.chunks == ((8, 8, 8, 8), (6, 6, 6, 6, 6, 6))


def test_periodic():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4))

    e = periodic(d, axis=0, depth=2)
    assert e.shape[0] == d.shape[0] + 4
    assert e.shape[1] == d.shape[1]

    assert eq(e[1, :], d[-1, :])
    assert eq(e[0, :], d[-2, :])


def test_reflect():
    x = np.arange(10)
    d = da.from_array(x, chunks=(5, 5))

    e = reflect(d, axis=0, depth=2)
    expected = np.array([1, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 8])
    assert eq(e, expected)

    e = reflect(d, axis=0, depth=1)
    expected = np.array([0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9])
    assert eq(e, expected)


def test_constant():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4))

    e = constant(d, axis=0, depth=2, value=10)
    assert e.shape[0] == d.shape[0] + 4
    assert e.shape[1] == d.shape[1]

    assert eq(e[1, :], 10)
    assert eq(e[-1, :], 10)


def test_boundaries():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4))

    e = boundaries(d, {0: 2, 1: 1}, {0: 0, 1: 'periodic'})

    expected = np.array(
        [[ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
         [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
         [ 7, 0, 1, 2, 3, 4, 5, 6, 7, 0],
         [15, 8, 9,10,11,12,13,14,15, 8],
         [23,16,17,18,19,20,21,22,23,16],
         [31,24,25,26,27,28,29,30,31,24],
         [39,32,33,34,35,36,37,38,39,32],
         [47,40,41,42,43,44,45,46,47,40],
         [55,48,49,50,51,52,53,54,55,48],
         [63,56,57,58,59,60,61,62,63,56],
         [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
         [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]])
    assert eq(e, expected)


def test_ghost():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4))
    g = ghost(d, depth={0: 2, 1: 1}, boundary={0: 100, 1: 'reflect'})
    assert g.chunks == ((8, 8), (6, 6))
    expected = np.array(
      [[100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
       [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
       [  0,   0,   1,   2,   3,   4,   3,   4,   5,   6,   7,   7],
       [  8,   8,   9,  10,  11,  12,  11,  12,  13,  14,  15,  15],
       [ 16,  16,  17,  18,  19,  20,  19,  20,  21,  22,  23,  23],
       [ 24,  24,  25,  26,  27,  28,  27,  28,  29,  30,  31,  31],
       [ 32,  32,  33,  34,  35,  36,  35,  36,  37,  38,  39,  39],
       [ 40,  40,  41,  42,  43,  44,  43,  44,  45,  46,  47,  47],
       [ 16,  16,  17,  18,  19,  20,  19,  20,  21,  22,  23,  23],
       [ 24,  24,  25,  26,  27,  28,  27,  28,  29,  30,  31,  31],
       [ 32,  32,  33,  34,  35,  36,  35,  36,  37,  38,  39,  39],
       [ 40,  40,  41,  42,  43,  44,  43,  44,  45,  46,  47,  47],
       [ 48,  48,  49,  50,  51,  52,  51,  52,  53,  54,  55,  55],
       [ 56,  56,  57,  58,  59,  60,  59,  60,  61,  62,  63,  63],
       [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
       [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100]])
    assert eq(g, expected)

    g = ghost(d, depth={0: 2, 1: 1}, boundary={0: 100})
    assert g.chunks == ((8, 8), (5, 5))

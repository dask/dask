import random

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq


functions = [
    lambda x: x,
    lambda x: da.expm1(x),
    lambda x: 2 * x,
    lambda x: x / 2,
    lambda x: x**2,
    lambda x: x + x,
    lambda x: x * x,
    lambda x: x[0],
    lambda x: x[:, 1],
    lambda x: x[:1, None, 1:3],
    lambda x: x.T,
    lambda x: da.transpose(x, (1, 2, 0)),
    lambda x: x.sum(),
    lambda x: x.dot(np.arange(x.shape[-1])),
    lambda x: x.dot(np.eye(x.shape[-1])),
    lambda x: da.tensordot(x, np.ones(x.shape[:2]), axes=[(0, 1), (0, 1)]),
    lambda x: x.sum(axis=0),
    lambda x: x.max(axis=0),
    lambda x: x.sum(axis=(1, 2)),
    lambda x: x.astype(np.complex128),
    lambda x: x.map_blocks(lambda x: x * 2),
    lambda x: x.round(1),
    lambda x: x.reshape((x.shape[0] * x.shape[1], x.shape[2])),
    lambda x: abs(x),
    lambda x: x > 0.5,
    lambda x: x.rechunk((4, 4, 4)),
    lambda x: x.rechunk((2, 2, 1)),
]


@pytest.mark.parametrize('func', functions)
def test_basic(func):
    x = da.random.random((2, 3, 4), chunks=(1, 2, 2))
    x[x < 0.8] = 0

    y = da.ma.masked_equal(x, 0)

    xx = func(x)
    yy = func(y)

    assert_eq(xx, da.ma.filled(yy, 0))

    if yy.shape:
        zz = yy.compute()
        assert isinstance(zz, np.ma.masked_array)


def test_tensordot():
    x = da.random.random((2, 3, 4), chunks=(1, 2, 2))
    x[x < 0.8] = 0
    y = da.random.random((4, 3, 2), chunks=(2, 2, 1))
    y[y < 0.8] = 0

    xx = da.ma.masked_equal(x, 0)
    yy = da.ma.masked_equal(y, 0)

    assert_eq(da.tensordot(x, y, axes=(2, 0)),
              da.ma.filled(da.tensordot(xx, yy, axes=(2, 0)), 0))
    assert_eq(da.tensordot(x, y, axes=(1, 1)),
              da.ma.filled(da.tensordot(xx, yy, axes=(1, 1)), 0))
    assert_eq(da.tensordot(x, y, axes=((1, 2), (1, 0))),
              da.ma.filled(da.tensordot(xx, yy, axes=((1, 2), (1, 0))), 0))


@pytest.mark.parametrize('func', functions)
def test_mixed_concatenate(func):
    x = da.random.random((2, 3, 4), chunks=(1, 2, 2))
    y = da.random.random((2, 3, 4), chunks=(1, 2, 2))

    y[y < 0.8] = 0
    yy = da.ma.masked_equal(y, 0)

    d = da.concatenate([x, y], axis=0)
    s = da.concatenate([x, yy], axis=0)

    dd = func(d)
    ss = func(s)
    assert_eq(dd, ss)


@pytest.mark.parametrize('func', functions)
def test_mixed_random(func):
    d = da.random.random((4, 3, 4), chunks=(1, 2, 2))
    d[d < 0.7] = 0

    fn = lambda x: np.ma.masked_equal(x, 0) if random.random() < 0.5 else x
    s = d.map_blocks(fn)

    dd = func(d)
    ss = func(s)

    assert_eq(dd, ss)


def test_mixed_output_type():
    y = da.random.random((10, 10), chunks=(5, 5))
    y[y < 0.8] = 0

    y = da.ma.masked_equal(y, 0)
    x = da.zeros((10, 1), chunks=(5, 1))

    z = da.concatenate([x, y], axis=1)
    assert z.shape == (10, 11)
    zz = z.compute()
    assert isinstance(zz, np.ma.masked_array)


def test_creation_functions():
    x = np.array([-2, -1, 0, 1, 2] * 20).reshape((10, 10))
    dx = da.from_array(x, chunks=5)

    assert_eq(da.ma.masked_equal(dx, 0), np.ma.masked_equal(x, 0))
    assert_eq(da.ma.masked_greater(dx, 0), np.ma.masked_greater(x, 0))
    assert_eq(da.ma.masked_greater_equal(dx, 0), np.ma.masked_greater_equal(x, 0))
    assert_eq(da.ma.masked_less(dx, 0), np.ma.masked_less(x, 0))
    assert_eq(da.ma.masked_less_equal(dx, 0), np.ma.masked_less_equal(x, 0))
    assert_eq(da.ma.masked_not_equal(dx, 0), np.ma.masked_not_equal(x, 0))
    assert_eq(da.ma.masked_where(False, dx), np.ma.masked_where(False, x))
    assert_eq(da.ma.masked_where(dx > 2, dx), np.ma.masked_where(x > 2, x))

    with pytest.raises(IndexError):
        da.ma.masked_where((dx > 2)[:, 0], dx)

    assert_eq(da.ma.masked_inside(dx, -1, 1), np.ma.masked_inside(x, -1, 1))
    assert_eq(da.ma.masked_outside(dx, -1, 1), np.ma.masked_outside(x, -1, 1))
    assert_eq(da.ma.masked_values(dx, -1), np.ma.masked_values(x, -1))

    y = x.astype('f8')
    y[0, 0] = y[7, 5] = np.nan
    dy = da.from_array(y, chunks=5)

    assert_eq(da.ma.masked_invalid(dy), np.ma.masked_invalid(y))

    my = np.ma.masked_greater(y, 0)
    dmy = da.ma.masked_greater(dy, 0)

    assert_eq(da.ma.fix_invalid(dmy, fill_value=0),
              np.ma.fix_invalid(my, fill_value=0))


def test_filled():
    x = np.array([-2, -1, 0, 1, 2] * 20).reshape((10, 10))
    dx = da.from_array(x, chunks=5)

    mx = np.ma.masked_equal(x, 0)
    mdx = da.ma.masked_equal(dx, 0)

    assert_eq(da.ma.filled(mdx), np.ma.filled(mx))
    assert_eq(da.ma.filled(mdx, -5), np.ma.filled(mx, -5))


def assert_eq_ma(a, b):
    res = a.compute()
    assert type(res) == type(b)
    if hasattr(res, 'mask'):
        np.testing.assert_equal(res.mask, b.mask)
        a = da.ma.filled(a)
        b = np.ma.filled(b)
    assert_eq(a, b, equal_nan=True)


@pytest.mark.parametrize('dtype', ('i8', 'f8'))
@pytest.mark.parametrize('reduction', ['sum', 'prod', 'mean', 'var', 'std',
                                       'min', 'max', 'any', 'all'])
def test_reductions(dtype, reduction):
    x = (np.random.RandomState(42).rand(11, 11) * 10).astype(dtype)
    dx = da.from_array(x, chunks=(4, 4))
    mx = np.ma.masked_greater(x, 5)
    mdx = da.ma.masked_greater(dx, 5)

    dfunc = getattr(da, reduction)
    func = getattr(np, reduction)

    assert_eq_ma(dfunc(mdx), func(mx))
    assert_eq_ma(dfunc(mdx, axis=0), func(mx, axis=0))
    assert_eq_ma(dfunc(mdx, keepdims=True, split_every=4),
                 func(mx, keepdims=True))
    assert_eq_ma(dfunc(mdx, axis=0, split_every=2), func(mx, axis=0))
    assert_eq_ma(dfunc(mdx, axis=0, keepdims=True, split_every=2),
                 func(mx, axis=0, keepdims=True))
    assert_eq_ma(dfunc(mdx, axis=1, split_every=2), func(mx, axis=1))
    assert_eq_ma(dfunc(mdx, axis=1, keepdims=True, split_every=2),
                 func(mx, axis=1, keepdims=True))


@pytest.mark.parametrize('reduction', ['argmin', 'argmax'])
def test_arg_reductions(reduction):
    x = np.random.random((10, 10, 10))
    dx = da.from_array(x, chunks=(3, 4, 5))
    mx = np.ma.masked_greater(x, 0.5)
    dmx = da.ma.masked_greater(dx, 0.5)

    dfunc = getattr(da, reduction)
    func = getattr(np, reduction)

    assert_eq_ma(dfunc(dmx), func(mx))
    assert_eq_ma(dfunc(dmx, 0), func(mx, 0))
    assert_eq_ma(dfunc(dmx, 1), func(mx, 1))
    assert_eq_ma(dfunc(dmx, 2), func(mx, 2))


def test_cumulative():
    x = np.random.RandomState(0).rand(20, 24, 13)
    dx = da.from_array(x, chunks=(6, 5, 4))
    mx = np.ma.masked_greater(x, 0.5)
    dmx = da.ma.masked_greater(dx, 0.5)

    for axis in [0, 1, 2]:
        assert_eq_ma(dmx.cumsum(axis=axis), mx.cumsum(axis=axis))
        assert_eq_ma(dmx.cumprod(axis=axis), mx.cumprod(axis=axis))

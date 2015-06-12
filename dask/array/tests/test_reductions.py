from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('numpy')

import dask.array as da
from dask.utils import ignoring
from dask.array.reductions import arg_aggregate
import numpy as np


def eq(a, b):
    if isinstance(a, da.Array):
        a = a.compute()
    if isinstance(b, da.Array):
        b = b.compute()
    if isinstance(a, (np.generic, np.ndarray)):
        return np.allclose(a, b)
    else:
        return a == b


def test_arg_reduction():
    pairs = [([4, 3, 5], [10, 11, 12]),
             ([3, 5, 1], [1, 2, 3])]
    result = arg_aggregate(np.min, np.argmin, (100, 100), pairs)
    assert eq(result, np.array([101, 11, 103]))


def test_reductions_1D():
    x = np.arange(5).astype('f4')
    a = da.from_array(x, chunks=(2,))

    assert eq(da.all(a), np.all(x))
    assert eq(da.any(a), np.any(x))
    assert eq(da.argmax(a, axis=0), np.argmax(x, axis=0))
    assert eq(da.argmin(a, axis=0), np.argmin(x, axis=0))
    assert eq(da.max(a), np.max(x))
    assert eq(da.mean(a), np.mean(x))
    assert eq(da.var(a), np.var(x))
    assert eq(da.min(a), np.min(x))
    assert eq(da.nanargmax(a, axis=0), np.nanargmax(x, axis=0))
    assert eq(da.nanargmin(a, axis=0), np.nanargmin(x, axis=0))
    assert eq(da.nanmax(a), np.nanmax(x))
    assert eq(da.nanmin(a), np.nanmin(x))
    assert eq(da.nansum(a), np.nansum(x))
    assert eq(da.nanvar(a), np.nanvar(x))
    assert eq(da.nanstd(a), np.nanstd(x))


def test_reductions_2D():
    x = np.arange(400).reshape((20, 20))
    a = da.from_array(x, chunks=(7, 7))

    assert eq(a.sum(), x.sum())
    assert eq(a.sum(axis=0), x.sum(axis=0))
    assert eq(a.sum(axis=1), x.sum(axis=1))
    assert eq(a.sum(axis=1, keepdims=True), x.sum(axis=1, keepdims=True))
    assert eq(a.sum(axis=(1, 0)), x.sum(axis=(1, 0)))

    assert eq(a.mean(), x.mean())
    assert eq(a.mean(axis=0), x.mean(axis=0))
    assert eq(a.mean(axis=1), x.mean(axis=1))
    assert eq(a.mean(axis=1, keepdims=True), x.mean(axis=1, keepdims=True))
    assert eq(a.mean(axis=(1, 0)), x.mean(axis=(1, 0)))

    assert eq(a.var(), x.var())
    assert eq(a.var(axis=0), x.var(axis=0))
    assert eq(a.var(axis=1), x.var(axis=1))
    assert eq(a.var(axis=1, keepdims=True), x.var(axis=1, keepdims=True))
    assert eq(a.var(axis=(1, 0)), x.var(axis=(1, 0)))
    # Poorly conditioned
    narr = np.array([1., 2., 3.]*10).reshape((3, 10)) + 1e8
    darr = da.from_array(narr, chunks=5)
    assert eq(darr.var(), narr.var())

    assert eq(a.std(), x.std())
    assert eq(a.std(axis=0), x.std(axis=0))
    assert eq(a.std(axis=1), x.std(axis=1))
    assert eq(a.std(axis=1, keepdims=True), x.std(axis=1, keepdims=True))
    assert eq(a.std(axis=(1, 0)), x.std(axis=(1, 0)))

    def moment(x, n, axis=None):
        return ((x - x.mean(axis=axis, keepdims=True))**n).sum(
                axis=axis)/np.ones_like(x).sum(axis=axis)

    assert eq(a.moment(3), moment(x, 3))
    assert eq(a.moment(4), moment(x, 4))
    assert eq(a.moment(4, axis=1), moment(x, 4, axis=1))
    assert eq(a.moment(4, axis=(1, 0)), moment(x, 4, axis=(1, 0)))

    b = a.sum(keepdims=True)
    assert b._keys() == [[(b.name, 0, 0)]]

    x = np.random.random((20, 20))
    a = da.from_array(x, chunks=(7, 7))
    assert eq(a.argmin(axis=1), x.argmin(axis=1))
    assert eq(a.argmax(axis=0), x.argmax(axis=0))


def test_reductions_with_negative_axes():
    x = np.random.random((4, 4, 4))
    a = da.from_array(x, chunks=2)

    assert eq(a.argmin(axis=-1), x.argmin(axis=-1))

    assert eq(a.sum(axis=-1), x.sum(axis=-1))
    assert eq(a.sum(axis=(0, -1)), x.sum(axis=(0, -1)))


def test_nan():
    x = np.array([[1, np.nan, 3, 4],
                  [5, 6, 7, np.nan],
                  [9, 10, 11, 12]])
    d = da.from_array(x, chunks=(2, 2))

    assert eq(np.nansum(x), da.nansum(d))
    assert eq(np.nansum(x, axis=0), da.nansum(d, axis=0))
    assert eq(np.nanmean(x, axis=1), da.nanmean(d, axis=1))
    assert eq(np.nanmin(x, axis=1), da.nanmin(d, axis=1))
    assert eq(np.nanmax(x, axis=(0, 1)), da.nanmax(d, axis=(0, 1)))
    assert eq(np.nanvar(x), da.nanvar(d))
    assert eq(np.nanstd(x, axis=0), da.nanstd(d, axis=0))
    assert eq(np.nanargmin(x, axis=0), da.nanargmin(d, axis=0))
    assert eq(np.nanargmax(x, axis=0), da.nanargmax(d, axis=0))
    with ignoring(AttributeError):
        assert eq(np.nanprod(x), da.nanprod(d))


def test_dtype():
    x = np.array([[1, 1], [2, 2], [3, 3]], dtype='i1')
    d = da.from_array(x, chunks=2)

    assert eq(d.sum(dtype='i1', axis=1), x.sum(dtype='i1', axis=1))

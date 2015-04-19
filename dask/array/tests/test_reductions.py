from __future__ import absolute_import, division, print_function

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


def test_reductions():
    x = np.random.random((20, 20))
    a = da.from_array(x, chunks=(7, 7))

    assert eq(a.argmin(axis=1), x.argmin(axis=1))
    assert eq(a.argmax(axis=0), x.argmax(axis=0))
    # assert eq(a.argmin(), x.argmin())


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

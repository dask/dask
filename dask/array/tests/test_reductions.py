from __future__ import absolute_import, division, print_function

import dask.array as da
from dask.array.reductions import arg_aggregate
import numpy as np


def eq(a, b):
    if isinstance(a, da.Array):
        a = a.compute()
    if isinstance(b, da.Array):
        b = b.compute()
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_arg_reduction():
    pairs = [([4, 3, 5], [10, 11, 12]),
             ([3, 5, 1], [1, 2, 3])]
    result = arg_aggregate(np.min, np.argmin, (100, 100), pairs)
    assert eq(result, np.array([101, 11, 103]))


def test_reductions():
    x = np.random.random((20, 20))
    a = da.from_array(x, blockshape=(7, 7))

    assert eq(a.argmin(axis=1), x.argmin(axis=1))
    assert eq(a.argmax(axis=0), x.argmax(axis=0))
    # assert eq(a.argmin(), x.argmin())


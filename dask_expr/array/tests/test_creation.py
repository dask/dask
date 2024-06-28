import numpy as np
from dask.array import assert_eq

import dask_expr.array as da


def test_arange():
    assert_eq(da.arange(1, 100, 7), np.arange(1, 100, 7))
    assert_eq(da.arange(100), np.arange(100))
    assert_eq(da.arange(100, like=np.arange(100)), np.arange(100))

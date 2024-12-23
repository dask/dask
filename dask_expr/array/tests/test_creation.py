import numpy as np
import pytest
from dask.array import assert_eq

import dask_expr.array as da


def test_arange():
    assert_eq(da.arange(1, 100, 7), np.arange(1, 100, 7))
    assert_eq(da.arange(100), np.arange(100))
    assert_eq(da.arange(100, like=np.arange(100)), np.arange(100))


def test_linspace():
    assert_eq(da.linspace(1, 100, 30), np.linspace(1, 100, 30))


@pytest.mark.parametrize("func", ["ones", "zeros"])
def test_ones(func):
    assert_eq(getattr(da, func)((10, 20, 15)), getattr(np, func)((10, 20, 15)))
    assert_eq(
        getattr(da, func)((10, 20, 15), dtype="i8"),
        getattr(np, func)((10, 20, 15), dtype="i8"),
    )

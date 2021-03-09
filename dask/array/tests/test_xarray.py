import pytest

import dask.array as da
from ..utils import assert_eq

xr = pytest.importorskip("xarray")


def test_mean():
    y = da.mean(xr.DataArray([1, 2, 3.0]))
    assert isinstance(y, da.Array)
    assert_eq(y, y)


def test_asarray():
    y = da.asarray(xr.DataArray([1, 2, 3.0]))
    assert isinstance(y, da.Array)
    assert_eq(y, y)


def test_asanyarray():
    y = da.asanyarray(xr.DataArray([1, 2, 3.0]))
    assert isinstance(y, da.Array)
    assert_eq(y, y)


def test_asarray_recognises_xarray_data():
    module = xr.DataArray.__module__
    try:
        xr.DataArray.__module__ = "xarray"
        y = da.asarray(xr.DataArray([1, 2, 3.0]))
        assert isinstance(y, da.Array)
        assert type(y._meta).__name__ == "ndarray"
        assert_eq(y, y)
    finally:
        xr.DataArray.__module__ = module

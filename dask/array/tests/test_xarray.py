import pytest

import dask.array as da
from dask.array.utils import assert_eq

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


def test_asarray_xarray_intersphinx_workaround():
    # test that the intersphinx workaround in https://github.com/pydata/xarray/issues/4279 works
    module = xr.DataArray.__module__
    try:
        xr.DataArray.__module__ = "xarray"
        y = da.asarray(xr.DataArray([1, 2, 3.0]))
        assert isinstance(y, da.Array)
        assert type(y._meta).__name__ == "ndarray"
        assert_eq(y, y)
    finally:
        xr.DataArray.__module__ = module

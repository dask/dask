import pytest

import dask.array as da
from ..utils import assert_eq

xr = pytest.importorskip("xarray")


def test_xarray():
    y = da.mean(xr.DataArray([1, 2, 3.0]))

    assert_eq(y, y)

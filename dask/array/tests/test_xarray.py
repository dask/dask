from __future__ import annotations

from functools import partial

import numpy as np
import pytest

import dask
import dask.array as da
from dask.array.utils import assert_eq
from dask.utils import tmpdir

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


def test_fft():
    # Regression test for https://github.com/dask/dask/issues/9679
    coord = da.arange(8, chunks=-1)
    data = da.random.random((8, 8), chunks=-1) + 1
    x = xr.DataArray(data, coords={"x": coord, "y": coord}, dims=["x", "y"])
    result = da.fft.fft(x)
    expected = da.fft.fft(x.data)
    assert_eq(result, expected)


def test_polyfit_reshaping():
    # Regression test for https://github.com/pydata/xarray/issues/4554
    arr = xr.DataArray(da.ones((10, 20, 30), chunks=(1, 5, 30)), dims=["z", "y", "x"])
    result = arr.polyfit("x", 1)
    assert result.polyfit_coefficients.chunks == ((2,), (1,) * 10, (5,) * 4)


def test_positional_indexer_multiple_variables():
    n = 200
    ds = xr.Dataset(
        data_vars=dict(
            a=(
                ["x", "y", "time"],
                da.random.randint(1, 100, (10, 10, n), chunks=(-1, -1, n // 2)),
            ),
            b=(
                ["x", "y", "time"],
                da.random.randint(1, 100, (10, 10, n), chunks=(-1, -1, n // 2)),
            ),
        ),
        coords=dict(
            x=list(range(10)),
            y=list(range(10)),
            time=np.arange(n),
        ),
    )
    indexer = np.arange(n)
    np.random.shuffle(indexer)
    result = ds.isel(time=indexer)
    graph = result.__dask_graph__()
    assert len({k for k in graph if "shuffle-taker" in k}) == 4
    assert len({k for k in graph if "shuffle-sorter" in k}) == 2


@pytest.mark.parametrize("compute", [True, False])
def test_xarray_blockwise_fusion_store(compute):
    def custom_scheduler_get(dsk, keys, expected, **kwargs):
        dsk = dsk.__dask_graph__()
        assert (
            len(dsk) == expected
        ), f"False number of tasks got {len(dsk)} but expected {expected}"
        return [42 for _ in keys]

    # First test that this mocking stuff works as expecged
    with pytest.raises(AssertionError, match="False number of tasks"):
        scheduler = partial(custom_scheduler_get, expected=42)
        dask.compute(da.ones(10), scheduler=scheduler)

    coord = da.arange(8, chunks=-1)
    data = da.random.random((8, 8), chunks=-1) + 1
    x = xr.DataArray(data, coords={"x": coord, "y": coord}, dims=["x", "y"])

    y = ((x + 1) * 2) / 2 - 1

    # Everything fused into one compute task
    # one finalize Alias
    expected = 2
    scheduler = partial(custom_scheduler_get, expected=expected)
    dask.compute(y, scheduler=scheduler)

    with tmpdir() as dirname:
        if compute:
            with dask.config.set(scheduler=scheduler):
                y.to_zarr(dirname, compute=True)
        else:
            # There's a delayed finalize store smashed on top which won't be fused by
            # default
            expected += 1
            scheduler = partial(custom_scheduler_get, expected=expected)
            stored = y.to_zarr(dirname, compute=False)
            dask.compute(stored, scheduler=scheduler)

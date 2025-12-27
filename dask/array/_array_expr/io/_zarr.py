from __future__ import annotations


def from_zarr(*args, **kwargs):
    """Forward to dask.array.core.from_zarr"""
    from dask.array.core import from_zarr as _from_zarr

    return _from_zarr(*args, **kwargs)


def to_zarr(*args, **kwargs):
    """Forward to dask.array.core.to_zarr"""
    from dask.array.core import to_zarr as _to_zarr

    return _to_zarr(*args, **kwargs)

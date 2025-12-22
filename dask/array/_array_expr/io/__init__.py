"""IO functions for array-expr."""

from __future__ import annotations

from dask.array._array_expr.io._base import IO
from dask.array._array_expr.io._from_array import FromArray
from dask.array._array_expr.io._from_delayed import FromDelayed, from_delayed
from dask.array._array_expr.io._from_graph import FromGraph
from dask.array._array_expr.io._from_npy_stack import FromNpyStack, from_npy_stack
from dask.array._array_expr.io._store import get_scheduler_lock, store
from dask.array._array_expr.io._to_npy_stack import to_npy_stack
from dask.array._array_expr.io._zarr import from_zarr, to_zarr

__all__ = [
    "IO",
    "FromArray",
    "FromDelayed",
    "FromGraph",
    "FromNpyStack",
    "from_delayed",
    "from_npy_stack",
    "from_zarr",
    "get_scheduler_lock",
    "store",
    "to_npy_stack",
    "to_zarr",
]

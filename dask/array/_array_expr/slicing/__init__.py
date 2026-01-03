"""Slicing operations for dask array expressions."""

from dask.array._array_expr.slicing._basic import (
    ArrayOffsetDep,
    Slice,
    SliceSlicesIntegers,
    TakeUnknownOneChunk,
    normalize_index,
    slice_array,
    slice_slices_and_integers,
    slice_with_int_dask_array,
    slice_with_int_dask_array_on_axis,
    slice_with_newaxes,
    slice_wrap_lists,
    take,
)
from dask.array._array_expr.slicing._bool_index import (
    BooleanIndexFlattened,
    getitem_variadic,
    slice_with_bool_dask_array,
)
from dask.array._array_expr.slicing._setitem import (
    ConcatenateArrayChunks,
    SetItem,
    concatenate_array_chunks_expr,
    setitem_array_expr,
)
from dask.array._array_expr.slicing._squeeze import Squeeze, squeeze
from dask.array._array_expr.slicing._vindex import (
    VIndexArray,
    _numpy_vindex,
    _vindex,
    _vindex_array,
)

__all__ = [
    # Basic slicing
    "ArrayOffsetDep",
    "Slice",
    "SliceSlicesIntegers",
    "TakeUnknownOneChunk",
    "normalize_index",
    "slice_array",
    "slice_slices_and_integers",
    "slice_with_int_dask_array",
    "slice_with_int_dask_array_on_axis",
    "slice_with_newaxes",
    "slice_wrap_lists",
    "take",
    # Boolean indexing
    "BooleanIndexFlattened",
    "getitem_variadic",
    "slice_with_bool_dask_array",
    # Setitem
    "ConcatenateArrayChunks",
    "SetItem",
    "concatenate_array_chunks_expr",
    "setitem_array_expr",
    # Squeeze
    "Squeeze",
    "squeeze",
    # Vindex
    "VIndexArray",
    "_numpy_vindex",
    "_vindex",
    "_vindex_array",
]

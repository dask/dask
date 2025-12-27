"""Index manipulation functions for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asarray, stack
from dask.base import is_dask_collection
from dask.utils import derived_from


def _unravel_index_kernel(indices, func_kwargs):
    return np.stack(np.unravel_index(indices, **func_kwargs))


@derived_from(np)
def unravel_index(indices, shape, order="C"):
    from dask.array._array_expr.creation import empty

    indices = asarray(indices)
    if shape and indices.size:
        unraveled_indices = tuple(
            indices.map_blocks(
                _unravel_index_kernel,
                dtype=np.intp,
                chunks=(((len(shape),),) + indices.chunks),
                new_axis=0,
                func_kwargs={"shape": shape, "order": order},
            )
        )
    else:
        unraveled_indices = tuple(empty((0,), dtype=np.intp, chunks=1) for i in shape)

    return unraveled_indices


@derived_from(np)
def ravel_multi_index(multi_index, dims, mode="raise", order="C"):
    from dask.array._array_expr.routines._broadcast import broadcast_arrays

    if np.isscalar(dims):
        dims = (dims,)
    if is_dask_collection(dims) or any(is_dask_collection(d) for d in dims):
        raise NotImplementedError(
            f"Dask types are not supported in the `dims` argument: {dims!r}"
        )

    if hasattr(multi_index, "ndim") and multi_index.ndim > 0:
        # It's an array-like
        index_stack = asarray(multi_index)
    else:
        multi_index_arrs = broadcast_arrays(*multi_index)
        index_stack = stack(multi_index_arrs)

    if not np.isnan(index_stack.shape).any() and len(index_stack) != len(dims):
        raise ValueError(
            f"parameter multi_index must be a sequence of length {len(dims)}"
        )
    if not np.issubdtype(index_stack.dtype, np.signedinteger):
        raise TypeError("only int indices permitted")
    return index_stack.map_blocks(
        np.ravel_multi_index,
        dtype=np.intp,
        chunks=index_stack.chunks[1:],
        drop_axis=0,
        dims=dims,
        mode=mode,
        order=order,
    )

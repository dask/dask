"""Top-k selection for array-expr."""

from __future__ import annotations

from functools import partial
from numbers import Number

import numpy as np

from dask.array._array_expr._collection import asarray
from dask.array.utils import validate_axis


def topk(a, k, axis=-1, split_every=None):
    """Extract the k largest elements from a on the given axis.

    Returns them sorted from largest to smallest. If k is negative,
    extract the -k smallest elements instead, and return them sorted
    from smallest to largest.
    """
    from dask.array import chunk
    from dask.array._array_expr.reductions import reduction

    a = asarray(a)
    axis = validate_axis(axis, a.ndim)

    chunk_combine = partial(chunk.topk, k=k)
    aggregate = partial(chunk.topk_aggregate, k=k)

    return reduction(
        a,
        chunk=chunk_combine,
        combine=chunk_combine,
        aggregate=aggregate,
        axis=axis,
        keepdims=True,
        dtype=a.dtype,
        split_every=split_every,
        output_size=abs(k),
    )


def argtopk(a, k, axis=-1, split_every=None):
    """Extract the indices of the k largest elements from a on the given axis.

    Returns them sorted from largest to smallest. If k is negative,
    extract the indices of the -k smallest elements instead.
    """
    from dask.array import chunk
    from dask.array._array_expr.creation import arange
    from dask.array._array_expr.reductions import reduction

    a = asarray(a)
    axis = validate_axis(axis, a.ndim)

    idx = arange(a.shape[axis], chunks=(a.chunks[axis],), dtype=np.intp)
    idx = idx[tuple(slice(None) if i == axis else np.newaxis for i in range(a.ndim))]
    a_plus_idx = a.map_blocks(chunk.argtopk_preprocess, idx, dtype=object)

    chunk_combine = partial(chunk.argtopk, k=k)
    aggregate = partial(chunk.argtopk_aggregate, k=k)

    if isinstance(axis, Number):
        naxis = 1
    else:
        naxis = len(axis)

    meta = a._meta.astype(np.intp).reshape((0,) * (a.ndim - naxis + 1))

    return reduction(
        a_plus_idx,
        chunk=chunk_combine,
        combine=chunk_combine,
        aggregate=aggregate,
        axis=axis,
        keepdims=True,
        dtype=np.intp,
        split_every=split_every,
        concatenate=False,
        output_size=abs(k),
        meta=meta,
    )

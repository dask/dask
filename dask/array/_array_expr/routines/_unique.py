"""Unique implementation for array-expr."""

from __future__ import annotations

from functools import cached_property

import numpy as np

from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._collection import (
    asarray,
    concatenate,
    new_collection,
    ravel,
)
from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import meta_from_array
from dask.utils import derived_from


def _unique_internal(ar, indices, counts, return_inverse=False):
    """Helper function for np.unique with structured array output."""
    return_index = indices is not None
    return_counts = counts is not None

    u = np.unique(ar)

    dt = [("values", u.dtype)]
    if return_index:
        dt.append(("indices", np.intp))
    if return_inverse:
        dt.append(("inverse", np.intp))
    if return_counts:
        dt.append(("counts", np.intp))

    r = np.empty(u.shape, dtype=dt)
    r["values"] = u
    if return_inverse:
        r["inverse"] = np.arange(len(r), dtype=np.intp)
    if return_index or return_counts:
        for i, v in enumerate(r["values"]):
            m = ar == v
            if return_index:
                indices[m].min(keepdims=True, out=r["indices"][i : i + 1])
            if return_counts:
                counts[m].sum(keepdims=True, out=r["counts"][i : i + 1])

    return r


class UniqueChunked(ArrayExpr):
    """Expression for per-chunk unique computation."""

    _parameters = ["x", "indices", "counts", "out_dtype"]
    _defaults = {"indices": None, "counts": None}

    @cached_property
    def _meta(self):
        return np.empty((0,), dtype=self.out_dtype)

    @cached_property
    def chunks(self):
        nchunks = len(self.x.chunks[0])
        return ((np.nan,) * nchunks,)

    @cached_property
    def _name(self):
        return f"unique-chunk-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        for i in range(len(self.x.chunks[0])):
            key = (self._name, i)
            x_ref = TaskRef((self.x._name, i))
            idx_ref = (
                TaskRef((self.indices._name, i)) if self.indices is not None else None
            )
            cnt_ref = (
                TaskRef((self.counts._name, i)) if self.counts is not None else None
            )
            dsk[key] = Task(key, _unique_internal, x_ref, idx_ref, cnt_ref, False)
        return dsk

    @property
    def _dependencies(self):
        deps = [self.x]
        if self.indices is not None:
            deps.append(self.indices)
        if self.counts is not None:
            deps.append(self.counts)
        return deps


def _unique_aggregate_func(chunks, return_inverse):
    """Aggregate unique results from multiple chunks."""
    combined = np.concatenate(chunks)
    return_index = "indices" in combined.dtype.names
    return_counts = "counts" in combined.dtype.names

    return _unique_internal(
        combined["values"],
        combined["indices"] if return_index else None,
        combined["counts"] if return_counts else None,
        return_inverse=return_inverse,
    )


class UniqueAggregate(ArrayExpr):
    """Expression for aggregating unique results from all chunks."""

    _parameters = ["chunked", "return_inverse", "out_dtype"]
    _defaults = {"return_inverse": False}

    @cached_property
    def _meta(self):
        return np.empty((0,), dtype=self.out_dtype)

    @cached_property
    def chunks(self):
        return ((np.nan,),)

    @cached_property
    def _name(self):
        return f"unique-aggregate-{self.deterministic_token}"

    def _layer(self):
        chunk_keys = [
            (self.chunked._name, i) for i in range(len(self.chunked.chunks[0]))
        ]
        key = (self._name, 0)
        chunks_list = List(*[TaskRef(k) for k in chunk_keys])
        dsk = {key: Task(key, _unique_aggregate_func, chunks_list, self.return_inverse)}
        return dsk

    @property
    def _dependencies(self):
        return [self.chunked]


def unique_no_structured_arr(
    ar, return_index=False, return_inverse=False, return_counts=False
):
    """Simplified version of unique for arrays that don't support structured arrays."""
    from dask.array._array_expr._blockwise import Blockwise
    from dask.array._array_expr._expr import ChunksOverride
    from dask.array._array_expr._reductions import _tree_reduce

    if return_index or return_inverse or return_counts:
        raise ValueError(
            "dask.array.unique does not support `return_index`, `return_inverse` "
            "or `return_counts` with array types that don't support structured arrays."
        )

    ar = ravel(ar)
    out = Blockwise(np.unique, "i", ar, "i", dtype=ar.dtype)
    chunked = new_collection(out)
    chunked = new_collection(
        ChunksOverride(chunked.expr, ((np.nan,) * len(ar.chunks[0]),))
    )

    def _unique_agg(arrays, axis, keepdims):
        if not isinstance(arrays, list):
            arrays = [arrays]
        return np.unique(np.concatenate(arrays))

    return _tree_reduce(
        chunked.expr,
        aggregate=_unique_agg,
        axis=(0,),
        keepdims=False,
        dtype=ar.dtype,
        concatenate=False,
    )


@derived_from(np)
def unique(ar, return_index=False, return_inverse=False, return_counts=False):
    """Find the unique elements of an array."""
    from dask.array._array_expr._creation import arange, ones
    from dask.array.numpy_compat import NUMPY_GE_200

    try:
        meta = meta_from_array(ar)
        np.empty_like(meta, dtype=[("a", int), ("b", float)])
    except TypeError:
        return unique_no_structured_arr(
            ar,
            return_index=return_index,
            return_inverse=return_inverse,
            return_counts=return_counts,
        )

    orig_shape = ar.shape
    ar = ravel(ar)

    out_dtype = [("values", ar.dtype)]
    indices_arr = None
    counts_arr = None

    if return_index:
        indices_arr = arange(ar.shape[0], dtype=np.intp, chunks=ar.chunks[0])
        out_dtype.append(("indices", np.intp))
    if return_counts:
        counts_arr = ones((ar.shape[0],), dtype=np.intp, chunks=ar.chunks[0])
        out_dtype.append(("counts", np.intp))

    out_dtype = np.dtype(out_dtype)
    chunked = UniqueChunked(
        ar.expr,
        indices_arr.expr if indices_arr is not None else None,
        counts_arr.expr if counts_arr is not None else None,
        out_dtype,
    )

    final_dtype = (
        out_dtype
        if not return_inverse
        else np.dtype(list(out_dtype.descr) + [("inverse", np.intp)])
    )
    aggregated = new_collection(UniqueAggregate(chunked, return_inverse, final_dtype))

    result = [aggregated["values"]]
    if return_index:
        result.append(aggregated["indices"])
    if return_inverse:
        matches = (ar[:, None] == aggregated["values"][None, :]).astype(np.intp)
        inverse = (matches * aggregated["inverse"]).sum(axis=1)
        if NUMPY_GE_200:
            from dask.array._array_expr._reshape import reshape

            inverse = reshape(inverse, orig_shape)
        result.append(inverse)
    if return_counts:
        result.append(aggregated["counts"])

    if len(result) == 1:
        return result[0]
    return tuple(result)


@derived_from(np)
def union1d(ar1, ar2):
    """Find the union of two arrays."""
    ar1 = asarray(ar1)
    ar2 = asarray(ar2)
    return unique(concatenate((ravel(ar1), ravel(ar2))))

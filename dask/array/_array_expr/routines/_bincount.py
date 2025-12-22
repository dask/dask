"""Bincount implementation for array-expr."""

from __future__ import annotations

from functools import cached_property, partial

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._collection import asarray, new_collection
from dask.array._array_expr._expr import ArrayExpr
from dask.utils import derived_from


def _bincount_chunk(x, weights, minlength):
    """Apply bincount to a single chunk, wrapping result in extra dimension."""
    if weights is not None:
        result = np.bincount(x, weights=weights, minlength=minlength)
    else:
        result = np.bincount(x, minlength=minlength)
    return result[np.newaxis]


def _bincount_sum(bincounts, axis, keepdims, dtype=None):
    """Sum bincount results, handling variable lengths when minlength=0."""
    if not isinstance(bincounts, list):
        bincounts = [bincounts]

    n = max(b.shape[1] for b in bincounts)
    out = np.zeros((1, n), dtype=bincounts[0].dtype)
    for b in bincounts:
        out[0, : b.shape[1]] += b[0]

    if not keepdims:
        return out[0]
    return out


class BincountChunked(ArrayExpr):
    """Expression for per-chunk bincount computation."""

    _parameters = ["x", "weights", "minlength", "output_size", "meta_provided"]
    _defaults = {"weights": None}

    @cached_property
    def _meta(self):
        return np.empty((0, 0), dtype=self.meta_provided.dtype)

    @cached_property
    def chunks(self):
        nchunks = len(self.x.chunks[0])
        return ((1,) * nchunks, (self.output_size,))

    @cached_property
    def _name(self):
        return f"bincount-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        minlen = self.minlength
        for i in range(len(self.x.chunks[0])):
            key = (self._name, i, 0)
            x_ref = TaskRef((self.x._name, i))
            w_ref = (
                TaskRef((self.weights._name, i)) if self.weights is not None else None
            )
            dsk[key] = Task(key, _bincount_chunk, x_ref, w_ref, minlen)
        return dsk

    @property
    def _dependencies(self):
        deps = [self.x]
        if self.weights is not None:
            deps.append(self.weights)
        return deps


@derived_from(np)
def bincount(x, weights=None, minlength=0, split_every=None):
    """Count number of occurrences of each value in array of non-negative ints."""
    from dask.array._array_expr.reductions import _tree_reduce

    x = asarray(x)
    if x.ndim != 1:
        raise ValueError("Input array must be one dimensional. Try using x.ravel()")
    if weights is not None:
        weights = asarray(weights)
        if weights.chunks != x.chunks:
            raise ValueError("Chunks of input array x and weights must match.")

    if weights is not None:
        meta = np.bincount([1], weights=np.array([1], dtype=weights.dtype))
    else:
        meta = np.bincount([])

    if minlength == 0:
        output_size = np.nan
    else:
        output_size = minlength

    chunked_counts = new_collection(
        BincountChunked(x, weights, minlength, output_size, meta_provided=meta)
    )

    if minlength > 0:
        return chunked_counts.sum(axis=0)
    else:
        return _tree_reduce(
            chunked_counts,
            aggregate=partial(_bincount_sum, dtype=meta.dtype),
            axis=(0,),
            keepdims=False,
            dtype=meta.dtype,
            split_every=split_every,
            concatenate=False,
        )

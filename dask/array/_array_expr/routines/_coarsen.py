"""Coarsen implementation for array-expr."""

from __future__ import annotations

import itertools
from functools import cached_property, partial

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._collection import asarray, new_collection
from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import meta_from_array


def _partition(n, size):
    """Partition n into evenly distributed sizes."""
    quotient, remainder = divmod(n, size)
    return [size] * quotient, [remainder] if remainder else []


def aligned_coarsen_chunks(chunks, multiple):
    """Returns a new chunking aligned with the coarsening multiple."""
    chunks = np.asarray(chunks)
    overflow = chunks % multiple
    excess = overflow.sum()
    new_chunks = chunks - overflow
    chunk_validity = new_chunks == chunks
    valid_inds, invalid_inds = np.where(chunk_validity)[0], np.where(~chunk_validity)[0]
    chunk_modification_order = [
        *invalid_inds[np.argsort(new_chunks[invalid_inds])],
        *valid_inds[np.argsort(new_chunks[valid_inds])],
    ]
    partitioned_excess, remainder = _partition(excess, multiple)
    for idx, extra in enumerate(partitioned_excess):
        new_chunks[chunk_modification_order[idx]] += extra
    new_chunks = np.array([*new_chunks, *remainder])
    new_chunks = new_chunks[new_chunks > 0]
    return tuple(new_chunks)


class Coarsen(ArrayExpr):
    """Expression class for coarsen operation."""

    _parameters = ["x", "reduction", "axes", "trim_excess", "kwargs"]
    _defaults = {"trim_excess": False, "kwargs": None}

    @cached_property
    def _reduction(self):
        reduction = self.reduction
        if reduction.__module__.startswith("dask."):
            return getattr(np, reduction.__name__)
        return reduction

    @cached_property
    def _kwargs(self):
        return self.kwargs or {}

    @cached_property
    def _meta(self):
        x = self.x
        meta = self._reduction(np.empty((1,) * x.ndim, dtype=x.dtype), **self._kwargs)
        return meta_from_array(meta, ndim=x.ndim)

    @cached_property
    def chunks(self):
        x = self.x
        axes = self.axes
        coarsen_dim = lambda dim, ax: int(dim // axes.get(ax, 1))
        return tuple(
            tuple(coarsen_dim(bd, i) for bd in bds if coarsen_dim(bd, i) > 0)
            for i, bds in enumerate(x.chunks)
        )

    def _layer(self):
        from dask.array import chunk

        x = self.x
        axes = self.axes
        name = self._name
        dsk = {}

        in_ranges = [range(len(c)) for c in x.chunks]
        for in_idx in itertools.product(*in_ranges):
            in_key = (x._name,) + in_idx
            out_key = (name,) + in_idx
            func = partial(
                chunk.coarsen,
                self._reduction,
                axes=axes,
                trim_excess=self.trim_excess,
                **self._kwargs,
            )
            dsk[out_key] = Task(out_key, func, TaskRef(in_key))

        return dsk


def coarsen(reduction, x, axes, trim_excess=False, **kwargs):
    """Coarsen array by applying reduction to fixed size neighborhoods."""
    x = asarray(x)

    if not trim_excess and not all(x.shape[i] % div == 0 for i, div in axes.items()):
        msg = f"Coarsening factors {axes} do not align with array shape {x.shape}."
        raise ValueError(msg)

    new_chunks = {}
    for i, div in axes.items():
        aligned = aligned_coarsen_chunks(x.chunks[i], div)
        if aligned != x.chunks[i]:
            new_chunks[i] = aligned
    if new_chunks:
        x = x.rechunk(new_chunks)

    return new_collection(Coarsen(x, reduction, axes, trim_excess, kwargs or None))

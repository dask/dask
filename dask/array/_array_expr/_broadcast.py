from __future__ import annotations

import functools
from itertools import product

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import normalize_chunks
from dask.array.utils import meta_from_array
from dask.blockwise import lol_tuples


class BroadcastTo(ArrayExpr):
    """Broadcast an array to a new shape."""

    _parameters = ["array", "_shape", "_chunks", "_meta_override"]
    _defaults = {"_meta_override": None}

    @functools.cached_property
    def _name(self):
        return f"broadcast_to-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        meta_override = self.operand("_meta_override")
        # Only use meta_override if it has the correct ndim
        if meta_override is not None and hasattr(meta_override, 'ndim') and meta_override.ndim == len(self._shape):
            return meta_override
        return meta_from_array(self.array._meta, ndim=len(self._shape))

    @functools.cached_property
    def chunks(self):
        return self._chunks

    def _layer(self) -> dict:
        x = self.array
        shape = self._shape
        chunks = self._chunks
        ndim_new = len(shape) - x.ndim

        dsk = {}
        enumerated_chunks = product(*(enumerate(bds) for bds in chunks))
        for ec in enumerated_chunks:
            new_index, chunk_shape = zip(*ec)
            old_index = tuple(
                0 if bd == (1,) else i for bd, i in zip(x.chunks, new_index[ndim_new:])
            )
            old_key = (x._name,) + old_index
            new_key = (self._name,) + new_index
            dsk[new_key] = Task(new_key, np.broadcast_to, TaskRef(old_key), chunk_shape)

        return dsk


def broadcast_to(x, shape, chunks=None, meta=None):
    """Broadcast an array to a new shape.

    Parameters
    ----------
    x : array_like
        The array to broadcast.
    shape : tuple
        The shape of the desired array.
    chunks : tuple, optional
        If provided, then the result will use these chunks instead of the same
        chunks as the source array.
    meta : empty ndarray, optional
        empty ndarray created with same NumPy backend, ndim and dtype as the
        Dask Array being created

    Returns
    -------
    BroadcastTo expression
    """
    from dask.array._array_expr._collection import asarray

    x = asarray(x)
    shape = tuple(shape)

    if meta is None:
        meta = meta_from_array(x._meta)

    # Identity case
    if x.shape == shape and (chunks is None or chunks == x.chunks):
        return x.expr

    ndim_new = len(shape) - x.ndim
    if ndim_new < 0 or any(
        new != old for new, old in zip(shape[ndim_new:], x.shape) if old != 1
    ):
        raise ValueError(f"cannot broadcast shape {x.shape} to shape {shape}")

    if chunks is None:
        chunks = tuple((s,) for s in shape[:ndim_new]) + tuple(
            bd if old > 1 else (new,)
            for bd, old, new in zip(x.chunks, x.shape, shape[ndim_new:])
        )
    else:
        chunks = normalize_chunks(
            chunks, shape, dtype=x.dtype, previous_chunks=x.chunks
        )
        for old_bd, new_bd in zip(x.chunks, chunks[ndim_new:]):
            if old_bd != new_bd and old_bd != (1,):
                raise ValueError(
                    f"cannot broadcast chunks {x.chunks} to chunks {chunks}: "
                    "new chunks must either be along a new "
                    "dimension or a dimension of size 1"
                )

    return BroadcastTo(x.expr, shape, chunks, meta)

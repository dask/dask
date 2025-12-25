from __future__ import annotations

import functools
from itertools import product

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import normalize_chunks
from dask.array.utils import meta_from_array


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
        if (
            meta_override is not None
            and hasattr(meta_override, "ndim")
            and meta_override.ndim == len(self._shape)
        ):
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

    def _simplify_up(self, parent, dependents):
        """Allow slice and shuffle operations to push through BroadcastTo."""
        from dask.array._array_expr._shuffle import Shuffle
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        if isinstance(parent, Shuffle):
            return self._accept_shuffle(parent)
        return None

    def _accept_shuffle(self, shuffle_expr):
        """Accept a shuffle being pushed through BroadcastTo.

        - Shuffle on a new dimension (added by broadcast): can't push through
        - Shuffle on dimension broadcast from size 1: no-op, return self
        - Shuffle on dimension with real data: push through to input
        """
        from dask.array._array_expr._shuffle import Shuffle

        axis = shuffle_expr.axis
        ndim_new = len(self._shape) - self.array.ndim

        # Shuffle on a new dimension (added by broadcast) - can't push through
        if axis < ndim_new:
            return None

        # Map to input axis
        input_axis = axis - ndim_new
        input_size = self.array.shape[input_axis]

        # If input dimension is size 1 (broadcasted), shuffle is a no-op
        if input_size == 1:
            return self

        # Push shuffle through to input
        shuffled_input = Shuffle(
            self.array,
            shuffle_expr.indexer,
            input_axis,
            shuffle_expr.operand("name"),
        )
        return BroadcastTo(shuffled_input, self._shape, self._chunks, self._meta)

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through BroadcastTo.

        For broadcast_to(x, shape)[slices]:
        - Dimensions added by broadcast (new dims): affect output shape only
        - Dimensions from input with size > 1: push slice to input
        - Dimensions from input with size == 1: affect output shape only
        """
        from numbers import Integral

        from dask._collections import new_collection

        input_arr = self.array
        output_shape = self._shape
        index = slice_expr.index

        # Pad index to full length
        full_index = index + (slice(None),) * (len(output_shape) - len(index))

        # For now, only handle simple slices
        if any(isinstance(idx, Integral) for idx in full_index):
            return None
        if any(idx is None for idx in full_index):
            return None

        ndim_new = len(output_shape) - input_arr.ndim

        # Compute new output shape and input slices
        new_output_shape = []
        input_slices = []

        for out_dim, idx in enumerate(full_index):
            if not isinstance(idx, slice):
                return None

            out_size = output_shape[out_dim]
            start, stop, step = idx.indices(out_size)
            if step != 1:
                return None
            new_dim_size = max(0, stop - start)
            new_output_shape.append(new_dim_size)

            # Check if this dimension maps to input
            if out_dim >= ndim_new:
                in_dim = out_dim - ndim_new
                in_size = input_arr.shape[in_dim]

                if in_size == 1:
                    # Broadcasted from size 1 - can't push, just take full slice
                    input_slices.append(slice(None))
                else:
                    # Real dimension - push the slice
                    input_slices.append(idx)

        # Slice the input array
        if input_slices:
            sliced_input = new_collection(input_arr)[tuple(input_slices)]
        else:
            sliced_input = new_collection(input_arr)

        # Compute new chunks for the output
        # For dimensions from input: use input's (sliced) chunks
        # For new dimensions: use the new output shape (single chunk)
        old_chunks = self._chunks
        new_chunks = []
        for out_dim, old_chunk in enumerate(old_chunks):
            if out_dim >= ndim_new:
                in_dim = out_dim - ndim_new
                in_size = input_arr.shape[in_dim]
                if in_size == 1:
                    # Broadcasted - compute new chunks from old
                    idx = full_index[out_dim]
                    start, stop, _ = idx.indices(output_shape[out_dim])
                    new_chunks.append(
                        self._slice_chunks(old_chunk, start, stop - start)
                    )
                else:
                    # Use sliced input's chunks
                    new_chunks.append(sliced_input.expr.chunks[in_dim])
            else:
                # New dimension - compute from slice
                idx = full_index[out_dim]
                start, stop, _ = idx.indices(output_shape[out_dim])
                new_chunks.append(self._slice_chunks(old_chunk, start, stop - start))

        # Compute meta for the new broadcast
        new_meta = meta_from_array(sliced_input.expr._meta)

        # Create new BroadcastTo
        return BroadcastTo(
            sliced_input.expr,
            tuple(new_output_shape),
            tuple(new_chunks),
            new_meta,
        )

    def _slice_chunks(self, chunks, start, length):
        """Compute new chunks after slicing."""
        if length == 0:
            return ()

        result = []
        pos = 0
        remaining = length
        for chunk_size in chunks:
            chunk_start = pos
            chunk_end = pos + chunk_size
            pos = chunk_end

            if chunk_end <= start:
                continue
            if chunk_start >= start + length:
                break

            # Overlap with the slice
            overlap_start = max(chunk_start, start)
            overlap_end = min(chunk_end, start + length)
            overlap_size = overlap_end - overlap_start

            if overlap_size > 0:
                result.append(overlap_size)
                remaining -= overlap_size

        return tuple(result)


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
    Array
    """
    from dask._collections import new_collection
    from dask.array._array_expr._collection import asarray

    x = asarray(x)
    shape = tuple(shape)

    if meta is None:
        meta = meta_from_array(x._meta)

    # Identity case
    if x.shape == shape and (chunks is None or chunks == x.chunks):
        return new_collection(x.expr)

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

    return new_collection(BroadcastTo(x.expr, shape, chunks, meta))

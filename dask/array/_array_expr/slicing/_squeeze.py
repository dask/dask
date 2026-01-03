from __future__ import annotations

import functools
from itertools import product

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import meta_from_array


class Squeeze(ArrayExpr):
    """Remove axes of length one from array."""

    _parameters = ["array", "axis"]

    @functools.cached_property
    def _name(self):
        return f"squeeze-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array._meta, ndim=len(self.chunks))

    @functools.cached_property
    def _axis_set(self):
        """Normalized axis as a set of positive integers."""
        axis = self.axis
        if axis is None:
            return set(i for i, d in enumerate(self.array.shape) if d == 1)
        if not isinstance(axis, tuple):
            axis = (axis,)
        # Normalize negative indices
        return set(i % self.array.ndim for i in axis)

    @functools.cached_property
    def chunks(self):
        return tuple(
            c for i, c in enumerate(self.array.chunks) if i not in self._axis_set
        )

    def _layer(self) -> dict:
        # Map from output chunk indices to input chunk indices
        # Input has more dimensions than output
        in_chunks = self.array.chunks
        out_chunks = self.chunks
        axis_set = self._axis_set

        # Generate all output chunk indices
        out_chunk_tuples = list(product(*(range(len(c)) for c in out_chunks)))

        dsk = {}
        for out_idx in out_chunk_tuples:
            # Build input index by inserting 0 at squeezed positions
            in_idx = []
            out_pos = 0
            for i in range(len(in_chunks)):
                if i in axis_set:
                    in_idx.append(0)
                else:
                    in_idx.append(out_idx[out_pos])
                    out_pos += 1

            out_key = (self._name,) + tuple(out_idx)
            in_key = (self.array._name,) + tuple(in_idx)

            # Build squeeze axis for this chunk (relative to input chunk dimensions)
            chunk_axis = tuple(sorted(axis_set))

            dsk[out_key] = Task(out_key, np.squeeze, TaskRef(in_key), axis=chunk_axis)

        return dsk


def squeeze(a, axis=None):
    """Remove axes of length one from array.

    Parameters
    ----------
    a : Array
        Input array
    axis : None or int or tuple of ints, optional
        Selects a subset of entries of length one in the shape.

    Returns
    -------
    squeezed : Array
    """
    from dask._collections import new_collection
    from dask.array.utils import validate_axis

    if axis is None:
        axis = tuple(i for i, d in enumerate(a.shape) if d == 1)
    elif not isinstance(axis, tuple):
        axis = (axis,)

    if any(a.shape[i] != 1 for i in axis):
        raise ValueError("cannot squeeze axis with size other than one")

    axis = validate_axis(axis, a.ndim)

    return new_collection(Squeeze(a.expr, axis))

from __future__ import annotations

import functools
from functools import reduce
from itertools import product
from operator import mul

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.reshape import reshape_rechunk
from dask.array.slicing import sanitize_index
from dask.array.utils import meta_from_array
from dask.utils import M


class Reshape(ArrayExpr):
    """Reshape array to new shape.

    This is the high-level expression that gets lowered to ReshapeLowered.
    The lowering step computes the required rechunking.
    """

    _parameters = ["array", "_shape"]

    def __new__(cls, *args, **kwargs):
        # Call parent __new__ to create the instance
        instance = super().__new__(cls, *args, **kwargs)
        # Eagerly validate by computing chunks (which calls reshape_rechunk)
        # This ensures NotImplementedError is raised at creation time
        _ = instance.chunks
        return instance

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array._meta, ndim=len(self._shape))

    @functools.cached_property
    def _reshape_chunks(self):
        """Compute input and output chunks for reshape."""
        inchunks, outchunks, _, _ = reshape_rechunk(
            self.array.shape, self._shape, self.array.chunks
        )
        return inchunks, outchunks

    @property
    def _inchunks(self):
        return self._reshape_chunks[0]

    @property
    def _outchunks(self):
        return self._reshape_chunks[1]

    @functools.cached_property
    def chunks(self):
        return self._outchunks

    def _lower(self):
        """Lower to ReshapeLowered with the rechunked array as an operand."""
        if self._inchunks == self.array.chunks:
            rechunked = self.array
        else:
            rechunked = self.array.rechunk(self._inchunks)
        return ReshapeLowered(rechunked, self._shape, self._outchunks)


class ReshapeLowered(ArrayExpr):
    """Lowered reshape expression with rechunked input as operand."""

    _parameters = ["array", "_shape", "_outchunks"]

    @functools.cached_property
    def _name(self):
        return f"reshape-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array._meta, ndim=len(self._shape))

    @functools.cached_property
    def chunks(self):
        return self._outchunks

    def _layer(self) -> dict:
        inchunks = self.array.chunks
        outchunks = self._outchunks

        in_keys = list(product([self.array._name], *[range(len(c)) for c in inchunks]))
        out_keys = list(product([self._name], *[range(len(c)) for c in outchunks]))
        shapes = list(product(*outchunks))

        dsk = {
            out_key: Task(out_key, M.reshape, TaskRef(in_key), shape)
            for out_key, in_key, shape in zip(out_keys, in_keys, shapes)
        }
        return dsk


def reshape(x, shape, merge_chunks=True, limit=None):
    """Reshape array to new shape.

    Parameters
    ----------
    x : Array
        Input array
    shape : int or tuple of ints
        The new shape should be compatible with the original shape. If
        an integer, then the result will be a 1-D array of that length.
        One shape dimension can be -1. In this case, the value is
        inferred from the length of the array and remaining dimensions.
    merge_chunks : bool, default True
        Whether to merge chunks using the logic in :meth:`dask.array.rechunk`
        when communication is necessary given the input array chunking and
        the output shape.
    limit : int (optional)
        The maximum block size to target in bytes.

    Returns
    -------
    reshaped : Array
    """
    # Normalize shape
    if isinstance(shape, int):
        shape = (shape,)
    shape = tuple(map(sanitize_index, shape))

    # Handle -1 in shape
    known_sizes = [s for s in shape if s != -1]
    if len(known_sizes) < len(shape):
        if len(shape) - len(known_sizes) > 1:
            raise ValueError("can only specify one unknown dimension")
        # Fastpath for x.reshape(-1) on 1D arrays
        if len(shape) == 1 and x.ndim == 1:
            return x
        missing_size = sanitize_index(x.size / reduce(mul, known_sizes, 1))
        shape = tuple(missing_size if s == -1 else s for s in shape)

    # Sanity checks
    if np.isnan(sum(x.shape)):
        raise ValueError(
            f"Array chunk size or shape is unknown. shape: {x.shape}\n\n"
            "Possible solution with x.compute_chunk_sizes()"
        )
    if reduce(mul, shape, 1) != x.size:
        raise ValueError("total size of new array must be unchanged")

    # Identity reshape
    if x.shape == shape:
        return x

    # Handle merge_chunks=False: pre-rechunk to size-1 chunks in early dimensions
    expr = x.expr
    if not merge_chunks and x.ndim > len(shape):
        pre_rechunk = dict.fromkeys(range(x.ndim - len(shape)), 1)
        expr = expr.rechunk(pre_rechunk)

    return Reshape(expr, shape)

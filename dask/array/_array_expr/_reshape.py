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

    def _simplify_up(self, parent, dependents):
        """Allow slice operations to push through Reshape."""
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        return None

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through Reshape.

        Reshape can be pushed through when the slice only affects dimensions
        that have the same size in both input and output shapes (preserved dims).

        For example:
            x.reshape((10, 2, 3))[:5]  # (10, 6) -> (10, 2, 3), first dim preserved
            becomes: x[:5].reshape((5, 2, 3))
        """
        from numbers import Integral

        from dask._collections import new_collection

        in_shape = self.array.shape
        out_shape = self._shape
        index = slice_expr.index

        # Separate None (newaxis) from real indices
        # None insertions don't interact with reshape and can be re-applied after
        none_positions = []  # positions where None appears in original index
        stripped_index = []  # index without Nones
        for i, idx in enumerate(index):
            if idx is None:
                none_positions.append(i)
            else:
                stripped_index.append(idx)

        # Pad stripped index to output ndim
        out_ndim = len(out_shape)
        full_index = list(stripped_index) + [slice(None)] * (
            out_ndim - len(stripped_index)
        )

        # Find how many leading dimensions are preserved (same size in both shapes)
        preserved_dims = 0
        for in_size, out_size in zip(in_shape, out_shape):
            if in_size == out_size:
                preserved_dims += 1
            else:
                break

        if preserved_dims == 0:
            return None  # No preserved dimensions, can't push through

        # Check if slice only affects preserved dimensions
        # (non-preserved dims must all be slice(None))
        if any(
            isinstance(idx, Integral) or idx != slice(None)
            for idx in full_index[preserved_dims:]
        ):
            return None

        # Build the input slice (only on preserved dims, same indices)
        in_ndim = len(in_shape)
        input_index = list(full_index[:preserved_dims])
        input_index += [slice(None)] * (in_ndim - preserved_dims)

        # Compute new output shape after slicing
        new_out_shape = []
        for idx, size in zip(full_index, out_shape):
            if isinstance(idx, Integral):
                # Integer index removes dimension
                continue
            elif idx == slice(None):
                new_out_shape.append(size)
            else:
                # Normalize slice
                start, stop, step = idx.indices(size)
                if step != 1:
                    return None  # Don't handle non-unit steps
                new_out_shape.append(stop - start)

        new_out_shape = tuple(new_out_shape)

        # Apply slice to input, then reshape
        sliced_input = new_collection(self.array)[tuple(input_index)]
        result = Reshape(sliced_input.expr, new_out_shape)

        # Re-apply None insertions if any using expand_dims
        if none_positions:
            from dask.array._array_expr.manipulation._expand import expand_dims

            # Compute where Nones should be inserted in the OUTPUT of reshape
            # Account for integer indices that remove dimensions
            axes = []
            for pos in none_positions:
                # Count how many real (non-None) indices come before this position
                real_before = sum(1 for idx in index[:pos] if idx is not None)
                # Account for integer indices that removed dimensions
                ints_before = sum(
                    1
                    for idx in stripped_index[:real_before]
                    if isinstance(idx, Integral)
                )
                axes.append(
                    pos - len([p for p in none_positions if p < pos]) - ints_before
                )

            return expand_dims(new_collection(result), axis=tuple(axes)).expr

        return result


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
    from dask._collections import new_collection

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
            return new_collection(x.expr)
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

    # Identity reshape - return input unchanged
    if x.shape == shape:
        return x

    # Single partition case: use simple blockwise reshape
    expr = x.expr
    npartitions = reduce(mul, (len(c) for c in expr.chunks), 1)
    if npartitions == 1:
        return new_collection(ReshapeLowered(expr, shape, tuple((d,) for d in shape)))

    # Handle merge_chunks=False: pre-rechunk to size-1 chunks in early dimensions
    if not merge_chunks and x.ndim > len(shape):
        pre_rechunk = dict.fromkeys(range(x.ndim - len(shape)), 1)
        expr = expr.rechunk(pre_rechunk)

    return new_collection(Reshape(expr, shape))


def ravel(array_like):
    """Return a flattened array.

    Parameters
    ----------
    array_like : array_like
        Input array. Non-array inputs are converted to arrays.

    Returns
    -------
    raveled : Array
        A 1-D array containing the elements of the input.

    See Also
    --------
    numpy.ravel

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones((2, 3), chunks=2)
    >>> da.ravel(x).compute()
    array([1., 1., 1., 1., 1., 1.])
    """
    from dask.array._array_expr.core import asanyarray

    return asanyarray(array_like).reshape((-1,))

"""Expand operations: expand_dims, atleast_1d, atleast_2d, atleast_3d."""

from __future__ import annotations

import functools

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.chunk import getitem


class ExpandDims(ArrayExpr):
    """Dimension expansion expression.

    Adds new axes of size 1 at specified positions using numpy indexing with None.
    This is more efficient than reshape for dimension expansion and integrates
    better with slice pushdown optimizations.

    Parameters
    ----------
    array : ArrayExpr
        The input array expression.
    axes : tuple of int
        Positions where new axes should be inserted (in output coordinates).
    """

    _parameters = ["array", "axes"]

    @functools.cached_property
    def _meta(self):
        meta = self.array._meta
        for ax in sorted(self.axes):
            meta = np.expand_dims(meta, axis=ax)
        return meta

    @functools.cached_property
    def chunks(self):
        chunks = list(self.array.chunks)
        for ax in sorted(self.axes):
            chunks.insert(ax, (1,))
        return tuple(chunks)

    @functools.cached_property
    def _name(self):
        return f"expand-dims-{self.deterministic_token}"

    @functools.cached_property
    def _indexer(self):
        """Build indexer tuple with None at expansion axes."""
        out_ndim = self.array.ndim + len(self.axes)
        return tuple(None if i in self.axes else slice(None) for i in range(out_ndim))

    def _layer(self) -> dict:
        indexer = self._indexer
        axes = sorted(self.axes)
        input_name = self.array._name

        dsk = {}
        for block_id in np.ndindex(self.array.numblocks):
            in_key = (input_name,) + block_id
            # Insert 0 at expansion axes: ('x', 2, 3) -> ('expand', 0, 2, 0, 3)
            out_block_id = list(block_id)
            for ax in axes:
                out_block_id.insert(ax, 0)
            out_key = (self._name,) + tuple(out_block_id)
            dsk[out_key] = Task(out_key, getitem, TaskRef(in_key), indexer)

        return dsk

    def _simplify_up(self, parent, dependents):
        """Allow slice and shuffle operations to push through ExpandDims."""
        from dask.array._array_expr._shuffle import Shuffle
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        if isinstance(parent, Shuffle):
            return self._accept_shuffle(parent)
        return None

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through ExpandDims.

        Maps output slice indices to input, removing expanded axes.
        """
        from numbers import Integral

        axes = set(self.axes)
        index = slice_expr.index
        out_ndim = self.ndim

        # Pad index to full output length
        full_index = index + (slice(None),) * (out_ndim - len(index))

        # Build input slice by removing expanded axes
        input_index = []
        dims_removed = 0
        for i, idx in enumerate(full_index):
            if i in axes:
                # This is an expanded axis (size 1)
                if isinstance(idx, Integral):
                    # Integer index on size-1 axis must be 0 or -1
                    if idx not in (0, -1):
                        return None  # Out of bounds
                    dims_removed += 1
                elif idx == slice(None):
                    pass  # Keep this expansion axis
                elif isinstance(idx, slice):
                    # Slicing a size-1 axis - normalize
                    start, stop, step = idx.indices(1)
                    if stop <= start:
                        return None  # Empty result
                else:
                    return None  # Can't handle this
            else:
                input_index.append(idx)

        # Slice the input
        if all(idx == slice(None) for idx in input_index):
            sliced_input = self.array
        else:
            sliced_input = new_collection(self.array)[tuple(input_index)].expr

        # Compute new axes positions after slicing
        # Axes that had integer indexing are removed
        new_axes = []
        removed_count = 0
        for i, idx in enumerate(full_index):
            if i in axes:
                if isinstance(idx, Integral):
                    removed_count += 1
                else:
                    # Adjust for removed dimensions before this position
                    new_axes.append(i - removed_count)

        if not new_axes:
            # All expansion axes were removed by integer indexing
            return sliced_input

        return ExpandDims(sliced_input, tuple(new_axes))

    def _accept_shuffle(self, shuffle_expr):
        """Accept a shuffle being pushed through ExpandDims.

        Maps shuffle axis through expansion to input axis.
        """
        from dask.array._array_expr._shuffle import Shuffle

        axes = set(self.axes)
        shuffle_axis = shuffle_expr.axis

        # Can't shuffle on an expanded axis (size 1, nothing to shuffle)
        if shuffle_axis in axes:
            return None

        # Map output axis to input axis (subtract number of expansion axes before it)
        input_axis = shuffle_axis - sum(1 for ax in axes if ax < shuffle_axis)

        shuffled_input = Shuffle(
            self.array,
            shuffle_expr.indexer,
            input_axis,
            shuffle_expr.operand("name"),
        )
        return ExpandDims(shuffled_input, self.axes)


def expand_dims(a, axis):
    """Expand the shape of an array.

    Insert a new axis that will appear at the axis position in the expanded
    array shape.

    Parameters
    ----------
    a : array_like
        Input array.
    axis : int or tuple of ints
        Position in the expanded axes where the new axis (or axes) is placed.

    Returns
    -------
    result : Array
        Array with the number of dimensions increased.

    See Also
    --------
    numpy.expand_dims
    """
    from dask.array.utils import validate_axis

    if axis is None:
        raise TypeError("axis must be an integer, not None")

    if type(axis) not in (tuple, list):
        axis = (axis,)

    out_ndim = len(axis) + a.ndim
    axis = validate_axis(axis, out_ndim)

    return new_collection(ExpandDims(a.expr, tuple(sorted(axis))))


def atleast_1d(*arys):
    """Convert inputs to arrays with at least one dimension.

    Parameters
    ----------
    arys : array_like
        One or more array-like sequences. Non-array inputs are converted
        to arrays. Arrays that already have one or more dimensions are
        preserved.

    Returns
    -------
    ret : Array or tuple of Arrays
        An array, or tuple of arrays, each with a.ndim >= 1.

    See Also
    --------
    numpy.atleast_1d
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import NUMPY_GE_200

    new_arys = []
    for x in arys:
        x = asanyarray(x)
        if x.ndim == 0:
            x = x[None]
        new_arys.append(x)

    if len(new_arys) == 1:
        return new_arys[0]
    else:
        if NUMPY_GE_200:
            new_arys = tuple(new_arys)
        return new_arys


def atleast_2d(*arys):
    """View inputs as arrays with at least two dimensions.

    Parameters
    ----------
    arys : array_like
        One or more array-like sequences. Non-array inputs are converted
        to arrays. Arrays that already have two or more dimensions are
        preserved.

    Returns
    -------
    ret : Array or tuple of Arrays
        An array, or tuple of arrays, each with a.ndim >= 2.

    See Also
    --------
    numpy.atleast_2d
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import NUMPY_GE_200

    new_arys = []
    for x in arys:
        x = asanyarray(x)
        if x.ndim == 0:
            x = x[None, None]
        elif x.ndim == 1:
            x = x[None, :]
        new_arys.append(x)

    if len(new_arys) == 1:
        return new_arys[0]
    else:
        if NUMPY_GE_200:
            new_arys = tuple(new_arys)
        return new_arys


def atleast_3d(*arys):
    """View inputs as arrays with at least three dimensions.

    Parameters
    ----------
    arys : array_like
        One or more array-like sequences. Non-array inputs are converted
        to arrays. Arrays that already have three or more dimensions are
        preserved.

    Returns
    -------
    ret : Array or tuple of Arrays
        An array, or tuple of arrays, each with a.ndim >= 3.

    See Also
    --------
    numpy.atleast_3d
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import NUMPY_GE_200

    new_arys = []
    for x in arys:
        x = asanyarray(x)
        if x.ndim == 0:
            x = x[None, None, None]
        elif x.ndim == 1:
            x = x[None, :, None]
        elif x.ndim == 2:
            x = x[:, :, None]
        new_arys.append(x)

    if len(new_arys) == 1:
        return new_arys[0]
    else:
        if NUMPY_GE_200:
            new_arys = tuple(new_arys)
        return new_arys

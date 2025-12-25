"""Transpose operations: transpose, swapaxes, moveaxis, rollaxis."""

from __future__ import annotations

import functools

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._blockwise import Blockwise


class Transpose(Blockwise):
    _parameters = ["array", "axes"]
    func = staticmethod(np.transpose)
    align_arrays = False
    adjust_chunks = None
    concatenate = None
    token = "transpose"

    @property
    def new_axes(self):
        return {}

    @property
    def name(self):
        return self._name

    @property
    def _meta_provided(self):
        return self.array._meta

    @property
    def dtype(self):
        return self._meta.dtype

    @property
    def out_ind(self):
        return self.axes

    @property
    def kwargs(self):
        return {"axes": self.axes}

    @property
    def args(self):
        return (self.array, tuple(range(self.array.ndim)))

    @functools.cached_property
    def _inverse_axes(self):
        """Inverse permutation of axes."""
        inv = [0] * len(self.axes)
        for i, a in enumerate(self.axes):
            inv[a] = i
        return tuple(inv)

    def _task(self, key, block_id: tuple[int, ...]) -> Task:
        """Generate task for a specific output block."""
        # Map output block_id to input block_id using inverse permutation
        # For axes=(1,0), output block (i,j) needs input block (j,i)
        input_block_id = self._input_block_id(self.array, block_id)
        return Task(
            key, self.func, TaskRef((self.array._name, *input_block_id)), **self.kwargs
        )

    def _input_block_id(self, dep, block_id: tuple[int, ...]) -> tuple[int, ...]:
        """Map output block_id to input block_id using inverse permutation."""
        return tuple(block_id[self._inverse_axes[d]] for d in range(len(block_id)))

    def _simplify_down(self):
        # Transpose(Transpose(x)) -> single Transpose with composed axes
        if isinstance(self.array, Transpose):
            axes = tuple(self.array.axes[i] for i in self.axes)
            return Transpose(self.array.array, axes)
        # Identity transpose -> return the array
        if self.axes == tuple(range(self.array.ndim)):
            return self.array
        # Transpose(Elemwise(x, y)) -> Elemwise(Transpose(x), Transpose(y))
        from dask.array._array_expr._blockwise import Elemwise

        if isinstance(self.array, Elemwise):
            return self._pushdown_through_elemwise()

    def _pushdown_through_elemwise(self):
        """Push transpose through elemwise by transposing each input."""
        from dask.array._array_expr._blockwise import Elemwise
        from dask.array.core import is_scalar_for_elemwise

        elemwise = self.array
        axes = self.axes
        out_ndim = len(axes)

        # Only push through if all array inputs have the same ndim as output
        # Broadcasting cases require index transformations we don't handle
        for arg in elemwise.elemwise_args:
            if is_scalar_for_elemwise(arg):
                continue
            if arg.ndim != out_ndim:
                return None

        # Check where/out as well
        if hasattr(elemwise.where, "ndim") and elemwise.where.ndim != out_ndim:
            return None
        if hasattr(elemwise.out, "ndim") and elemwise.out.ndim != out_ndim:
            return None

        # Transpose each array input
        new_args = [
            arg if is_scalar_for_elemwise(arg) else Transpose(arg, axes)
            for arg in elemwise.elemwise_args
        ]

        # Transpose where/out if they are arrays
        new_where = elemwise.where
        if hasattr(new_where, "ndim"):
            new_where = Transpose(new_where, axes)

        new_out = elemwise.out
        if hasattr(new_out, "ndim"):
            new_out = Transpose(new_out, axes)

        return Elemwise(
            elemwise.op,
            elemwise.operand("dtype"),
            elemwise.operand("name"),
            new_where,
            new_out,
            elemwise.operand("_user_kwargs"),
            *new_args,
        )

    def _simplify_up(self, parent, dependents):
        """Allow slice and shuffle operations to push through Transpose."""
        from dask.array._array_expr._shuffle import Shuffle
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        if isinstance(parent, Shuffle):
            return self._accept_shuffle(parent)
        return None

    def _accept_shuffle(self, shuffle_expr):
        """Accept a shuffle being pushed through Transpose.

        Maps shuffle axis through transpose to get input axis.
        """
        axes = self.axes
        shuffle_axis = shuffle_expr.axis

        # Map shuffle axis through transpose: axes[i] tells us which input axis
        # becomes output axis i. So to shuffle output axis `shuffle_axis`, we need
        # to shuffle input axis `axes[shuffle_axis]`.
        input_axis = axes[shuffle_axis]

        from dask.array._array_expr._shuffle import Shuffle

        shuffled_input = Shuffle(
            self.array, shuffle_expr.indexer, input_axis, shuffle_expr.operand("name")
        )
        return Transpose(shuffled_input, axes)

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through Transpose.

        Maps output slice indices through transpose axes to get input slice.
        """
        from numbers import Integral

        from dask._collections import new_collection

        axes = self.axes
        index = slice_expr.index

        # Don't handle None/newaxis (adds dimensions)
        if any(idx is None for idx in index):
            return None

        # Pad index to full length
        full_index = index + (slice(None),) * (self.ndim - len(index))

        # Map output slice through transpose axes to get input slice
        # axes[i] tells us which input axis becomes output axis i
        # So output axis i gets slice full_index[i], which should go to input axis axes[i]
        input_index = [slice(None)] * len(axes)
        for out_axis, in_axis in enumerate(axes):
            input_index[in_axis] = full_index[out_axis]

        sliced_input = new_collection(self.array)[tuple(input_index)]

        # Check if any dimensions were removed by integer indexing
        has_integers = any(isinstance(idx, Integral) for idx in full_index)

        if not has_integers:
            # No dimension changes - just apply original transpose
            return Transpose(sliced_input.expr, axes)

        # Integer indices remove dimensions - compute new axes for remaining dims
        # Track which input dimensions remain (those not indexed by integers)
        remaining_input_dims = [
            in_axis
            for out_axis, in_axis in enumerate(axes)
            if not isinstance(full_index[out_axis], Integral)
        ]

        if len(remaining_input_dims) <= 1:
            # 0 or 1 dimension left - no transpose needed
            return sliced_input.expr

        # Map old input dim indices to new (post-slice) indices
        # After slicing, input dims are renumbered 0, 1, 2, ...
        sorted_remaining = sorted(remaining_input_dims)
        dim_map = {old: new for new, old in enumerate(sorted_remaining)}

        # Build new axes: for each remaining output dim, what's the new input dim?
        new_axes = tuple(dim_map[in_dim] for in_dim in remaining_input_dims)

        # Check if it's an identity transpose
        if new_axes == tuple(range(len(new_axes))):
            return sliced_input.expr

        return Transpose(sliced_input.expr, new_axes)


def transpose(a, axes=None):
    """Reverse or permute the axes of an array.

    See Also
    --------
    numpy.transpose
    """
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)
    if axes is not None:
        return a.transpose(axes)
    return a.transpose()


def swapaxes(a, axis1, axis2):
    """Interchange two axes of an array.

    See Also
    --------
    numpy.swapaxes
    """
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)
    if axis1 == axis2:
        return a
    if axis1 < 0:
        axis1 = axis1 + a.ndim
    if axis2 < 0:
        axis2 = axis2 + a.ndim
    ind = list(range(a.ndim))
    ind[axis1], ind[axis2] = ind[axis2], ind[axis1]
    return transpose(a, ind)


def moveaxis(a, source, destination):
    """Move axes of an array to new positions.

    See Also
    --------
    numpy.moveaxis
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import normalize_axis_tuple

    a = asanyarray(a)
    source = normalize_axis_tuple(source, a.ndim, "source")
    destination = normalize_axis_tuple(destination, a.ndim, "destination")
    if len(source) != len(destination):
        raise ValueError(
            "`source` and `destination` arguments must have "
            "the same number of elements"
        )

    order = [n for n in range(a.ndim) if n not in source]

    for dest, src in sorted(zip(destination, source)):
        order.insert(dest, src)

    return transpose(a, order)


def rollaxis(a, axis, start=0):
    """Roll the specified axis backwards, until it lies in a given position.

    See Also
    --------
    numpy.rollaxis
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import normalize_axis_index

    a = asanyarray(a)
    n = a.ndim
    axis = normalize_axis_index(axis, n)
    if start < 0:
        start += n
    msg = "'%s' arg requires %d <= %s < %d, but %d was passed in"
    if not (0 <= start < n + 1):
        raise ValueError(msg % ("start", -n, "start", n + 1, start))
    if axis < start:
        start -= 1
    if axis == start:
        return a[...]
    axes = list(range(0, n))
    axes.remove(axis)
    axes.insert(start, axis)
    return transpose(a, axes)

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

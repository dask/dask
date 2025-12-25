"""Stack operation - expression and collection function."""

from __future__ import annotations

import functools
from itertools import product

import numpy as np
from toolz import concat, first

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr, unify_chunks_expr
from dask.array.chunk import getitem
from dask.array.utils import meta_from_array


class Stack(ArrayExpr):
    _parameters = ["array", "axis", "meta"]

    @functools.cached_property
    def args(self):
        return [self.array] + self.operands[len(self._parameters) :]

    @functools.cached_property
    def _meta(self):
        return self.operand("meta")

    @functools.cached_property
    def chunks(self):
        n = len(self.args)
        return (
            self.array.chunks[: self.axis]
            + ((1,) * n,)
            + self.array.chunks[self.axis :]
        )

    @functools.cached_property
    def _name(self):
        return "stack-" + self.deterministic_token

    def _layer(self) -> dict:
        keys = list(product([self._name], *[range(len(bd)) for bd in self.chunks]))
        names = [a.name for a in self.args]
        axis = self.axis
        ndim = self._meta.ndim - 1

        inputs = [
            (names[key[axis + 1]],) + key[1 : axis + 1] + key[axis + 2 :]
            for key in keys
        ]
        values = [
            Task(
                key,
                getitem,
                TaskRef(inp),
                (slice(None, None, None),) * axis
                + (None,)
                + (slice(None, None, None),) * (ndim - axis),
            )
            for key, inp in zip(keys, inputs)
        ]
        return dict(zip(keys, values))

    def _simplify_up(self, parent, dependents):
        """Allow slice and shuffle operations to push through Stack."""
        from dask.array._array_expr._shuffle import Shuffle
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        if isinstance(parent, Shuffle):
            return self._accept_shuffle(parent)
        return None

    def _accept_shuffle(self, shuffle_expr):
        """Accept a shuffle being pushed through Stack.

        Stack adds a new dimension at axis. Can't shuffle on the stacked axis.
        For other axes, adjust axis index for inputs (they have one fewer dim).
        """
        from dask.array._array_expr._shuffle import Shuffle

        stack_axis = self.axis
        shuffle_axis = shuffle_expr.axis

        # Can't shuffle on the stacked axis itself
        if shuffle_axis == stack_axis:
            return None

        # Adjust axis for inputs (they have one fewer dimension)
        if shuffle_axis > stack_axis:
            input_shuffle_axis = shuffle_axis - 1
        else:
            input_shuffle_axis = shuffle_axis

        # Shuffle each input
        arrays = self.args
        shuffled_arrays = [
            Shuffle(
                a,
                shuffle_expr.indexer,
                input_shuffle_axis,
                shuffle_expr.operand("name"),
            )
            for a in arrays
        ]

        return type(self)(
            shuffled_arrays[0],
            stack_axis,
            self._meta,
            *shuffled_arrays[1:],
        )

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through Stack.

        Stack adds a new dimension at axis. Cases:
        1. Slice on stacked axis: select subset of inputs
        2. Slice on other axes: push to all inputs (adjusting for added dim)
        """
        from numbers import Integral

        from dask._collections import new_collection
        from dask.array.utils import meta_from_array

        axis = self.axis
        arrays = self.args
        index = slice_expr.index

        # Pad index to full length (output has one more dim than inputs)
        full_index = index + (slice(None),) * (self.ndim - len(index))

        # For now, only handle simple slices (no integers that reduce dims)
        if any(isinstance(idx, Integral) for idx in full_index):
            return None
        if any(idx is None for idx in full_index):
            return None

        # Handle the stacked axis slice
        stacked_axis_slice = full_index[axis]
        n_arrays = len(arrays)

        if isinstance(stacked_axis_slice, slice):
            start, stop, step = stacked_axis_slice.indices(n_arrays)
            if step != 1:
                return None
            selected_arrays = arrays[start:stop]
        else:
            return None

        if not selected_arrays:
            return None

        # Build slice for the other axes (remove the stacked axis from index)
        other_slices = full_index[:axis] + full_index[axis + 1 :]

        # Check if we need to slice the other axes
        needs_other_slice = any(s != slice(None) for s in other_slices)

        # Slice each selected array on the other axes
        sliced_arrays = []
        for arr in selected_arrays:
            if needs_other_slice:
                sliced_arr = new_collection(arr)[other_slices]
                sliced_arrays.append(sliced_arr.expr)
            else:
                sliced_arrays.append(arr)

        # Compute new meta for the resulting stack
        new_meta = np.stack(
            [meta_from_array(new_collection(a)) for a in sliced_arrays], axis=axis
        )

        # Create new Stack with selected/sliced arrays
        return type(self)(
            sliced_arrays[0],
            axis,
            new_meta,
            *sliced_arrays[1:],
        )


def stack(seq, axis=0, allow_unknown_chunksizes=False):
    """
    Stack arrays along a new axis

    Given a sequence of dask arrays, form a new dask array by stacking them
    along a new dimension (axis=0 by default)

    Parameters
    ----------
    seq: list of dask.arrays
    axis: int
        Dimension along which to align all of the arrays
    allow_unknown_chunksizes: bool
        Allow unknown chunksizes, such as come from converting from dask
        dataframes.  Dask.array is unable to verify that chunks line up.  If
        data comes from differently aligned sources then this can cause
        unexpected results.

    Examples
    --------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [da.from_array(np.ones((4, 4)), chunks=(2, 2))
    ...         for i in range(3)]

    >>> x = da.stack(data, axis=0)
    >>> x.shape
    (3, 4, 4)

    >>> da.stack(data, axis=1).shape
    (4, 3, 4)

    >>> da.stack(data, axis=-1).shape
    (4, 4, 3)

    Result is a new dask Array

    See Also
    --------
    concatenate
    """
    from dask.array import wrap

    # Lazy import to avoid circular dependency
    from dask.array._array_expr.core import asarray

    seq = [asarray(a, allow_unknown_chunksizes=allow_unknown_chunksizes) for a in seq]

    if not seq:
        raise ValueError("Need array(s) to stack")
    if not allow_unknown_chunksizes and not all(x.shape == seq[0].shape for x in seq):
        idx = first(i for i in enumerate(seq) if i[1].shape != seq[0].shape)
        raise ValueError(
            "Stacked arrays must have the same shape. The first array had shape "
            f"{seq[0].shape}, while array {idx[0] + 1} has shape {idx[1].shape}."
        )

    meta = np.stack([meta_from_array(a) for a in seq], axis=axis)
    seq = [x.astype(meta.dtype) for x in seq]

    ndim = meta.ndim - 1
    if axis < 0:
        axis = ndim + axis + 1
    shape = tuple(
        (
            len(seq)
            if i == axis
            else (seq[0].shape[i] if i < axis else seq[0].shape[i - 1])
        )
        for i in range(meta.ndim)
    )

    seq2 = [a for a in seq if a.size]
    if not seq2:
        seq2 = seq

    n = len(seq2)
    if n == 0:
        try:
            return wrap.empty_like(meta, shape=shape, chunks=shape, dtype=meta.dtype)
        except TypeError:
            return wrap.empty(shape, chunks=shape, dtype=meta.dtype)

    ind = list(range(ndim))
    uc_args = list(concat((x.expr, ind) for x in seq2))
    _, seq2, _ = unify_chunks_expr(*uc_args)

    assert len({a.chunks for a in seq2}) == 1  # same chunks

    return new_collection(Stack(seq2[0], axis, meta, *seq2[1:]))

from __future__ import annotations

import functools
from itertools import product
from numbers import Integral

import numpy as np
from toolz import pluck

from dask._task_spec import Alias, DataNode, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.chunk import getitem
from dask.array.optimization import fuse_slice
from dask.array.slicing import (
    _slice_1d,
    check_index,
    expander,
    new_blockdim,
    normalize_slice,
    posify_index,
    replace_ellipsis,
    sanitize_index,
)
from dask.array.utils import meta_from_array
from dask.layers import ArrayBlockwiseDep
from dask.tokenize import tokenize
from dask.utils import cached_cumsum, is_arraylike


def _compute_sliced_chunks(chunks, slc, dim_size):
    """Compute chunk sizes for the sliced region of a dimension."""
    if slc == slice(None):
        return chunks

    start, stop, step = slc.indices(dim_size)

    # Handle step == -1 (flip) specially - preserve chunks in reverse order
    if step == -1:
        # Check if this is a full flip (equivalent to slice(None, None, -1))
        if start == dim_size - 1 and stop == -1:
            # Full flip: reverse the chunks
            return chunks[::-1]
        else:
            # Partial negative step: fall back to single chunk
            new_size = len(range(start, stop, step))
            return (new_size,)

    if step != 1:
        # For non-unit step (other than -1), fall back to single chunk
        new_size = len(range(start, stop, step))
        return (new_size,)

    # Handle empty slice - return single chunk of size 0
    if start >= stop:
        return (0,)

    # Find chunks that overlap with [start, stop)
    result = []
    pos = 0
    for chunk_size in chunks:
        chunk_start = pos
        chunk_end = pos + chunk_size
        pos = chunk_end

        # Skip chunks entirely before the slice
        if chunk_end <= start:
            continue
        # Stop at chunks entirely after the slice
        if chunk_start >= stop:
            break

        # Compute the portion of this chunk included in the slice
        included_start = max(chunk_start, start)
        included_end = min(chunk_end, stop)
        result.append(included_end - included_start)

    return tuple(result) if result else (0,)


def slice_with_int_dask_array(x, index):
    """Slice x with at most one 1D dask arrays of ints.

    This is a helper function of :meth:`Array.__getitem__`.

    Parameters
    ----------
    x: Array
    index: tuple with as many elements as x.ndim, among which there are
           one or more Array's with dtype=int

    Returns
    -------
    tuple of (sliced x, new index)

    where the new index is the same as the input, but with slice(None)
    replaced to the original slicer where a 1D filter has been applied and
    one less element where a zero-dimensional filter has been applied.
    """
    from dask.array._array_expr._collection import Array

    assert len(index) == x.ndim
    fancy_indexes = [
        isinstance(idx, (tuple, list))
        or (isinstance(idx, (np.ndarray, Array)) and idx.ndim > 0)
        for idx in index
    ]
    if sum(fancy_indexes) > 1:
        raise NotImplementedError("Don't yet support nd fancy indexing")

    out_index = []
    dropped_axis_cnt = 0
    for in_axis, idx in enumerate(index):
        out_axis = in_axis - dropped_axis_cnt
        if isinstance(idx, Array) and idx.dtype.kind in "iu":
            if idx.ndim == 0:
                idx = idx[np.newaxis]
                x = slice_with_int_dask_array_on_axis(x, idx, out_axis)
                x = x[tuple(0 if i == out_axis else slice(None) for i in range(x.ndim))]
                dropped_axis_cnt += 1
            elif idx.ndim == 1:
                x = slice_with_int_dask_array_on_axis(x, idx, out_axis)
                out_index.append(slice(None))
            else:
                raise NotImplementedError(
                    "Slicing with dask.array of ints only permitted when "
                    "the indexer has zero or one dimensions"
                )
        else:
            out_index.append(idx)
    return x, tuple(out_index)


def normalize_index(idx, shape):
    """Normalize slicing indexes

    1.  Replaces ellipses with many full slices
    2.  Adds full slices to end of index
    3.  Checks bounding conditions
    4.  Replace multidimensional numpy arrays with dask arrays
    5.  Replaces numpy arrays with lists
    6.  Posify's integers and lists
    7.  Normalizes slices to canonical form

    Examples
    --------
    >>> normalize_index(1, (10,))
    (1,)
    >>> normalize_index(-1, (10,))
    (9,)
    >>> normalize_index([-1], (10,))
    (array([9]),)
    >>> normalize_index(slice(-3, 10, 1), (10,))
    (slice(7, None, None),)
    >>> normalize_index((Ellipsis, None), (10,))
    (slice(None, None, None), None)
    >>> normalize_index(np.array([[True, False], [False, True], [True, True]]), (3, 2))
    (dask.array<array, shape=(3, 2), dtype=bool, chunksize=(3, 2), chunktype=numpy.ndarray>,)
    """
    from dask.array._array_expr._collection import Array, from_array

    if not isinstance(idx, tuple):
        idx = (idx,)

    # if a > 1D numpy.array is provided, cast it to a dask array
    if len(idx) > 0 and len(shape) > 1:
        i = idx[0]
        if is_arraylike(i) and not isinstance(i, Array) and i.shape == shape:
            idx = (from_array(i), *idx[1:])

    idx = replace_ellipsis(len(shape), idx)
    n_sliced_dims = 0
    for i in idx:
        if hasattr(i, "ndim") and i.ndim >= 1:
            n_sliced_dims += i.ndim
        elif i is None:
            continue
        else:
            n_sliced_dims += 1

    idx = idx + (slice(None),) * (len(shape) - n_sliced_dims)
    if len([i for i in idx if i is not None]) > len(shape):
        raise IndexError("Too many indices for array")

    none_shape = []
    i = 0
    for ind in idx:
        if ind is not None:
            none_shape.append(shape[i])
            i += 1
        else:
            none_shape.append(None)

    for axis, (i, d) in enumerate(zip(idx, none_shape)):
        if d is not None:
            check_index(axis, i, d)
    idx = tuple(map(sanitize_index, idx))
    idx = tuple(map(normalize_slice, idx, none_shape))
    idx = posify_index(none_shape, idx)
    return idx


def slice_with_int_dask_array_on_axis(x, idx, axis):
    """Slice a ND dask array with a 1D dask arrays of ints along the given
    axis.

    This is a helper function of :func:`slice_with_int_dask_array`.
    """
    from dask.array import chunk
    from dask.array._array_expr._collection import blockwise

    assert 0 <= axis < x.ndim

    if np.isnan(x.chunks[axis]).any():
        raise NotImplementedError(
            "Slicing an array with unknown chunks with "
            "a dask.array of ints is not supported"
        )
    x_axes = tuple(range(x.ndim))
    idx_axes = (x.ndim,)  # arbitrary index not already in x_axes
    offset_axes = (axis,)

    # Calculate the offset at which each chunk starts along axis
    # e.g. chunks=(..., (5, 3, 4), ...) -> offset=[0, 5, 8]
    offset = np.roll(np.cumsum(np.asarray(x.chunks[axis], like=x._meta)), 1)
    offset[0] = 0
    # ArrayOffsetDep needs 1D chunks matching x.chunks[axis], not full x.chunks
    offset = ArrayOffsetDep((x.chunks[axis],), offset)

    p_axes = x_axes[: axis + 1] + idx_axes + x_axes[axis + 1 :]
    y_axes = x_axes[:axis] + idx_axes + x_axes[axis + 1 :]

    # Calculate the cartesian product of every chunk of x vs every chunk of idx
    p = blockwise(
        chunk.slice_with_int_dask_array,
        p_axes,
        x,
        x_axes,
        idx,
        idx_axes,
        offset,
        offset_axes,
        x_size=x.shape[axis],
        axis=axis,
        dtype=x.dtype,
        meta=x._meta,
    )

    # Aggregate on the chunks of x along axis
    y = blockwise(
        chunk.slice_with_int_dask_array_aggregate,
        y_axes,
        idx,
        idx_axes,
        p,
        p_axes,
        concatenate=True,
        x_chunks=x.chunks[axis],
        axis=axis,
        dtype=x.dtype,
        meta=x._meta,
    )
    return y


class ArrayOffsetDep(ArrayBlockwiseDep):
    """1D BlockwiseDep that provides chunk offset values."""

    def __init__(self, chunks: tuple[tuple[int, ...], ...], values: np.ndarray | dict):
        super().__init__(chunks)
        self.values = values

    def __getitem__(self, idx: tuple):
        return self.values[idx[0]]


def slice_array(x, index):
    """
    slice_with_newaxis : handle None/newaxis case
    slice_wrap_lists : handle fancy indexing with lists
    slice_slices_and_integers : handle everything else
    """
    if all(
        isinstance(index, slice) and index == slice(None, None, None) for index in index
    ):
        # all none slices
        return x.expr

    # Add in missing colons at the end as needed.  x[5] -> x[5, :, :]
    not_none_count = sum(i is not None for i in index)
    missing = len(x.chunks) - not_none_count
    index += (slice(None, None, None),) * missing
    return slice_with_newaxes(x, index)


def slice_with_newaxes(x, index):
    """
    Handle indexing with Nones

    Strips out Nones then hands off to slice_wrap_lists
    """
    # Strip Nones from index
    index2 = tuple(ind for ind in index if ind is not None)
    where_none = [i for i, ind in enumerate(index) if ind is None]
    for i, xx in enumerate(where_none):
        n = sum(isinstance(ind, Integral) for ind in index[:xx])
        if n:
            where_none[i] -= n

    # Pass down and do work
    x = slice_wrap_lists(x, index2, not where_none)

    if where_none:
        # Check if result has the expected attributes for SlicesWrapNone
        if hasattr(x, "index") and hasattr(x, "allow_getitem_optimization"):
            return SlicesWrapNone(
                x.array, x.index, x.allow_getitem_optimization, where_none
            )
        else:
            # For other expression types (e.g., Shuffle from fancy indexing),
            # use expand_dims to add the new axes. Need to wrap in Array first.
            from dask.array._array_expr._collection import Array
            from dask.array._array_expr.manipulation._expand import expand_dims

            return expand_dims(Array(x), axis=tuple(where_none)).expr

    else:
        return x


def slice_wrap_lists(x, index, allow_getitem_optimization):
    """
    Fancy indexing along blocked array dasks

    Handles index of type list.  Calls slice_slices_and_integers for the rest

    See Also
    --------

    take : handle slicing with lists ("fancy" indexing)
    slice_slices_and_integers : handle slicing with slices and integers
    """
    assert all(isinstance(i, (slice, list, Integral)) or is_arraylike(i) for i in index)
    if not len(x.chunks) == len(index):
        raise IndexError("Too many indices for array")

    # Do we have more than one list in the index?
    where_list = [
        i for i, ind in enumerate(index) if is_arraylike(ind) and ind.ndim > 0
    ]
    if len(where_list) > 1:
        raise NotImplementedError("Don't yet support nd fancy indexing")
    # Is the single list an empty list? In this case just treat it as a zero
    # length slice
    if where_list and not index[where_list[0]].size:
        index = list(index)
        index[where_list.pop()] = slice(0, 0, 1)
        index = tuple(index)

    # No lists, hooray! just use slice_slices_and_integers
    if not where_list:
        return slice_slices_and_integers(x, index, allow_getitem_optimization)

    # Replace all lists with full slices  [3, 1, 0] -> slice(None, None, None)
    index_without_list = tuple(
        slice(None, None, None) if is_arraylike(i) else i for i in index
    )

    # lists and full slices.  Just use take
    if all(is_arraylike(i) or i == slice(None, None, None) for i in index):
        axis = where_list[0]
        x = take(x, index[where_list[0]], axis=axis)
    # Mixed case. Both slices/integers and lists. slice/integer then take
    else:
        x = slice_slices_and_integers(
            x,
            index_without_list,
            allow_getitem_optimization=False,
        )
        axis = where_list[0]
        axis2 = axis - sum(
            1 for i, ind in enumerate(index) if i < axis and isinstance(ind, Integral)
        )
        x = take(x, index[axis], axis=axis2)

    return x


def slice_slices_and_integers(x, index, allow_getitem_optimization=False):
    from dask.array.core import unknown_chunk_message

    shape = tuple(cached_cumsum(dim, initial_zero=True)[-1] for dim in x.chunks)

    for dim, ind in zip(shape, index):
        if np.isnan(dim) and ind != slice(None, None, None):
            raise ValueError(
                f"Arrays chunk sizes are unknown: {shape}{unknown_chunk_message}"
            )
    assert all(isinstance(ind, (slice, Integral)) for ind in index)
    return SliceSlicesIntegers(x, index, allow_getitem_optimization)


def take(x, index, axis=0):
    from dask.base import is_dask_collection

    if not np.isnan(x.chunks[axis]).any():
        from dask.array._array_expr._shuffle import _shuffle
        from dask.array.utils import arange_safe, asarray_safe

        # No-op check only for numpy arrays (dask array comparison triggers warnings)
        # Use is_dask_collection to catch both array-expr and legacy dask Arrays
        if not is_dask_collection(index):
            arange = arange_safe(np.sum(x.chunks[axis]), like=index)
            if len(index) == len(arange) and np.abs(index - arange).sum() == 0:
                return x

        # If index is a dask collection, use lazy blockwise approach
        if is_dask_collection(index):
            return slice_with_int_dask_array_on_axis(x, index, axis)

        average_chunk_size = int(sum(x.chunks[axis]) / len(x.chunks[axis]))

        indexer = []
        index = asarray_safe(index, like=index)
        for i in range(0, len(index), average_chunk_size):
            indexer.append(index[i : i + average_chunk_size].tolist())
        return _shuffle(x, indexer, axis, "getitem-")
    elif len(x.chunks[axis]) == 1:
        return TakeUnknownOneChunk(x, index, axis)
    else:
        from dask.array.core import unknown_chunk_message

        raise ValueError(
            f"Array chunk size or shape is unknown. {unknown_chunk_message}"
        )


class Slice(ArrayExpr):
    @functools.cached_property
    def _name(self):
        return f"getitem-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        meta = meta_from_array(self.array._meta, ndim=len(self.chunks))
        if np.isscalar(meta):
            meta = np.array(meta)
        return meta


class SliceSlicesIntegers(Slice):
    _parameters = ["array", "index", "allow_getitem_optimization"]

    def _simplify_down(self):
        from dask.array._array_expr._blockwise import Blockwise, Elemwise
        from dask.array._array_expr.manipulation._transpose import Transpose
        from dask.array._array_expr.reductions._reduction import PartialReduce

        # Slice(Slice(x)) -> single Slice with fused indices
        if isinstance(self.array, SliceSlicesIntegers):
            try:
                fused = fuse_slice(self.array.index, self.index)
                normalized = tuple(
                    normalize_slice(idx, dim) if isinstance(idx, slice) else idx
                    for idx, dim in zip(fused, self.array.array.shape)
                )
                return SliceSlicesIntegers(
                    self.array.array, normalized, self.allow_getitem_optimization
                )
            except NotImplementedError:
                # Skip fusion for unsupported slicing patterns (e.g., negative step)
                pass

        # Slice(Elemwise) -> Elemwise(sliced inputs)
        if isinstance(self.array, Elemwise):
            return self._pushdown_through_elemwise()

        # Slice(Transpose) -> Transpose(sliced input) - check before Blockwise
        if isinstance(self.array, Transpose):
            return self._pushdown_through_transpose()

        # Slice(Blockwise) -> Blockwise(sliced inputs) - for non-Elemwise blockwise
        if isinstance(self.array, Blockwise):
            return self._pushdown_through_blockwise()

        # Slice(PartialReduce) -> PartialReduce(sliced input)
        if isinstance(self.array, PartialReduce):
            return self._pushdown_through_reduction()

        # Slice(FromArray) -> FromArray(sliced source) - push slice into IO
        if getattr(self.array, "_slice_pushdown", False):
            return self._pushdown_into_io()

        # Slice(BroadcastTrick) -> BroadcastTrick(sliced shape)
        # For ones/zeros/full/empty, just create new with sliced shape
        from dask.array._array_expr.creation import BroadcastTrick

        if isinstance(self.array, BroadcastTrick):
            return self._simplify_creation()

        # Slice(Concatenate) -> push slice through concatenate
        from dask.array._array_expr._concatenate import Concatenate

        if isinstance(self.array, Concatenate):
            return self._pushdown_through_concatenate()

        # Slice(Stack) -> push slice through stack
        from dask.array._array_expr._stack import Stack

        if isinstance(self.array, Stack):
            return self._pushdown_through_stack()

        # Slice(BroadcastTo) -> push slice through broadcast
        from dask.array._array_expr._broadcast import BroadcastTo

        if isinstance(self.array, BroadcastTo):
            return self._pushdown_through_broadcast_to()

    def _simplify_creation(self):
        """Simplify Slice(BroadcastTrick) to BroadcastTrick with new shape.

        For ones, zeros, full, empty - just create a new instance with
        the sliced shape and chunks.
        """
        creation = self.array
        index = self.index
        old_shape = creation.shape
        old_chunks = creation.chunks

        # Pad index to full length
        full_index = index + (slice(None),) * (len(old_shape) - len(index))

        # Handle integers and newaxis - for now, only handle simple slices
        if any(idx is None for idx in full_index):
            return None
        if any(isinstance(idx, Integral) for idx in full_index):
            return None

        # Compute new shape and chunks from slices
        new_shape = []
        new_chunks = []
        for i, idx in enumerate(full_index):
            if isinstance(idx, slice):
                # Normalize slice
                start, stop, step = idx.indices(old_shape[i])
                if step != 1:
                    return None  # Don't handle non-unit steps
                new_dim = max(0, stop - start)
                new_shape.append(new_dim)

                # Compute new chunks for this dimension
                old_axis_chunks = old_chunks[i]
                axis_chunks = []
                cumsum = 0
                for chunk_size in old_axis_chunks:
                    chunk_start = cumsum
                    chunk_end = cumsum + chunk_size
                    cumsum = chunk_end

                    # Intersection of [chunk_start, chunk_end) with [start, stop)
                    overlap_start = max(chunk_start, start)
                    overlap_end = min(chunk_end, stop)
                    if overlap_end > overlap_start:
                        axis_chunks.append(overlap_end - overlap_start)

                new_chunks.append(tuple(axis_chunks) if axis_chunks else (0,))
            else:
                return None  # Unexpected index type

        # Substitute shape and chunks, clear name for new expression
        return creation.substitute_parameters(
            {
                "shape": tuple(new_shape),
                "chunks": tuple(new_chunks),
                "name": None,
            }
        )

    def _pushdown_through_concatenate(self):
        """Push slice through concatenate.

        Cases:
        1. Slice on concat axis: select/trim relevant arrays
        2. Slice on other axis: push to all inputs
        """
        from dask.array._array_expr._collection import new_collection

        concat = self.array
        axis = concat.axis
        arrays = concat.args
        index = self.index

        # Pad index to full length
        full_index = index + (slice(None),) * (concat.ndim - len(index))

        # For now, only handle simple slices (no integers that reduce dims)
        if any(isinstance(idx, Integral) for idx in full_index):
            return None
        if any(idx is None for idx in full_index):
            return None

        axis_slice = full_index[axis]

        # Normalize the axis slice
        concat_dim_size = sum(a.shape[axis] for a in arrays)
        if isinstance(axis_slice, slice):
            start, stop, step = axis_slice.indices(concat_dim_size)
            if step != 1:
                return None  # Don't handle non-unit steps
        else:
            return None

        # Build sliced arrays
        sliced_arrays = []
        cumsum = 0
        for arr in arrays:
            arr_size = arr.shape[axis]
            arr_start = cumsum
            arr_end = cumsum + arr_size

            # Check if this array overlaps with the slice
            overlap_start = max(start, arr_start)
            overlap_end = min(stop, arr_end)

            if overlap_end > overlap_start:
                # Build slice for this array
                local_start = overlap_start - arr_start
                local_stop = overlap_end - arr_start

                # Build full slice tuple for this array
                arr_slices = list(full_index)
                arr_slices[axis] = slice(local_start, local_stop)

                sliced_arr = new_collection(arr)[tuple(arr_slices)]
                sliced_arrays.append(sliced_arr.expr)

            cumsum = arr_end

        if not sliced_arrays:
            # Empty result - shouldn't happen with valid slice
            return None

        if len(sliced_arrays) == 1:
            # Only one array needed - just return it (already sliced)
            return sliced_arrays[0]

        # Multiple arrays - create new Concatenate
        return type(concat)(
            sliced_arrays[0],
            axis,
            concat._meta,
            *sliced_arrays[1:],
        )

    def _pushdown_through_stack(self):
        """Push slice through stack.

        Stack adds a new dimension at axis. Cases:
        1. Slice on stacked axis: select subset of inputs
        2. Slice on other axes: push to all inputs (adjusting for added dim)
        """
        import numpy as np

        from dask.array._array_expr._collection import new_collection
        from dask.array.utils import meta_from_array

        stack = self.array
        axis = stack.axis
        arrays = stack.args
        index = self.index

        # Pad index to full length (output has one more dim than inputs)
        full_index = index + (slice(None),) * (stack.ndim - len(index))

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
        return type(stack)(
            sliced_arrays[0],
            axis,
            new_meta,
            *sliced_arrays[1:],
        )

    def _pushdown_through_broadcast_to(self):
        """Push slice through broadcast_to.

        For broadcast_to(x, shape)[slices]:
        - Dimensions added by broadcast (new dims): affect output shape only
        - Dimensions from input with size > 1: push slice to input
        - Dimensions from input with size == 1: affect output shape only
        """
        from dask.array._array_expr._broadcast import BroadcastTo
        from dask.array._array_expr._collection import new_collection

        broadcast = self.array
        input_arr = broadcast.array
        output_shape = broadcast._shape
        index = self.index

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
        old_chunks = broadcast._chunks
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
        from dask.array.utils import meta_from_array

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
        result = []
        cumsum = 0
        for chunk_size in chunks:
            chunk_start = cumsum
            chunk_end = cumsum + chunk_size
            cumsum = chunk_end

            if chunk_end <= start:
                continue
            if chunk_start >= start + length:
                break

            overlap_start = max(start, chunk_start)
            overlap_end = min(start + length, chunk_end)
            overlap_size = overlap_end - overlap_start
            if overlap_size > 0:
                result.append(overlap_size)

        return tuple(result) if result else (0,)

    def _pushdown_through_elemwise(self):
        """Push slice through elemwise by slicing each input appropriately."""
        from dask.array._array_expr._blockwise import Elemwise, is_scalar_for_elemwise
        from dask.array._array_expr._collection import new_collection

        elemwise = self.array
        out_ind = elemwise.out_ind
        index = self.index

        # Pad index to full length
        full_index = index + (slice(None),) * (len(out_ind) - len(index))

        # Build sliced inputs
        new_args = []
        for arg in elemwise.elemwise_args:
            if is_scalar_for_elemwise(arg):
                new_args.append(arg)
            else:
                # Map output slice to this input's dimensions
                # arg has indices tuple(range(arg.ndim)[::-1])
                arg_ind = tuple(range(arg.ndim)[::-1])

                # For each dimension of arg, find where its index appears in out_ind
                # and get the corresponding slice
                arg_slices = []
                for dim_idx in arg_ind:
                    # Find position of this index in out_ind
                    try:
                        out_pos = out_ind.index(dim_idx)
                        arg_slices.append(full_index[out_pos])
                    except ValueError:
                        # Index not in output (shouldn't happen for elemwise)
                        arg_slices.append(slice(None))

                sliced_arg = new_collection(arg)[tuple(arg_slices)]
                new_args.append(sliced_arg.expr)

        return Elemwise(
            elemwise.op,
            elemwise.operand("dtype"),
            elemwise.operand("name"),
            elemwise.where,
            elemwise.out,
            elemwise.operand("_user_kwargs"),
            *new_args,
        )

    def _pushdown_through_blockwise(self):
        """Push slice through general Blockwise by slicing each input appropriately.

        This optimization is safe when:
        - The blockwise doesn't adjust chunk sizes
        - The blockwise doesn't add new axes
        - The slice uses only slices or integers (no newaxis)

        For integer indices, we convert them to size-1 slices, push through,
        then extract with [0] at the end.
        """
        from dask.array._array_expr._blockwise import Blockwise
        from dask.array._array_expr._collection import new_collection

        bw = self.array
        out_ind = bw.out_ind
        index = self.index

        # Don't handle None/newaxis
        if any(idx is None for idx in index):
            return None

        # Pad index to full output length
        full_index = index + (slice(None),) * (len(out_ind) - len(index))

        # Find which output axes have non-trivial slices
        sliced_axes = {
            i
            for i, idx in enumerate(full_index)
            if isinstance(idx, Integral) or idx != slice(None)
        }

        # Use getattr since subclasses may define as class attribute or property
        adjust_chunks = getattr(bw, "adjust_chunks", None)
        if adjust_chunks:
            # Only reject if we're slicing an adjusted dimension
            # adjust_chunks keys are output axis indices
            adjusted_axes = set(adjust_chunks.keys())
            if sliced_axes & adjusted_axes:
                return None

        # Don't handle if blockwise adds new axes and we're slicing those axes
        new_axes = getattr(bw, "new_axes", None)
        if new_axes:
            new_axis_positions = set(new_axes.keys())
            if sliced_axes & new_axis_positions:
                return None

        # Convert integers to size-1 slices for pushdown
        slice_index = tuple(
            slice(idx, idx + 1) if isinstance(idx, Integral) else idx
            for idx in full_index
        )
        has_integers = any(isinstance(idx, Integral) for idx in full_index)

        # For subclasses with a single "array" parameter, use substitute_parameters
        if "array" in type(bw)._parameters:
            # Map output slice indices to input dimensions
            arg_ind = tuple(range(bw.array.ndim))  # Input indices
            arg_slices = []
            for dim_idx in arg_ind:
                try:
                    out_pos = out_ind.index(dim_idx)
                    arg_slices.append(slice_index[out_pos])
                except ValueError:
                    arg_slices.append(slice(None))

            sliced_input = new_collection(bw.array)[tuple(arg_slices)]
            result = bw.substitute_parameters({"array": sliced_input.expr})
        else:
            # For base Blockwise with multiple inputs in args
            args = bw.args
            new_args = []
            for i in range(0, len(args), 2):
                arg = args[i]
                arg_ind = args[i + 1]

                if arg_ind is None:
                    new_args.extend([arg, arg_ind])
                else:
                    arg_slices = []
                    for dim_idx in arg_ind:
                        try:
                            out_pos = out_ind.index(dim_idx)
                            arg_slices.append(slice_index[out_pos])
                        except ValueError:
                            arg_slices.append(slice(None))

                    sliced_arg = new_collection(arg)[tuple(arg_slices)]
                    new_args.extend([sliced_arg.expr, arg_ind])

            result = Blockwise(
                bw.func,
                bw.out_ind,
                bw.operand("name"),
                bw.operand("token"),
                bw.operand("dtype"),
                bw.operand("adjust_chunks"),
                bw.operand("new_axes"),
                bw.operand("align_arrays"),
                bw.operand("concatenate"),
                bw.operand("_meta_provided"),
                bw.operand("kwargs"),
                *new_args,
            )

        # If we converted integers to slices, extract with [0] to restore dimensions
        if has_integers:
            extract_index = tuple(
                0 if isinstance(idx, Integral) else slice(None) for idx in full_index
            )
            return SliceSlicesIntegers(
                result, extract_index, self.allow_getitem_optimization
            )

        return result

    def _pushdown_through_transpose(self):
        """Push slice through transpose by reordering slice indices."""
        from dask.array._array_expr._collection import new_collection
        from dask.array._array_expr.manipulation._transpose import Transpose

        transpose = self.array
        axes = transpose.axes
        index = self.index

        # Don't handle None/newaxis (adds dimensions)
        if any(idx is None for idx in index):
            return None

        # Pad index to full length
        full_index = index + (slice(None),) * (transpose.ndim - len(index))

        # Map output slice through transpose axes to get input slice
        # axes[i] tells us which input axis becomes output axis i
        # So output axis i gets slice full_index[i], which should go to input axis axes[i]
        input_index = [slice(None)] * len(axes)
        for out_axis, in_axis in enumerate(axes):
            input_index[in_axis] = full_index[out_axis]

        sliced_input = new_collection(transpose.array)[tuple(input_index)]

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

    def _pushdown_through_reduction(self):
        """Push slice through PartialReduce by slicing the input.

        For x.sum(axis=0)[:5], we transform to x[:, :5].sum(axis=0).
        The key is mapping output slice indices back to input indices,
        inserting slice(None) for the reduced axes.

        For integer indices, we convert to size-1 slices, push through,
        then extract with [0] at the end.
        """
        from dask.array._array_expr._collection import new_collection
        from dask.array._array_expr.reductions._reduction import PartialReduce

        reduction = self.array
        index = self.index
        input_array = reduction.array

        # Don't handle None/newaxis
        if any(idx is None for idx in index):
            return None

        # Get reduced axes from split_every
        reduced_axes = set(reduction.split_every.keys())
        keepdims = reduction.keepdims
        input_ndim = input_array.ndim

        if keepdims:
            # With keepdims, output has same ndim as input
            full_index = index + (slice(None),) * (input_ndim - len(index))
        else:
            # Without keepdims, reduced axes are removed from output
            out_axis = [i for i in range(input_ndim) if i not in reduced_axes]
            output_ndim = len(out_axis)
            full_index = index + (slice(None),) * (output_ndim - len(index))

        # Convert integers to size-1 slices to preserve dimensions
        slice_index = tuple(
            slice(idx, idx + 1) if isinstance(idx, Integral) else idx
            for idx in full_index
        )
        has_integers = any(isinstance(idx, Integral) for idx in full_index)

        # Build input index mapping output axes to input axes
        if keepdims:
            input_index = slice_index
        else:
            input_index = []
            out_pos = 0
            for in_ax in range(input_ndim):
                if in_ax in reduced_axes:
                    input_index.append(slice(None))
                else:
                    input_index.append(slice_index[out_pos])
                    out_pos += 1
            input_index = tuple(input_index)

        # Apply the slice to the input and create new PartialReduce
        sliced_input = new_collection(input_array)[input_index]

        result = PartialReduce(
            sliced_input.expr,
            reduction.func,
            reduction.split_every,
            reduction.keepdims,
            reduction.operand("dtype"),
            reduction.operand("name"),
            reduction.reduced_meta,
        )

        # If we converted integers to slices, extract with [0] to restore dimensions
        if has_integers:
            extract_index = tuple(
                0 if isinstance(idx, Integral) else slice(None) for idx in full_index
            )
            return SliceSlicesIntegers(
                result, extract_index, self.allow_getitem_optimization
            )

        return result

    def _pushdown_into_io(self):
        """Push slice into IO expression by setting a region (deferred slice)."""
        from dask.array._array_expr.io import FromArray

        # Only handle slices and integers (no None/newaxis, no fancy indexing)
        index = self.index
        if any(idx is None for idx in index):
            return None
        if any(not isinstance(idx, (slice, Integral)) for idx in index):
            return None
        # Don't push non-unit step slices - FromArray._layer doesn't handle them correctly
        if any(
            isinstance(idx, slice) and idx.step is not None and idx.step != 1
            for idx in index
        ):
            return None

        io_expr = self.array

        # For FromArray, set region instead of slicing eagerly
        if isinstance(io_expr, FromArray):
            source = io_expr.array
            old_chunks = io_expr.chunks  # Use normalized chunks property
            old_region = io_expr.operand("_region")

            # Pad index to full dimensions
            full_index = index + (slice(None),) * (source.ndim - len(index))

            # Check if any integers are present - they need special handling
            has_integers = any(isinstance(idx, Integral) for idx in full_index)

            # Convert integers to 1-element slices for region calculation
            region_index = tuple(
                slice(idx, idx + 1) if isinstance(idx, Integral) else idx
                for idx in full_index
            )

            # Compute new region by combining with existing region
            if old_region is not None:
                # Compose slices: new slice is relative to old region
                new_region = tuple(
                    _compose_slices(old_slc, new_slc, dim_size)
                    for old_slc, new_slc, dim_size in zip(
                        old_region, region_index, source.shape
                    )
                )
            else:
                new_region = region_index

            # Compute new chunks - use same chunk sizes but clipped to new shape
            new_chunks = tuple(
                _compute_sliced_chunks(dim_chunks, slc, dim_size)
                for dim_chunks, slc, dim_size in zip(
                    old_chunks, region_index, io_expr._effective_shape
                )
            )

            # Create new FromArray with region (deferred slice)
            new_io = FromArray(
                source,  # Keep original source, don't slice
                new_chunks,
                lock=io_expr.operand("lock"),
                getitem=io_expr.operand("getitem"),
                inline_array=io_expr.inline_array,
                meta=io_expr.operand("meta"),
                asarray=io_expr.operand("asarray"),
                fancy=io_expr.operand("fancy"),
                _name_override=io_expr.operand("_name_override"),
                _region=new_region,
            )

            # If integers were present, apply them to extract elements
            if has_integers:
                # Build index with 0s for integer dims (they're now size-1)
                extract_index = tuple(
                    0 if isinstance(idx, Integral) else slice(None)
                    for idx in full_index
                )
                return SliceSlicesIntegers(new_io, extract_index, False)

            return new_io

        return None

    @functools.cached_property
    def chunks(self):
        new_blockdims = [
            new_blockdim(d, db, i)
            for d, i, db in zip(self.array.shape, self.index, self.array.chunks)
            if not isinstance(i, Integral)
        ]
        return tuple(map(tuple, new_blockdims))

    def _layer(self) -> dict:
        # Get a list (for each dimension) of dicts{blocknum: slice()}
        block_slices = list(
            map(_slice_1d, self.array.shape, self.array.chunks, self.index)
        )
        sorted_block_slices = [sorted(i.items()) for i in block_slices]

        # (in_name, 1, 1, 2), (in_name, 1, 1, 4), (in_name, 2, 1, 2), ...
        in_names = list(
            product([self.array._name], *[pluck(0, s) for s in sorted_block_slices])
        )

        # (out_name, 0, 0, 0), (out_name, 0, 0, 1), (out_name, 0, 1, 0), ...
        out_names = list(
            product(
                [self._name],
                *[
                    range(len(d))[::-1] if i.step and i.step < 0 else range(len(d))
                    for d, i in zip(block_slices, self.index)
                    if not isinstance(i, Integral)
                ],
            )
        )

        all_slices = list(product(*[pluck(1, s) for s in sorted_block_slices]))

        dsk_out = {
            out_name: (
                Task(out_name, getitem, TaskRef(in_name), slices)
                if not self.allow_getitem_optimization
                or not all(sl == slice(None, None, None) for sl in slices)
                else Alias(out_name, in_name)
            )
            for out_name, in_name, slices in zip(out_names, in_names, all_slices)
        }
        return dsk_out


def _compose_slices(outer_slice, inner_slice, dim_size):
    """Compose two slices: inner_slice is relative to outer_slice's result."""
    # Get the range of the outer slice
    outer_start, outer_stop, outer_step = outer_slice.indices(dim_size)
    outer_len = len(range(outer_start, outer_stop, outer_step))

    # Get the range of the inner slice relative to outer's result
    inner_start, inner_stop, inner_step = inner_slice.indices(outer_len)

    # Compose: offset inner by outer_start
    if outer_step != 1 or inner_step != 1:
        new_start = outer_start + inner_start * outer_step
        new_stop = outer_start + inner_stop * outer_step
        new_step = outer_step * inner_step
    else:
        new_start = outer_start + inner_start
        new_stop = outer_start + inner_stop
        new_step = 1

    return slice(new_start, new_stop, new_step if new_step != 1 else None)


class SlicesWrapNone(SliceSlicesIntegers):
    _parameters = ["array", "index", "allow_getitem_optimization", "where_none"]

    def _simplify_down(self):
        # Disable inherited simplification - SlicesWrapNone adds dimensions via
        # None indexing and the parent class's simplifications don't preserve this
        return None

    @functools.cached_property
    def chunks(self):
        return self.expand(super().chunks, (1,))

    @functools.cached_property
    def expand(self):
        return expander(self.where_none)

    def _layer(self) -> dict:
        dsk = super()._layer()

        where_none_orig = list(self.where_none)
        expand_orig = expander(where_none_orig)

        # Insert ",0" into the key:  ('x', 2, 3) -> ('x', 0, 2, 0, 3)
        dsk2: dict = {}
        for k, v in dsk.items():
            if k[0] == self._name:
                k2 = (self._name,) + self.expand(k[1:], 0)
                if isinstance(v.args[1], Alias):
                    # positional indexing with newaxis
                    indexer = expand_orig(dsk[v.args[1].key].value[1], None)
                    tok = "shuffle-taker-" + tokenize(indexer)
                    dsk2[tok] = DataNode(tok, (1, indexer))
                    arg = TaskRef(tok)
                else:
                    arg = expand_orig(v.args[1], None)
                # raise NotImplementedError
                dsk2[k2] = Task(k2, v.func, v.args[0], arg)
            else:
                dsk2[k] = v
        return dsk2


class TakeUnknownOneChunk(Slice):
    _parameters = ["array", "index", "axis"]

    @functools.cached_property
    def chunks(self):
        return self.array.chunks

    def _layer(self) -> dict:
        slices = [slice(None)] * len(self.array.chunks)
        slices[self.axis] = list(self.index)
        sl = tuple(slices)
        chunk_tuples = list(
            product(*(range(len(c)) for i, c in enumerate(self.array.chunks)))
        )
        dsk = {
            (self._name,)
            + ct: Task(
                (self._name,) + ct, getitem, TaskRef((self.array.name,) + ct), sl
            )
            for ct in chunk_tuples
        }
        return dsk

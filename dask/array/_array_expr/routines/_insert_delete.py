"""Insert/delete operations for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import (
    asanyarray,
    asarray,
    broadcast_to,
    concatenate,
    ravel,
)
from dask.array.utils import validate_axis
from dask.utils import derived_from


@derived_from(np)
def append(arr, values, axis=None):
    """Append values to the end of an array."""
    arr = asanyarray(arr)
    if axis is None:
        if arr.ndim != 1:
            arr = ravel(arr)
        values = ravel(asanyarray(values))
        axis = arr.ndim - 1
    return concatenate((arr, values), axis=axis)


@derived_from(np)
def ediff1d(ary, to_end=None, to_begin=None):
    """Compute the differences between consecutive elements of an array."""
    ary = asarray(ary)

    aryf = ary.flatten()
    r = aryf[1:] - aryf[:-1]

    r = [r]
    if to_begin is not None:
        r = [asarray(to_begin).flatten()] + r
    if to_end is not None:
        r = r + [asarray(to_end).flatten()]
    r = concatenate(r)

    return r


def _split_at_breaks(array, breaks, axis=0):
    """Split an array into a list of arrays (using slices) at the given breaks

    >>> _split_at_breaks(np.arange(6), [3, 5])
    [array([0, 1, 2]), array([3, 4]), array([5])]
    """
    from tlz import concat, sliding_window

    padded_breaks = list(concat([[None], breaks, [None]]))
    slices = [slice(i, j) for i, j in sliding_window(2, padded_breaks)]
    preslice = (slice(None),) * axis
    split_array = [array[preslice + (s,)] for s in slices]
    return split_array


@derived_from(np)
def insert(arr, obj, values, axis):
    """Insert values along the given axis before the given indices."""
    from tlz import interleave

    # axis is a required argument here to avoid needing to deal with the numpy
    # default case (which reshapes the array to make it flat)
    arr = asarray(arr)
    axis = validate_axis(axis, arr.ndim)

    if isinstance(obj, slice):
        obj = np.arange(*obj.indices(arr.shape[axis]))
    obj = np.asarray(obj)
    scalar_obj = obj.ndim == 0
    if scalar_obj:
        obj = np.atleast_1d(obj)

    obj = np.where(obj < 0, obj + arr.shape[axis], obj)
    if (np.diff(obj) < 0).any():
        raise NotImplementedError(
            "da.insert only implemented for monotonic ``obj`` argument"
        )

    split_arr = _split_at_breaks(arr, np.unique(obj), axis)

    if getattr(values, "ndim", 0) == 0:
        # we need to turn values into a dask array
        values = asarray(values)

        values_shape = tuple(
            len(obj) if axis == n else s for n, s in enumerate(arr.shape)
        )
        values = broadcast_to(values, values_shape)
    elif scalar_obj:
        values = values[(slice(None),) * axis + (None,)]

    values = asarray(values)
    values_chunks = tuple(
        values_bd if axis == n else arr_bd
        for n, (arr_bd, values_bd) in enumerate(zip(arr.chunks, values.chunks))
    )
    values = values.rechunk(values_chunks)

    counts = np.bincount(obj)[:-1]
    values_breaks = np.cumsum(counts[counts > 0])
    split_values = _split_at_breaks(values, values_breaks, axis)

    interleaved = list(interleave([split_arr, split_values]))
    interleaved = [i for i in interleaved if i.size]
    return concatenate(interleaved, axis=axis)


@derived_from(np)
def delete(arr, obj, axis):
    """Remove elements from an array along an axis."""
    # axis is a required argument here to avoid needing to deal with the numpy
    # default case (which reshapes the array to make it flat)
    arr = asarray(arr)
    axis = validate_axis(axis, arr.ndim)

    if isinstance(obj, slice):
        tmp = np.arange(*obj.indices(arr.shape[axis]))
        obj = tmp[::-1] if obj.step and obj.step < 0 else tmp
    else:
        obj = np.asarray(obj)
        obj = np.where(obj < 0, obj + arr.shape[axis], obj)
        obj = np.unique(obj)

    target_arr = _split_at_breaks(arr, obj, axis)

    target_arr = [
        (
            arr[
                tuple(
                    slice(1, None) if axis == n else slice(None)
                    for n in range(arr.ndim)
                )
            ]
            if i != 0
            else arr
        )
        for i, arr in enumerate(target_arr)
    ]
    return concatenate(target_arr, axis=axis)

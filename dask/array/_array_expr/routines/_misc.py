"""Miscellaneous utility functions for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import Array, asarray, elemwise
from dask.array.utils import validate_axis
from dask.utils import derived_from


@derived_from(np)
def ndim(a):
    """Return the number of dimensions of an array."""
    a = asarray(a)
    return a.ndim


@derived_from(np)
def shape(a):
    """Return the shape of an array."""
    a = asarray(a)
    return a.shape


def result_type(*arrays_and_dtypes):
    """Returns the type from NumPy type promotion rules."""
    args = [a.dtype if isinstance(a, Array) else a for a in arrays_and_dtypes]
    return np.result_type(*args)


def compress(condition, a, axis=None):
    """
    Return selected slices of an array along given axis.

    This docstring was copied from numpy.compress.

    Some inconsistencies with the Dask version may exist.

    Parameters
    ----------
    condition : 1-D array of bools
        Array that selects which entries to return. If len(condition)
        is less than the size of a along the given axis, then output is
        truncated to the length of the condition array.
    a : array_like
        Array from which to extract a part.
    axis : int, optional
        Axis along which to take slices. If None (default), work on the
        flattened array.

    Returns
    -------
    compressed_array : ndarray
        A copy of a without the slices along axis for which condition
        is false.
    """
    from dask.array.utils import is_arraylike

    if not is_arraylike(condition):
        condition = np.asarray(condition)
    condition = condition.astype(bool)
    a = asarray(a)

    if condition.ndim != 1:
        raise ValueError("Condition must be one dimensional")

    if axis is None:
        a = a.ravel()
        axis = 0
    axis = validate_axis(axis, a.ndim)

    # Treat `condition` as filled with `False` (if it is too short)
    a = a[
        tuple(
            slice(None, len(condition)) if i == axis else slice(None)
            for i in range(a.ndim)
        )
    ]

    # Use `condition` to select along 1 dimension
    a = a[tuple(condition if i == axis else slice(None) for i in range(a.ndim))]

    return a


def _take_constant(indices, a, axis):
    """Take from a constant array using indices."""
    return np.take(a, indices, axis)


def _take_dask_array_from_numpy(a, indices, axis):
    """Take from a numpy array using a dask array of indices."""
    assert isinstance(a, np.ndarray)
    assert isinstance(indices, Array)

    return elemwise(_take_constant, indices, dtype=a.dtype, a=a, axis=axis)


@derived_from(np)
def take(a, indices, axis=0):
    """
    Take elements from an array along an axis.

    This docstring was copied from numpy.take.

    Parameters
    ----------
    a : dask array
        The source array.
    indices : array_like
        The indices of the values to extract.
    axis : int, optional
        The axis over which to select values.

    Returns
    -------
    out : dask array
        The returned array has the same type as a.
    """
    a = asarray(a)
    axis = validate_axis(axis, a.ndim)

    if isinstance(a, np.ndarray) and isinstance(indices, Array):
        return _take_dask_array_from_numpy(a, indices, axis)
    else:
        return a[(slice(None),) * axis + (indices,)]

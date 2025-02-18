from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asanyarray, asarray, stack
from dask.array._array_expr._creation import indices
from dask.array.utils import validate_axis
from dask.utils import derived_from, is_arraylike


@derived_from(np)
def ravel(array_like):
    return asanyarray(array_like).reshape((-1,))


@derived_from(np)
def argwhere(a):
    a = asarray(a)

    nz = isnonzero(a).flatten()

    ind = indices(a.shape, dtype=np.intp, chunks=a.chunks)
    if ind.ndim > 1:
        ind = stack([ind[i].ravel() for i in range(len(ind))], axis=1)
    ind = compress(nz, ind, axis=0)

    return ind


@derived_from(np)
def compress(condition, a, axis=None):
    if not is_arraylike(condition):
        # Allow `condition` to be anything array-like, otherwise ensure `condition`
        # is a numpy array.
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


def _isnonzero_vec(v):
    return bool(np.count_nonzero(v))


_isnonzero_vec = np.vectorize(_isnonzero_vec, otypes=[bool])


def _isnonzero(a):
    # Output of np.vectorize can't be pickled
    return _isnonzero_vec(a)


def isnonzero(a):
    """Handle special cases where conversion to bool does not work correctly.
    xref: https://github.com/numpy/numpy/issues/9479
    """
    try:
        np.zeros([], dtype=a.dtype).astype(bool)
    except ValueError:
        return a.map_blocks(_isnonzero, dtype=bool)
    else:
        return a.astype(bool)

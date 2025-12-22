"""Nonzero-related functions for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asarray, elemwise, stack
from dask.utils import derived_from


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
        return elemwise(_isnonzero, a, dtype=bool)
    else:
        return a.astype(bool)


@derived_from(np)
def count_nonzero(a, axis=None):
    """Counts the number of non-zero values in the array."""
    return isnonzero(asarray(a)).astype(np.intp).sum(axis=axis)


@derived_from(np)
def argwhere(a):
    """Find the indices of array elements that are non-zero."""
    from dask.array._array_expr._routines import compress
    from dask.array._array_expr.creation import indices

    a = asarray(a)

    nz = isnonzero(a).flatten()

    ind = indices(a.shape, dtype=np.intp, chunks=a.chunks)
    if ind.ndim > 1:
        ind = stack([ind[i].ravel() for i in range(len(ind))], axis=1)
    ind = compress(nz, ind, axis=0)

    return ind


@derived_from(np)
def flatnonzero(a):
    """Return indices that are non-zero in the flattened array."""
    return argwhere(asarray(a).ravel())[:, 0]


@derived_from(np)
def nonzero(a):
    """Return the indices of the elements that are non-zero."""
    ind = argwhere(a)
    if ind.ndim > 1:
        return tuple(ind[:, i] for i in range(ind.shape[1]))
    else:
        return (ind,)

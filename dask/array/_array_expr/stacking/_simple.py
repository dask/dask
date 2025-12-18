"""Simple stacking operations: vstack, hstack, dstack."""

from __future__ import annotations


def vstack(tup, allow_unknown_chunksizes=False):
    """Stack arrays in sequence vertically (row wise).

    See Also
    --------
    numpy.vstack
    """
    # Import here to avoid circular imports
    from dask.array._array_expr._collection import concatenate
    from dask.array._array_expr.manipulation._expand import atleast_2d

    tup = tuple(atleast_2d(x) for x in tup)
    return concatenate(tup, axis=0, allow_unknown_chunksizes=allow_unknown_chunksizes)


def hstack(tup, allow_unknown_chunksizes=False):
    """Stack arrays in sequence horizontally (column wise).

    See Also
    --------
    numpy.hstack
    """
    # Import here to avoid circular imports
    from dask.array._array_expr._collection import concatenate

    if all(x.ndim == 1 for x in tup):
        return concatenate(tup, axis=0, allow_unknown_chunksizes=allow_unknown_chunksizes)
    else:
        return concatenate(tup, axis=1, allow_unknown_chunksizes=allow_unknown_chunksizes)


def dstack(tup, allow_unknown_chunksizes=False):
    """Stack arrays in sequence depth wise (along third axis).

    See Also
    --------
    numpy.dstack
    """
    # Import here to avoid circular imports
    from dask.array._array_expr._collection import concatenate
    from dask.array._array_expr.manipulation._expand import atleast_3d

    tup = tuple(atleast_3d(x) for x in tup)
    return concatenate(tup, axis=2, allow_unknown_chunksizes=allow_unknown_chunksizes)

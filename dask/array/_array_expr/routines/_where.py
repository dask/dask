"""Where operation."""

from __future__ import annotations

import numpy as np


def where(condition, x=None, y=None):
    """Return elements chosen from x or y depending on condition.

    Parameters
    ----------
    condition : array_like, bool
        Where True, yield x, otherwise yield y.
    x, y : array_like
        Values from which to choose. x, y and condition need to be
        broadcastable to some shape.

    Returns
    -------
    out : Array
        An array with elements from x where condition is True,
        and elements from y elsewhere.

    See Also
    --------
    numpy.where

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.arange(10, chunks=5)
    >>> da.where(x < 5, x, 10 * x).compute()  # doctest: +NORMALIZE_WHITESPACE
    array([ 0,  1,  2,  3,  4, 50, 60, 70, 80, 90])
    """
    # Lazy imports to avoid circular dependencies
    from dask.array._array_expr.core import asarray
    from dask.array._array_expr.core._blockwise_funcs import elemwise

    if (x is None) != (y is None):
        raise ValueError("either both or neither of x and y should be given")
    if (x is None) and (y is None):
        # Single arg case - returns indices of nonzero elements
        from dask.array._array_expr._routines import nonzero

        return nonzero(condition)

    # Optimization: for scalar conditions, avoid elemwise overhead
    if np.isscalar(condition):
        from dask.array._array_expr._broadcast import broadcast_to
        from dask.array.core import broadcast_shapes
        from dask.array.routines import result_type

        dtype = result_type(x, y)
        x = asarray(x)
        y = asarray(y)
        shape = broadcast_shapes(x.shape, y.shape)
        out = x if condition else y
        return broadcast_to(out, shape).astype(dtype)

    # Use elemwise with np.where to handle all cases
    return elemwise(np.where, condition, x, y)

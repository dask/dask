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
        raise NotImplementedError(
            "where(condition) is not yet implemented in array-expr mode. "
            "Use da.nonzero(condition) instead."
        )

    # Optimization: for scalar conditions with matching shapes, return the chosen array directly
    if np.isscalar(condition):
        from dask.array.core import broadcast_shapes

        x = asarray(x)
        y = asarray(y)
        out = x if condition else y

        # If shapes match, we can return the original array (possibly cast)
        shape = broadcast_shapes(x.shape, y.shape)
        if out.shape == shape:
            dtype = np.result_type(x.dtype, y.dtype)
            if out.dtype == dtype:
                return out
            return out.astype(dtype)

    # Use elemwise with np.where to handle all cases
    return elemwise(np.where, condition, x, y)

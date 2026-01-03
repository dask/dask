"""Difference operation."""

from __future__ import annotations


def diff(a, n=1, axis=-1, prepend=None, append=None):
    """Calculate the n-th discrete difference along the given axis.

    Parameters
    ----------
    a : array_like
        Input array.
    n : int, optional
        The number of times values are differenced. Default is 1.
    axis : int, optional
        The axis along which the difference is taken. Default is -1.
    prepend, append : array_like, optional
        Values to prepend or append to a along axis prior to
        performing the difference.

    Returns
    -------
    diff : Array
        The n-th differences.

    See Also
    --------
    numpy.diff
    """
    # Lazy imports to avoid circular dependencies
    from dask.array._array_expr._broadcast import broadcast_to
    from dask.array._array_expr.core import asarray
    from dask.array._array_expr.stacking import concatenate

    a = asarray(a)
    n = int(n)
    axis = int(axis)

    if n == 0:
        return a
    if n < 0:
        raise ValueError(f"order must be non-negative but got {n}")

    combined = []
    if prepend is not None:
        prepend = asarray(prepend)
        if prepend.ndim == 0:
            shape = list(a.shape)
            shape[axis] = 1
            prepend = broadcast_to(prepend, tuple(shape))
        combined.append(prepend)

    combined.append(a)

    if append is not None:
        append = asarray(append)
        if append.ndim == 0:
            shape = list(a.shape)
            shape[axis] = 1
            append = broadcast_to(append, tuple(shape))
        combined.append(append)

    if len(combined) > 1:
        a = concatenate(combined, axis)

    sl_1 = a.ndim * [slice(None)]
    sl_2 = a.ndim * [slice(None)]

    sl_1[axis] = slice(1, None)
    sl_2[axis] = slice(None, -1)

    sl_1 = tuple(sl_1)
    sl_2 = tuple(sl_2)

    r = a
    for _ in range(n):
        r = r[sl_1] - r[sl_2]

    return r

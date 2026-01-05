from __future__ import annotations


def trace(a, offset=0, axis1=0, axis2=1, dtype=None):
    """
    Return the sum along diagonals of the array.

    This docstring was copied from numpy.trace.

    Some inconsistencies with the Dask version may exist.

    If `a` is 2-D, the sum along its diagonal with the given offset
    is returned, i.e., the sum of elements ``a[i,i+offset]`` for all i.

    If `a` has more than two dimensions, then the axes specified by axis1 and
    axis2 are used to determine the 2-D sub-arrays whose traces are returned.
    The shape of the resulting array is the same as that of `a` with `axis1`
    and `axis2` removed.

    Parameters
    ----------
    a : array_like
        Input array, from which the diagonals are taken.
    offset : int, optional
        Offset of the diagonal from the main diagonal. Can be both positive
        and negative. Defaults to 0.
    axis1, axis2 : int, optional
        Axes to be used as the first and second axis of the 2-D sub-arrays
        from which the diagonals should be taken. Defaults are the first two
        axes of `a`.
    dtype : dtype, optional
        Determines the data-type of the returned array and of the accumulator
        where the elements are summed. If dtype has the value None and `a` is
        of integer type of precision less than the default integer precision,
        then the default integer precision is used. Otherwise, the precision
        is the same as that of `a`.

    Returns
    -------
    sum_along_diagonals : ndarray
        If `a` is 2-D, the sum along the diagonal is returned.  If `a` has
        larger dimensions, then an array of sums along diagonals is returned.

    See Also
    --------
    diag, diagonal, diagflat

    Examples
    --------
    >>> import dask.array as da
    >>> da.trace(da.eye(3)).compute()  # doctest: +SKIP
    3.0
    """
    from dask.array._array_expr.creation import diagonal

    return diagonal(a, offset=offset, axis1=axis1, axis2=axis2).sum(-1, dtype=dtype)

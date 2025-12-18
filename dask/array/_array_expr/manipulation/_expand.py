"""Expand operations: expand_dims, atleast_1d, atleast_2d, atleast_3d."""

from __future__ import annotations


def expand_dims(a, axis):
    """Expand the shape of an array.

    Insert a new axis that will appear at the axis position in the expanded
    array shape.

    Parameters
    ----------
    a : array_like
        Input array.
    axis : int or tuple of ints
        Position in the expanded axes where the new axis (or axes) is placed.

    Returns
    -------
    result : Array
        Array with the number of dimensions increased.

    See Also
    --------
    numpy.expand_dims
    """
    from dask.array.utils import validate_axis

    if axis is None:
        raise TypeError("axis must be an integer, not None")

    if type(axis) not in (tuple, list):
        axis = (axis,)

    out_ndim = len(axis) + a.ndim
    axis = validate_axis(axis, out_ndim)

    shape_it = iter(a.shape)
    shape = [1 if ax in axis else next(shape_it) for ax in range(out_ndim)]

    return a.reshape(shape)


def atleast_1d(*arys):
    """Convert inputs to arrays with at least one dimension.

    Parameters
    ----------
    arys : array_like
        One or more array-like sequences. Non-array inputs are converted
        to arrays. Arrays that already have one or more dimensions are
        preserved.

    Returns
    -------
    ret : Array or tuple of Arrays
        An array, or tuple of arrays, each with a.ndim >= 1.

    See Also
    --------
    numpy.atleast_1d
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import NUMPY_GE_200

    new_arys = []
    for x in arys:
        x = asanyarray(x)
        if x.ndim == 0:
            x = x[None]
        new_arys.append(x)

    if len(new_arys) == 1:
        return new_arys[0]
    else:
        if NUMPY_GE_200:
            new_arys = tuple(new_arys)
        return new_arys


def atleast_2d(*arys):
    """View inputs as arrays with at least two dimensions.

    Parameters
    ----------
    arys : array_like
        One or more array-like sequences. Non-array inputs are converted
        to arrays. Arrays that already have two or more dimensions are
        preserved.

    Returns
    -------
    ret : Array or tuple of Arrays
        An array, or tuple of arrays, each with a.ndim >= 2.

    See Also
    --------
    numpy.atleast_2d
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import NUMPY_GE_200

    new_arys = []
    for x in arys:
        x = asanyarray(x)
        if x.ndim == 0:
            x = x[None, None]
        elif x.ndim == 1:
            x = x[None, :]
        new_arys.append(x)

    if len(new_arys) == 1:
        return new_arys[0]
    else:
        if NUMPY_GE_200:
            new_arys = tuple(new_arys)
        return new_arys


def atleast_3d(*arys):
    """View inputs as arrays with at least three dimensions.

    Parameters
    ----------
    arys : array_like
        One or more array-like sequences. Non-array inputs are converted
        to arrays. Arrays that already have three or more dimensions are
        preserved.

    Returns
    -------
    ret : Array or tuple of Arrays
        An array, or tuple of arrays, each with a.ndim >= 3.

    See Also
    --------
    numpy.atleast_3d
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array.numpy_compat import NUMPY_GE_200

    new_arys = []
    for x in arys:
        x = asanyarray(x)
        if x.ndim == 0:
            x = x[None, None, None]
        elif x.ndim == 1:
            x = x[None, :, None]
        elif x.ndim == 2:
            x = x[:, :, None]
        new_arys.append(x)

    if len(new_arys) == 1:
        return new_arys[0]
    else:
        if NUMPY_GE_200:
            new_arys = tuple(new_arys)
        return new_arys

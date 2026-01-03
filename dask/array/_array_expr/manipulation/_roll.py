"""Roll operation."""

from __future__ import annotations

from numbers import Integral


def roll(array, shift, axis=None):
    """Roll array elements along a given axis.

    Elements that roll beyond the last position are re-introduced at the first.

    Parameters
    ----------
    array : array_like
        Input array.
    shift : int or tuple of ints
        The number of places by which elements are shifted.
    axis : int or tuple of ints, optional
        Axis or axes along which elements are shifted. By default, the
        array is flattened before shifting, after which the original shape
        is restored.

    Returns
    -------
    result : Array
        Array with the same shape as array.

    See Also
    --------
    numpy.roll
    """
    # Import here to avoid circular imports
    from dask.array._array_expr._collection import concatenate, ravel

    result = array

    if axis is None:
        result = ravel(result)

        if not isinstance(shift, Integral):
            raise TypeError(
                "Expect `shift` to be an instance of Integral when `axis` is None."
            )

        shift = (shift,)
        axis = (0,)
    else:
        try:
            len(shift)
        except TypeError:
            shift = (shift,)
        try:
            len(axis)
        except TypeError:
            axis = (axis,)

    if len(shift) != len(axis):
        raise ValueError("Must have the same number of shifts as axes.")

    for i, s in zip(axis, shift):
        shape = result.shape[i]
        s = 0 if shape == 0 else -s % shape

        sl1 = result.ndim * [slice(None)]
        sl2 = result.ndim * [slice(None)]

        sl1[i] = slice(s, None)
        sl2[i] = slice(None, s)

        sl1 = tuple(sl1)
        sl2 = tuple(sl2)

        result = concatenate([result[sl1], result[sl2]], axis=i)

    result = result.reshape(array.shape)
    # Ensure that the output is always a new array object
    result = result.copy() if result is array else result

    return result

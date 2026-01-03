"""Flip operations: flip, flipud, fliplr, rot90."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np


def flip(m, axis=None):
    """Reverse element order along axis.

    See Also
    --------
    numpy.flip
    """
    from dask.array._array_expr.core import asanyarray

    m = asanyarray(m)

    sl = m.ndim * [slice(None)]
    if axis is None:
        axis = range(m.ndim)
    if not isinstance(axis, Iterable):
        axis = (axis,)
    try:
        for ax in axis:
            sl[ax] = slice(None, None, -1)
    except IndexError as e:
        raise ValueError(f"`axis` of {axis} invalid for {m.ndim}-D array") from e
    sl = tuple(sl)

    return m[sl]


def flipud(m):
    """Flip array in the up/down direction.

    See Also
    --------
    numpy.flipud
    """
    return flip(m, 0)


def fliplr(m):
    """Flip array in the left/right direction.

    See Also
    --------
    numpy.fliplr
    """
    return flip(m, 1)


def rot90(m, k=1, axes=(0, 1)):
    """Rotate an array by 90 degrees in the plane specified by axes.

    See Also
    --------
    numpy.rot90
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array._array_expr.manipulation._transpose import transpose

    axes = tuple(axes)
    if len(axes) != 2:
        raise ValueError("len(axes) must be 2.")

    m = asanyarray(m)

    if axes[0] == axes[1] or np.absolute(axes[0] - axes[1]) == m.ndim:
        raise ValueError("Axes must be different.")

    if axes[0] >= m.ndim or axes[0] < -m.ndim or axes[1] >= m.ndim or axes[1] < -m.ndim:
        raise ValueError(f"Axes={axes} out of range for array of ndim={m.ndim}.")

    k %= 4

    if k == 0:
        return m[:]
    if k == 2:
        return flip(flip(m, axes[0]), axes[1])

    axes_list = list(range(0, m.ndim))
    (axes_list[axes[0]], axes_list[axes[1]]) = (axes_list[axes[1]], axes_list[axes[0]])

    if k == 1:
        return transpose(flip(m, axes[1]), axes_list)
    else:
        # k == 3
        return flip(transpose(m, axes_list), axes[1])

"""Matrix and vector norms for array-expr."""

from __future__ import annotations

from numbers import Number

import numpy as np

from dask.utils import derived_from


@derived_from(np.linalg)
def norm(x, ord=None, axis=None, keepdims=False):
    """Matrix or vector norm.

    This function uses array operations (abs, sum, max, min) which are
    already implemented in array-expr.
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array._array_expr.linalg._svd import svd

    x = asanyarray(x)

    if axis is None:
        axis = tuple(range(x.ndim))
    elif isinstance(axis, Number):
        axis = (int(axis),)
    else:
        axis = tuple(axis)

    if len(axis) > 2:
        raise ValueError("Improper number of dimensions to norm.")

    if ord == "fro":
        ord = None
        if len(axis) == 1:
            raise ValueError("Invalid norm order for vectors.")

    r = abs(x)

    if ord is None:
        r = (r**2).sum(axis=axis, keepdims=keepdims) ** 0.5
    elif ord == "nuc":
        if len(axis) == 1:
            raise ValueError("Invalid norm order for vectors.")
        if x.ndim > 2:
            raise NotImplementedError("SVD based norm not implemented for ndim > 2")

        r = svd(x)[1][None].sum(keepdims=keepdims)
    elif ord == np.inf:
        if len(axis) == 1:
            r = r.max(axis=axis, keepdims=keepdims)
        else:
            r = r.sum(axis=axis[1], keepdims=True).max(axis=axis[0], keepdims=True)
            if keepdims is False:
                r = r.squeeze(axis=axis)
    elif ord == -np.inf:
        if len(axis) == 1:
            r = r.min(axis=axis, keepdims=keepdims)
        else:
            r = r.sum(axis=axis[1], keepdims=True).min(axis=axis[0], keepdims=True)
            if keepdims is False:
                r = r.squeeze(axis=axis)
    elif ord == 0:
        if len(axis) == 2:
            raise ValueError("Invalid norm order for matrices.")

        r = (r != 0).astype(r.dtype).sum(axis=axis, keepdims=keepdims)
    elif ord == 1:
        if len(axis) == 1:
            r = r.sum(axis=axis, keepdims=keepdims)
        else:
            r = r.sum(axis=axis[0], keepdims=True).max(axis=axis[1], keepdims=True)
            if keepdims is False:
                r = r.squeeze(axis=axis)
    elif len(axis) == 2 and ord == -1:
        r = r.sum(axis=axis[0], keepdims=True).min(axis=axis[1], keepdims=True)
        if keepdims is False:
            r = r.squeeze(axis=axis)
    elif len(axis) == 2 and ord == 2:
        if x.ndim > 2:
            raise NotImplementedError("SVD based norm not implemented for ndim > 2")
        r = svd(x)[1][None].max(keepdims=keepdims)
    elif len(axis) == 2 and ord == -2:
        if x.ndim > 2:
            raise NotImplementedError("SVD based norm not implemented for ndim > 2")
        r = svd(x)[1][None].min(keepdims=keepdims)
    else:
        if len(axis) == 2:
            raise ValueError("Invalid norm order for matrices.")

        r = (r**ord).sum(axis=axis, keepdims=keepdims) ** (1.0 / ord)

    return r

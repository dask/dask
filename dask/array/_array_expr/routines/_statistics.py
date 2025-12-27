"""Statistical functions for array-expr."""

from __future__ import annotations

import warnings

import numpy as np

from dask.array._array_expr._collection import (
    array,
    asanyarray,
    asarray,
    broadcast_to,
    concatenate,
)
from dask.utils import derived_from


def result_type(*arrays_and_dtypes):
    """Returns the type from NumPy type promotion rules."""
    from dask.array._array_expr._collection import Array

    args = [a.dtype if isinstance(a, Array) else a for a in arrays_and_dtypes]
    return np.result_type(*args)


def average(a, axis=None, weights=None, returned=False, keepdims=False):
    """Compute the weighted average along the specified axis."""
    a = asanyarray(a)

    if weights is None:
        avg = a.mean(axis, keepdims=keepdims)
        scl = avg.dtype.type(a.size / avg.size)
    else:
        wgt = asanyarray(weights)

        if issubclass(a.dtype.type, (np.integer, np.bool_)):
            result_dtype = result_type(a.dtype, wgt.dtype, "f8")
        else:
            result_dtype = result_type(a.dtype, wgt.dtype)

        if a.shape != wgt.shape:
            if axis is None:
                raise TypeError(
                    "Axis must be specified when shapes of a and weights differ."
                )
            if wgt.ndim != 1:
                raise TypeError(
                    "1D weights expected when shapes of a and weights differ."
                )
            if wgt.shape[0] != a.shape[axis]:
                raise ValueError(
                    "Length of weights not compatible with specified axis."
                )

            wgt = broadcast_to(wgt, (a.ndim - 1) * (1,) + wgt.shape)
            wgt = wgt.swapaxes(-1, axis)

        scl = wgt.sum(axis=axis, dtype=result_dtype, keepdims=keepdims)
        from dask.array._array_expr._ufunc import multiply

        avg = multiply(a, wgt, dtype=result_dtype).sum(axis, keepdims=keepdims) / scl

    if returned:
        if scl.shape != avg.shape:
            scl = broadcast_to(scl, avg.shape)
        return avg, scl
    else:
        return avg


@derived_from(np)
def cov(
    m,
    y=None,
    rowvar=True,
    bias=False,
    ddof=None,
    fweights=None,
    aweights=None,
    *,
    dtype=None,
):
    """Estimate a covariance matrix."""
    from dask.array._array_expr._ufunc import true_divide
    from dask.array._array_expr.linalg import dot

    if ddof is not None and ddof != int(ddof):
        raise ValueError("ddof must be integer")

    m = asarray(m)

    if y is None:
        dtype = result_type(m, np.float64)
    else:
        y = asarray(y)
        dtype = result_type(m, y, np.float64)

    if m.ndim > 2:
        raise ValueError("m has more than 2 dimensions")
    if y is not None and y.ndim > 2:
        raise ValueError("y has more than 2 dimensions")

    X = array(m, ndmin=2, dtype=dtype)

    if ddof is None:
        ddof = 1 if bias == 0 else 0

    if not rowvar and m.ndim != 1:
        X = X.T
    if X.shape[0] == 0:
        return array([]).reshape(0, 0)
    if y is not None:
        y = array(y, ndmin=2, dtype=dtype)
        if not rowvar and y.shape[0] != 1:
            y = y.T
        X = concatenate((X, y), axis=0)

    w = None
    if fweights is not None:
        fweights = asarray(fweights, dtype=float)
        if fweights.ndim > 1:
            raise RuntimeError("cannot handle multidimensional fweights")
        if fweights.shape[0] != X.shape[1]:
            raise RuntimeError("incompatible numbers of samples and fweights")
        w = fweights
    if aweights is not None:
        aweights = asarray(aweights, dtype=float)
        if aweights.ndim > 1:
            raise RuntimeError("cannot handle multidimensional aweights")
        if aweights.shape[0] != X.shape[1]:
            raise RuntimeError("incompatible numbers of samples and aweights")
        if w is None:
            w = aweights
        else:
            w *= aweights

    avg, w_sum = average(X, axis=1, weights=w, returned=True)
    w_sum = w_sum[0]

    if w is None:
        fact = X.shape[1] - ddof
    elif ddof == 0:
        fact = w_sum
    elif aweights is None:
        fact = w_sum - ddof
    else:
        fact = w_sum - ddof * (w * aweights).sum() / w_sum

    if fact <= 0:
        warnings.warn("Degrees of freedom <= 0 for slice", RuntimeWarning)
        fact = 0.0

    X -= avg[:, None]
    if w is None:
        X_T = X.T
    else:
        X_T = (X * w).T
    c = dot(X, X_T.conj())
    c *= true_divide(1, fact)
    return c.squeeze()


@derived_from(np)
def corrcoef(x, y=None, rowvar=1):
    """Return Pearson product-moment correlation coefficients."""
    from dask.array._array_expr._ufunc import sqrt
    from dask.array._array_expr.creation import diag

    c = cov(x, y, rowvar)

    if c.shape == ():
        return c / c
    d = diag(c)
    d = d.reshape((d.shape[0], 1))
    sqr_d = sqrt(d)
    return (c / sqr_d) / sqr_d.T

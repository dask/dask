"""Tensor operations for array-expr (tensordot, dot, vdot, matmul)."""

from __future__ import annotations

from collections.abc import Iterable
from numbers import Integral

import numpy as np

from dask.array.core import is_scalar_for_elemwise, tensordot_lookup
from dask.utils import derived_from


def _result_type(*args):
    """Compute result dtype for operation."""
    args = [a if is_scalar_for_elemwise(a) else a.dtype for a in args]
    return np.result_type(*args)


def _tensordot(a, b, axes, is_sparse):
    """Helper function for tensordot that handles the actual numpy computation."""
    x = max([a, b], key=lambda x: x.__array_priority__)
    tensordot = tensordot_lookup.dispatch(type(x))
    x = tensordot(a, b, axes=axes)
    if is_sparse and len(axes[0]) == 1:
        return x
    else:
        ind = [slice(None, None)] * x.ndim
        for a in sorted(axes[0]):
            ind.insert(a, None)
        x = x[tuple(ind)]
        return x


def _tensordot_is_sparse(x):
    """Check if array is sparse (scipy sparse, not pydata sparse)."""
    is_sparse = "sparse" in str(type(x._meta))
    if is_sparse:
        # exclude pydata sparse arrays, no workaround required for these in tensordot
        is_sparse = "sparse._coo.core.COO" not in str(type(x._meta))
    return is_sparse


@derived_from(np)
def tensordot(lhs, rhs, axes=2):
    """Compute tensor dot product along specified axes.

    Parameters
    ----------
    lhs : array_like
        Left argument
    rhs : array_like
        Right argument
    axes : int or tuple of (int, int) or tuple of (sequence[int], sequence[int])
        If integer, sum over the last `axes` axes of `lhs` and first `axes`
        axes of `rhs`. If tuple, specifies axes to contract.

    Returns
    -------
    output : dask array

    See Also
    --------
    numpy.tensordot
    """
    from dask.array._array_expr._collection import Array, asarray, blockwise

    if not isinstance(lhs, Array):
        lhs = asarray(lhs)
    if not isinstance(rhs, Array):
        rhs = asarray(rhs)

    if isinstance(axes, Iterable):
        left_axes, right_axes = axes
    else:
        left_axes = tuple(range(lhs.ndim - axes, lhs.ndim))
        right_axes = tuple(range(0, axes))
    if isinstance(left_axes, Integral):
        left_axes = (left_axes,)
    if isinstance(right_axes, Integral):
        right_axes = (right_axes,)
    if isinstance(left_axes, list):
        left_axes = tuple(left_axes)
    if isinstance(right_axes, list):
        right_axes = tuple(right_axes)

    is_sparse = _tensordot_is_sparse(lhs) or _tensordot_is_sparse(rhs)
    if is_sparse and len(left_axes) == 1:
        concatenate = True
    else:
        concatenate = False

    dt = np.promote_types(lhs.dtype, rhs.dtype)

    left_index = list(range(lhs.ndim))
    right_index = list(range(lhs.ndim, lhs.ndim + rhs.ndim))
    out_index = left_index + right_index
    adjust_chunks = {}

    for l, r in zip(left_axes, right_axes):
        out_index.remove(right_index[r])
        right_index[r] = left_index[l]
        if concatenate:
            out_index.remove(left_index[l])
        else:
            adjust_chunks[left_index[l]] = lambda c: 1

    # Compute explicit meta to preserve masked array type
    # (compute_meta fails for masked arrays due to reshape issues with 0-dim arrays)
    meta = None
    for arr in (lhs, rhs):
        if hasattr(arr._meta, "mask"):  # MaskedArray check
            out_ndim = len(out_index)
            meta = np.ma.empty((0,) * out_ndim, dtype=dt)
            break

    intermediate = blockwise(
        _tensordot,
        out_index,
        lhs,
        left_index,
        rhs,
        right_index,
        dtype=dt,
        concatenate=concatenate,
        adjust_chunks=adjust_chunks,
        axes=(left_axes, right_axes),
        is_sparse=is_sparse,
        meta=meta,
    )

    if concatenate:
        return intermediate
    else:
        left_axes = [ax if ax >= 0 else lhs.ndim + ax for ax in left_axes]
        return intermediate.sum(axis=left_axes)


@derived_from(np, ua_args=["out"])
def dot(a, b):
    """Dot product of two arrays.

    For 2-D arrays it is equivalent to matrix multiplication,
    for 1-D arrays to inner product of vectors.

    Parameters
    ----------
    a : array_like
        First argument
    b : array_like
        Second argument

    Returns
    -------
    output : dask array

    See Also
    --------
    numpy.dot
    tensordot
    """
    return tensordot(a, b, axes=((a.ndim - 1,), (b.ndim - 2,)))


@derived_from(np)
def vdot(a, b):
    """Return the dot product of two vectors.

    The vdot function handles complex numbers differently than dot:
    if the first argument is complex the complex conjugate of the
    first argument is used for the calculation of the dot product.

    Parameters
    ----------
    a : array_like
        First argument
    b : array_like
        Second argument

    Returns
    -------
    output : dask array

    See Also
    --------
    numpy.vdot
    dot
    """
    from dask.array._array_expr._collection import ravel

    return dot(ravel(a).conj(), ravel(b))


def _matmul(a, b):
    """Helper function for matmul that handles the actual numpy computation."""
    xp = np

    # Check for cupy
    try:
        import cupy

        if hasattr(a, "__cuda_array_interface__") or hasattr(
            b, "__cuda_array_interface__"
        ):
            xp = cupy
    except ImportError:
        pass

    chunk = xp.matmul(a, b)
    # Since we have performed the contraction via xp.matmul
    # but blockwise expects all dimensions back (including
    # the contraction-axis in the 2nd-to-last position of
    # the output), we must then put it back in the expected
    # the position ourselves:
    return chunk[..., xp.newaxis, :]


def _sum_wo_cat(a, axis=None, dtype=None):
    """Sum without concatenation - used for matmul reduction."""
    from functools import partial, reduce

    from dask.array._array_expr.reductions import reduction

    def _chunk_sum(a, axis=None, dtype=None, keepdims=None):
        # Caution: this is not your conventional array-sum: due
        # to the special nature of the preceding blockwise con-
        # traction, each chunk is expected to have exactly the
        # same shape, with a size of 1 for the dimension given
        # by `axis` (the reduction axis). This makes mere ele-
        # ment-wise addition of the arrays possible. Besides,
        # the output can be merely squeezed to lose the `axis`-
        # dimension when keepdims = False
        if type(a) is list:
            out = reduce(partial(np.add, dtype=dtype), a)
        else:
            out = a

        if keepdims:
            return out
        else:
            return out.squeeze(axis[0])

    if dtype is None:
        dtype = getattr(np.zeros(1, dtype=a.dtype).sum(), "dtype", object)

    if a.shape[axis] == 1:
        from dask.array._array_expr._collection import squeeze

        return squeeze(a, axis=axis)

    return reduction(
        a, _chunk_sum, _chunk_sum, axis=axis, dtype=dtype, concatenate=False
    )


@derived_from(np)
def matmul(a, b):
    """Matrix product of two arrays.

    Parameters
    ----------
    a : array_like
        First argument
    b : array_like
        Second argument

    Returns
    -------
    output : dask array

    See Also
    --------
    numpy.matmul
    """
    from dask.array._array_expr._collection import asanyarray, blockwise

    a = asanyarray(a)
    b = asanyarray(b)

    if a.ndim == 0 or b.ndim == 0:
        raise ValueError("`matmul` does not support scalars.")

    a_is_1d = False
    if a.ndim == 1:
        a_is_1d = True
        a = a[np.newaxis, :]

    b_is_1d = False
    if b.ndim == 1:
        b_is_1d = True
        b = b[:, np.newaxis]

    if a.ndim < b.ndim:
        a = a[(b.ndim - a.ndim) * (np.newaxis,)]
    elif a.ndim > b.ndim:
        b = b[(a.ndim - b.ndim) * (np.newaxis,)]

    # out_ind includes all dimensions to prevent contraction
    # in the blockwise below. We set the last two dimensions
    # of the output to the contraction axis and the 2nd
    # (last) dimension of b in that order
    out_ind = tuple(range(a.ndim + 1))
    # lhs_ind includes `a`/LHS dimensions
    lhs_ind = tuple(range(a.ndim))
    # on `b`/RHS everything above 2nd dimension, is the same
    # as `a`, -2 dimension is "contracted" with the last dimension
    # of `a`, last dimension of `b` is `b` specific
    rhs_ind = tuple(range(a.ndim - 2)) + (lhs_ind[-1], a.ndim)

    out = blockwise(
        _matmul,
        out_ind,
        a,
        lhs_ind,
        b,
        rhs_ind,
        adjust_chunks={lhs_ind[-1]: 1},
        dtype=_result_type(a, b),
        concatenate=False,
    )

    # Because contraction + concatenate in blockwise leads to high
    # memory footprints, we want to avoid them. Instead we will perform
    # blockwise (without contraction) followed by reduction. More about
    # this issue: https://github.com/dask/dask/issues/6874

    # We will also perform the reduction without concatenation
    out = _sum_wo_cat(out, axis=-2)

    if a_is_1d or b_is_1d:
        from dask.array._array_expr._collection import squeeze

        if a_is_1d:
            out = squeeze(out, axis=-2)
        if b_is_1d:
            out = squeeze(out, axis=-1)

    return out

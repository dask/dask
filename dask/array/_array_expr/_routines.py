from __future__ import annotations

import functools
import warnings
from collections.abc import Iterable
from functools import partial, reduce, wraps
from numbers import Integral

import numpy as np
from tlz import concat, interleave, sliding_window

from dask._collections import new_collection
from dask.array import chunk
from dask.array._array_expr import ArrayExpr
from dask.array._array_expr._collection import (
    Array,
    asanyarray,
    asarray,
    blockwise,
    broadcast_arrays,
    broadcast_to,
    concatenate,
    elemwise,
    from_array,
    stack,
)
from dask.array._array_expr._creation import arange, diag, empty, indices, ones
from dask.array._array_expr._map_blocks import map_blocks
from dask.array._array_expr._ufunc import multiply, sqrt
from dask.array.core import broadcast_shapes, implements, is_scalar_for_elemwise
from dask.array.dispatch import tensordot_lookup
from dask.array.numpy_compat import NUMPY_GE_200
from dask.array.utils import (
    array_safe,
    asarray_safe,
    meta_from_array,
    safe_wraps,
    validate_axis,
)
from dask.base import is_dask_collection
from dask.core import flatten
from dask.delayed import Delayed, unpack_collections
from dask.utils import apply, derived_from, funcname, is_arraylike, is_cupy_type

# save built-in for histogram functions which use range as a kwarg.
_range = range


@derived_from(np)
def ravel(array_like):
    return asanyarray(array_like).reshape((-1,))


@derived_from(np)
def argwhere(a):
    a = asarray(a)

    nz = isnonzero(a).flatten()

    ind = indices(a.shape, dtype=np.intp, chunks=a.chunks)
    if ind.ndim > 1:
        ind = stack([ind[i].ravel() for i in range(len(ind))], axis=1)
    ind = compress(nz, ind, axis=0)

    return ind


@derived_from(np)
def compress(condition, a, axis=None):
    if not is_arraylike(condition):
        # Allow `condition` to be anything array-like, otherwise ensure `condition`
        # is a numpy array.
        condition = np.asarray(condition)
    condition = condition.astype(bool)
    a = asarray(a)

    if condition.ndim != 1:
        raise ValueError("Condition must be one dimensional")

    if axis is None:
        a = a.ravel()
        axis = 0
    axis = validate_axis(axis, a.ndim)

    # Treat `condition` as filled with `False` (if it is too short)
    a = a[
        tuple(
            slice(None, len(condition)) if i == axis else slice(None)
            for i in range(a.ndim)
        )
    ]

    # Use `condition` to select along 1 dimension
    a = a[tuple(condition if i == axis else slice(None) for i in range(a.ndim))]

    return a


def _isnonzero_vec(v):
    return bool(np.count_nonzero(v))


_isnonzero_vec = np.vectorize(_isnonzero_vec, otypes=[bool])


def _isnonzero(a):
    # Output of np.vectorize can't be pickled
    return _isnonzero_vec(a)


def isnonzero(a):
    """Handle special cases where conversion to bool does not work correctly.
    xref: https://github.com/numpy/numpy/issues/9479
    """
    try:
        np.zeros([], dtype=a.dtype).astype(bool)
    except ValueError:
        return a.map_blocks(_isnonzero, dtype=bool)
    else:
        return a.astype(bool)


@derived_from(np)
def array(x, dtype=None, ndmin=None, *, like=None):
    x = asarray(x, like=like)
    while ndmin is not None and x.ndim < ndmin:
        x = x[None, :]
    if dtype is not None and x.dtype != dtype:
        x = x.astype(dtype)
    return x


@derived_from(np)
def result_type(*args):
    args = [a if is_scalar_for_elemwise(a) else a.dtype for a in args]
    return np.result_type(*args)


@derived_from(np)
def atleast_3d(*arys):
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


@derived_from(np)
def atleast_2d(*arys):
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


@derived_from(np)
def atleast_1d(*arys):
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


@derived_from(np)
def vstack(tup, allow_unknown_chunksizes=False):
    if isinstance(tup, Array):
        raise NotImplementedError(
            "``vstack`` expects a sequence of arrays as the first argument"
        )

    tup = tuple(atleast_2d(x) for x in tup)
    return concatenate(tup, axis=0, allow_unknown_chunksizes=allow_unknown_chunksizes)


@derived_from(np)
def hstack(tup, allow_unknown_chunksizes=False):
    if isinstance(tup, Array):
        raise NotImplementedError(
            "``hstack`` expects a sequence of arrays as the first argument"
        )

    if all(x.ndim == 1 for x in tup):
        return concatenate(
            tup, axis=0, allow_unknown_chunksizes=allow_unknown_chunksizes
        )
    else:
        return concatenate(
            tup, axis=1, allow_unknown_chunksizes=allow_unknown_chunksizes
        )


@derived_from(np)
def dstack(tup, allow_unknown_chunksizes=False):
    if isinstance(tup, Array):
        raise NotImplementedError(
            "``dstack`` expects a sequence of arrays as the first argument"
        )

    tup = tuple(atleast_3d(x) for x in tup)
    return concatenate(tup, axis=2, allow_unknown_chunksizes=allow_unknown_chunksizes)


@derived_from(np)
def swapaxes(a, axis1, axis2):
    if axis1 == axis2:
        return a
    if axis1 < 0:
        axis1 = axis1 + a.ndim
    if axis2 < 0:
        axis2 = axis2 + a.ndim
    ind = list(range(a.ndim))
    out = list(ind)
    out[axis1], out[axis2] = axis2, axis1

    return blockwise(np.swapaxes, out, a, ind, axis1=axis1, axis2=axis2, dtype=a.dtype)


@derived_from(np)
def transpose(a, axes=None):
    if axes:
        if len(axes) != a.ndim:
            raise ValueError("axes don't match array")
        axes = tuple(d + a.ndim if d < 0 else d for d in axes)
    else:
        axes = tuple(range(a.ndim))[::-1]
    return blockwise(
        np.transpose, axes, a, tuple(range(a.ndim)), dtype=a.dtype, axes=axes
    )


def flip(m, axis=None):
    """
    Reverse element order along axis.

    Parameters
    ----------
    m : array_like
        Input array.
    axis : None or int or tuple of ints, optional
        Axis or axes to reverse element order of. None will reverse all axes.

    Returns
    -------
    dask.array.Array
        The flipped array.
    """

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
        raise ValueError(
            f"`axis` of {str(axis)} invalid for {str(m.ndim)}-D array"
        ) from e
    sl = tuple(sl)

    return m[sl]


@derived_from(np)
def flipud(m):
    return flip(m, 0)


@derived_from(np)
def fliplr(m):
    return flip(m, 1)


@derived_from(np)
def rot90(m, k=1, axes=(0, 1)):
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


def _tensordot(a, b, axes, is_sparse):
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
    is_sparse = "sparse" in str(type(x._meta))
    if is_sparse:
        # exclude pydata sparse arrays, no workaround required for these in tensordot
        is_sparse = "sparse._coo.core.COO" not in str(type(x._meta))
    return is_sparse


@derived_from(np)
def tensordot(lhs, rhs, axes=2):
    if not isinstance(lhs, Array):
        lhs = from_array(lhs)
    if not isinstance(rhs, Array):
        rhs = from_array(rhs)

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
    )
    if concatenate:
        return intermediate
    else:
        left_axes = [ax if ax >= 0 else lhs.ndim + ax for ax in left_axes]
        return intermediate.sum(axis=left_axes)


@derived_from(np, ua_args=["out"])
def dot(a, b):
    return tensordot(a, b, axes=((a.ndim - 1,), (b.ndim - 2,)))


@derived_from(np)
def vdot(a, b):
    return dot(a.conj().ravel(), b.ravel())


def _chunk_sum(a, axis=None, dtype=None, keepdims=None):
    # Caution: this is not your conventional array-sum: due
    # to the special nature of the preceding blockwise con-
    # traction,  each chunk is expected to have exactly the
    # same shape,  with a size of 1 for the dimension given
    # by `axis` (the reduction axis).  This makes mere ele-
    # ment-wise addition of the arrays possible.   Besides,
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


def _sum_wo_cat(a, axis=None, dtype=None):
    if dtype is None:
        dtype = getattr(np.zeros(1, dtype=a.dtype).sum(), "dtype", object)

    if a.shape[axis] == 1:
        return a.squeeze(axis)
    from dask.array.reductions import reduction

    return reduction(
        a, _chunk_sum, _chunk_sum, axis=axis, dtype=dtype, concatenate=False
    )


def _matmul(a, b):
    xp = np

    if is_cupy_type(a):
        # This branch appears to  be unnecessary since cupy
        # version 9.0. See the following link:
        # https://github.com/dask/dask/pull/8423#discussion_r768291271
        # But it remains here  for  backward-compatibility.
        # Consider removing it in a future version of dask.
        import cupy

        xp = cupy

    chunk = xp.matmul(a, b)
    # Since we have performed the contraction via xp.matmul
    # but blockwise expects all dimensions back  (including
    # the contraction-axis in  the 2nd-to-last position  of
    # the output), we must then put it back in the expected
    # the position ourselves:
    return chunk[..., xp.newaxis, :]


@derived_from(np)
def matmul(a, b):
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
    # in the blockwise below.  We set the last two dimensions
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
        dtype=result_type(a, b),
        concatenate=False,
    )

    # Because contraction + concatenate in blockwise leads to high
    # memory footprints, we want to avoid them. Instead we will perform
    # blockwise (without contraction) followed by reduction. More about
    # this issue: https://github.com/dask/dask/issues/6874

    # We will also perform the reduction without concatenation
    out = _sum_wo_cat(out, axis=-2)

    if a_is_1d:
        out = out.squeeze(-2)
    if b_is_1d:
        out = out.squeeze(-1)

    return out


@derived_from(np)
def outer(a, b):
    a = a.flatten()
    b = b.flatten()

    dtype = np.outer(a.dtype.type(), b.dtype.type()).dtype

    return blockwise(np.outer, "ij", a, "i", b, "j", dtype=dtype)


def _inner_apply_along_axis(arr, func1d, func1d_axis, func1d_args, func1d_kwargs):
    return np.apply_along_axis(func1d, func1d_axis, arr, *func1d_args, **func1d_kwargs)


@derived_from(np)
def apply_along_axis(func1d, axis, arr, *args, dtype=None, shape=None, **kwargs):
    """
    This is a blocked variant of :func:`numpy.apply_along_axis` implemented via
    :func:`dask.array.map_blocks`

    Notes
    -----
    If either of `dtype` or `shape` are not provided, Dask attempts to
    determine them by calling `func1d` on a dummy array. This may produce
    incorrect values for `dtype` or `shape`, so we recommend providing them.
    """
    arr = asarray(arr)

    # Verify that axis is valid and throw an error otherwise
    axis = len(arr.shape[:axis])

    # If necessary, infer dtype and shape of the output of func1d by calling it on test data.
    if shape is None or dtype is None:
        test_data = np.ones((1,), dtype=arr.dtype)
        test_result = np.array(func1d(test_data, *args, **kwargs))
        if shape is None:
            shape = test_result.shape
        if dtype is None:
            dtype = test_result.dtype

    # Rechunk so that func1d is applied over the full axis.
    arr = arr.rechunk(
        arr.chunks[:axis] + (arr.shape[axis : axis + 1],) + arr.chunks[axis + 1 :]
    )

    # Map func1d over the data to get the result
    # Adds other axes as needed.
    result = arr.map_blocks(
        _inner_apply_along_axis,
        prefix=funcname(func1d) + "-along-axis",
        dtype=dtype,
        chunks=(arr.chunks[:axis] + shape + arr.chunks[axis + 1 :]),
        drop_axis=axis,
        new_axis=list(range(axis, axis + len(shape), 1)),
        func1d=func1d,
        func1d_axis=axis,
        func1d_args=args,
        func1d_kwargs=kwargs,
    )

    return result


@derived_from(np)
def apply_over_axes(func, a, axes):
    # Validate arguments
    a = asarray(a)
    try:
        axes = tuple(axes)
    except TypeError:
        axes = (axes,)

    sl = a.ndim * (slice(None),)

    # Compute using `apply_along_axis`.
    result = a
    for i in axes:
        result = apply_along_axis(func, i, result, 0)

        # Restore original dimensionality or error.
        if result.ndim == (a.ndim - 1):
            result = result[sl[:i] + (None,)]
        elif result.ndim != a.ndim:
            raise ValueError(
                "func must either preserve dimensionality of the input"
                " or reduce it by one."
            )

    return result


@derived_from(np)
def ptp(a, axis=None):
    return a.max(axis=axis) - a.min(axis=axis)


@derived_from(np)
def diff(a, n=1, axis=-1, prepend=None, append=None):
    a = asarray(a)
    n = int(n)
    axis = int(axis)

    if n == 0:
        return a
    if n < 0:
        raise ValueError("order must be non-negative but got %d" % n)

    combined = []
    if prepend is not None:
        prepend = asarray_safe(prepend, like=meta_from_array(a))
        if prepend.ndim == 0:
            shape = list(a.shape)
            shape[axis] = 1
            prepend = broadcast_to(prepend, tuple(shape))
        combined.append(prepend)

    combined.append(a)

    if append is not None:
        append = asarray_safe(append, like=meta_from_array(a))
        if append.ndim == 0:
            shape = list(a.shape)
            shape[axis] = 1
            append = np.broadcast_to(append, tuple(shape))
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


@derived_from(np)
def ediff1d(ary, to_end=None, to_begin=None):
    ary = asarray(ary)

    aryf = ary.flatten()
    r = aryf[1:] - aryf[:-1]

    r = [r]
    if to_begin is not None:
        r = [asarray(to_begin).flatten()] + r
    if to_end is not None:
        r = r + [asarray(to_end).flatten()]
    r = concatenate(r)

    return r


def _gradient_kernel(x, block_id, coord, axis, array_locs, grad_kwargs):
    """
    x: nd-array
        array of one block
    coord: 1d-array or scalar
        coordinate along which the gradient is computed.
    axis: int
        axis along which the gradient is computed
    array_locs:
        actual location along axis. None if coordinate is scalar
    grad_kwargs:
        keyword to be passed to np.gradient
    """
    block_loc = block_id[axis]
    if array_locs is not None:
        coord = coord[array_locs[0][block_loc] : array_locs[1][block_loc]]
    grad = np.gradient(x, coord, axis=axis, **grad_kwargs)
    return grad


@derived_from(np)
def gradient(f, *varargs, axis=None, **kwargs):
    # TODO(expr-soon): Needs map_overlap
    raise NotImplementedError


def _bincount_agg(bincounts, dtype, **kwargs):
    if not isinstance(bincounts, list):
        return bincounts

    n = max(map(len, bincounts))
    out = np.zeros_like(bincounts[0], shape=n, dtype=dtype)
    for b in bincounts:
        out[: len(b)] += b
    return out


@derived_from(np)
def bincount(x, weights=None, minlength=0, split_every=None):
    # TODO(expr-soon): Does this weird chunking overwriting
    raise NotImplementedError


# TODO(expr-soon)
@derived_from(np)
def digitize(a, bins, right=False):
    bins = asarray_safe(bins, like=meta_from_array(a))
    dtype = np.digitize(asarray_safe([0], like=bins), bins, right=False).dtype
    return a.map_blocks(np.digitize, dtype=dtype, bins=bins, right=right)


def _searchsorted_block(x, y, side):
    res = np.searchsorted(x, y, side=side)
    # 0 is only correct for the first block of a, but blockwise doesn't have a way
    # of telling which block is being operated on (unlike map_blocks),
    # so set all 0 values to a special value and set back at the end of searchsorted
    res[res == 0] = -1
    return res[np.newaxis, :]


@derived_from(np)
def searchsorted(a, v, side="left", sorter=None):
    if a.ndim != 1:
        raise ValueError("Input array a must be one dimensional")

    if sorter is not None:
        raise NotImplementedError(
            "da.searchsorted with a sorter argument is not supported"
        )

    # call np.searchsorted for each pair of blocks in a and v
    meta = np.searchsorted(a._meta, v._meta)
    out = blockwise(
        _searchsorted_block,
        list(range(v.ndim + 1)),
        a,
        [0],
        v,
        list(range(1, v.ndim + 1)),
        side,
        None,
        meta=meta,
        adjust_chunks={0: 1},  # one row for each block in a
    )

    # add offsets to take account of the position of each block within the array a
    a_chunk_sizes = array_safe((0, *a.chunks[0]), like=meta_from_array(a))
    a_chunk_offsets = np.cumsum(a_chunk_sizes)[:-1]
    a_chunk_offsets = a_chunk_offsets[(Ellipsis,) + v.ndim * (np.newaxis,)]
    a_offsets = asarray(a_chunk_offsets, chunks=1)
    out = where(out < 0, out, out + a_offsets)

    # combine the results from each block (of a)
    out = out.max(axis=0)

    # fix up any -1 values
    out[out == -1] = 0

    return out


# TODO: dask linspace doesn't support delayed values
def _linspace_from_delayed(start, stop, num=50):
    (start_ref, stop_ref, num_ref), _ = unpack_collections([start, stop, num])
    return new_collection(Linspace(start, stop, num, start_ref, stop_ref, num_ref))


class Linspace(ArrayExpr):
    _parameters = ["start", "stop", "num", "start_ref", "stop_ref", "num_ref"]

    @functools.cached_property
    def _meta(self):
        return meta_from_array(None, dtype=float)

    @functools.cached_property
    def chunks(self):
        num = self.operand("num")
        return ((np.nan,),) if isinstance(num, ArrayExpr) else ((num,),)

    def _layer(self) -> dict:
        return {
            (self.name, 0): (np.linspace, self.start_ref, self.stop_ref, self.num_ref)
        }


def _block_hist(x, bins, range=None, weights=None):
    return np.histogram(x, bins, range=range, weights=weights)[0][np.newaxis]


def histogram(a, bins=None, range=None, normed=False, weights=None, density=None):
    """
    Blocked variant of :func:`numpy.histogram`.

    Parameters
    ----------
    a : dask.array.Array
        Input data; the histogram is computed over the flattened
        array. If the ``weights`` argument is used, the chunks of
        ``a`` are accessed to check chunking compatibility between
        ``a`` and ``weights``. If ``weights`` is ``None``, a
        :py:class:`dask.dataframe.Series` object can be passed as
        input data.
    bins : int or sequence of scalars, optional
        Either an iterable specifying the ``bins`` or the number of ``bins``
        and a ``range`` argument is required as computing ``min`` and ``max``
        over blocked arrays is an expensive operation that must be performed
        explicitly.
        If `bins` is an int, it defines the number of equal-width
        bins in the given range (10, by default). If `bins` is a
        sequence, it defines a monotonically increasing array of bin edges,
        including the rightmost edge, allowing for non-uniform bin widths.
    range : (float, float), optional
        The lower and upper range of the bins.  If not provided, range
        is simply ``(a.min(), a.max())``.  Values outside the range are
        ignored. The first element of the range must be less than or
        equal to the second. `range` affects the automatic bin
        computation as well. While bin width is computed to be optimal
        based on the actual data within `range`, the bin count will fill
        the entire range including portions containing no data.
    normed : bool, optional
        This is equivalent to the ``density`` argument, but produces incorrect
        results for unequal bin widths. It should not be used.
    weights : dask.array.Array, optional
        A dask.array.Array of weights, of the same block structure as ``a``.  Each value in
        ``a`` only contributes its associated weight towards the bin count
        (instead of 1). If ``density`` is True, the weights are
        normalized, so that the integral of the density over the range
        remains 1.
    density : bool, optional
        If ``False``, the result will contain the number of samples in
        each bin. If ``True``, the result is the value of the
        probability *density* function at the bin, normalized such that
        the *integral* over the range is 1. Note that the sum of the
        histogram values will not be equal to 1 unless bins of unity
        width are chosen; it is not a probability *mass* function.
        Overrides the ``normed`` keyword if given.
        If ``density`` is True, ``bins`` cannot be a single-number delayed
        value. It must be a concrete number, or a (possibly-delayed)
        array/sequence of the bin edges.

    Returns
    -------
    hist : dask Array
        The values of the histogram. See `density` and `weights` for a
        description of the possible semantics.
    bin_edges : dask Array of dtype float
        Return the bin edges ``(length(hist)+1)``.

    Examples
    --------
    Using number of bins and range:

    >>> import dask.array as da
    >>> import numpy as np
    >>> x = da.from_array(np.arange(10000), chunks=10)
    >>> h, bins = da.histogram(x, bins=10, range=[0, 10000])
    >>> bins
    array([    0.,  1000.,  2000.,  3000.,  4000.,  5000.,  6000.,  7000.,
            8000.,  9000., 10000.])
    >>> h.compute()
    array([1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000])

    Explicitly specifying the bins:

    >>> h, bins = da.histogram(x, bins=np.array([0, 5000, 10000]))
    >>> bins
    array([    0,  5000, 10000])
    >>> h.compute()
    array([5000, 5000])
    """
    if isinstance(bins, Array):
        scalar_bins = bins.ndim == 0
        # ^ `np.ndim` is not implemented by Dask array.
    elif isinstance(bins, Delayed):
        scalar_bins = bins._length is None or bins._length == 1
    else:
        scalar_bins = np.ndim(bins) == 0

    if bins is None or (scalar_bins and range is None):
        raise ValueError(
            "dask.array.histogram requires either specifying "
            "bins as an iterable or specifying both a range and "
            "the number of bins"
        )

    if weights is not None and weights.chunks != a.chunks:
        raise ValueError("Input array and weights must have the same chunked structure")

    if normed is not False:
        raise ValueError(
            "The normed= keyword argument has been deprecated. "
            "Please use density instead. "
            "See the numpy.histogram docstring for more information."
        )

    if density and scalar_bins and isinstance(bins, (Array, Delayed)):
        raise NotImplementedError(
            "When `density` is True, `bins` cannot be a scalar Dask object. "
            "It must be a concrete number or a (possibly-delayed) array/sequence of bin edges."
        )

    for argname, val in [("bins", bins), ("range", range), ("weights", weights)]:
        if not isinstance(bins, (Array, Delayed)) and is_dask_collection(bins):
            raise TypeError(
                "Dask types besides Array and Delayed are not supported "
                "for `histogram`. For argument `{}`, got: {!r}".format(argname, val)
            )

    if range is not None:
        try:
            if len(range) != 2:
                raise ValueError(
                    f"range must be a sequence or array of length 2, but got {len(range)} items"
                )
            if isinstance(range, (Array, np.ndarray)) and range.shape != (2,):
                raise ValueError(
                    f"range must be a 1-dimensional array of two items, but got an array of shape {range.shape}"
                )
        except TypeError:
            raise TypeError(
                f"Expected a sequence or array for range, not {range}"
            ) from None

    if scalar_bins:
        bins = _linspace_from_delayed(range[0], range[1], bins + 1)
        # ^ NOTE `range[1]` is safe because of the above check, and the initial check
        # that range must not be None if `scalar_bins`
    else:
        if not isinstance(bins, (Array, np.ndarray)):
            bins = asarray(bins)
        if bins.ndim != 1:
            raise ValueError(
                f"bins must be a 1-dimensional array or sequence, got shape {bins.shape}"
            )
    (bins_ref, range_ref), _ = unpack_collections([bins, range])
    mapped = new_collection(Histogram(a, weights, bins, bins_ref, range_ref))

    # Sum over chunks to get the final histogram
    n = mapped.sum(axis=0)

    # We need to replicate normed and density options from numpy
    if density is not None:
        if density:
            db = asarray(np.diff(bins).astype(float), chunks=n.chunks)
            return n / db / n.sum(), bins
        else:
            return n, bins
    else:
        return n, bins


class Histogram(ArrayExpr):
    _parameters = ["array", "weights", "bins", "bins_ref", "range_ref"]

    @functools.cached_property
    def _meta(self):
        if self.weights is None:
            dtype = np.histogram([])[0].dtype
        else:
            dtype = self.weights.dtype
        return meta_from_array(None, ndim=self.ndim, dtype=dtype)

    @functools.cached_property
    def chunks(self):
        nchunks = len(list(flatten(self.array.__dask_keys__())))
        nbins = self.bins.size - 1  # since `bins` is 1D
        return ((1,) * nchunks, (nbins,))

    def _layer(self) -> dict:
        weights, bins_ref, range_ref = self.weights, self.bins_ref, self.range_ref
        a = self.array
        dsk: dict
        if weights is None:
            dsk = {
                (self._name, i, 0): (_block_hist, k, bins_ref, range_ref)
                for i, k in enumerate(flatten(a.__dask_keys__()))
            }
        else:
            a_keys = flatten(a.__dask_keys__())
            w_keys = flatten(weights.__dask_keys__())
            dsk = {
                (self._name, i, 0): (_block_hist, k, bins_ref, range_ref, w)
                for i, (k, w) in enumerate(zip(a_keys, w_keys))
            }
        return dsk


def histogram2d(x, y, bins=10, range=None, normed=None, weights=None, density=None):
    """Blocked variant of :func:`numpy.histogram2d`.

    Parameters
    ----------
    x : dask.array.Array
        An array containing the `x`-coordinates of the points to be
        histogrammed.
    y : dask.array.Array
        An array containing the `y`-coordinates of the points to be
        histogrammed.
    bins : sequence of arrays describing bin edges, int, or sequence of ints
        The bin specification. See the `bins` argument description for
        :py:func:`histogramdd` for a complete description of all
        possible bin configurations (this function is a 2D specific
        version of histogramdd).
    range : tuple of pairs, optional.
        The leftmost and rightmost edges of the bins along each
        dimension when integers are passed to `bins`; of the form:
        ((xmin, xmax), (ymin, ymax)).
    normed : bool, optional
        An alias for the density argument that behaves identically. To
        avoid confusion with the broken argument in the `histogram`
        function, `density` should be preferred.
    weights : dask.array.Array, optional
        An array of values weighing each sample in the input data. The
        chunks of the weights must be identical to the chunking along
        the 0th (row) axis of the data sample.
    density : bool, optional
        If False (the default) return the number of samples in each
        bin. If True, the returned array represents the probability
        density function at each bin.

    Returns
    -------
    dask.array.Array
        The values of the histogram.
    dask.array.Array
        The edges along the `x`-dimension.
    dask.array.Array
        The edges along the `y`-dimension.

    See Also
    --------
    histogram
    histogramdd

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.array([2, 4, 2, 4, 2, 4])
    >>> y = da.array([2, 2, 4, 4, 2, 4])
    >>> bins = 2
    >>> range = ((0, 6), (0, 6))
    >>> h, xedges, yedges = da.histogram2d(x, y, bins=bins, range=range)
    >>> h
    dask.array<sum-aggregate, shape=(2, 2), dtype=float64, chunksize=(2, 2), chunktype=numpy.ndarray>
    >>> xedges
    dask.array<array, shape=(3,), dtype=float64, chunksize=(3,), chunktype=numpy.ndarray>
    >>> h.compute()
    array([[2., 1.],
           [1., 2.]])
    """
    raise NotImplementedError("not done yet")


@derived_from(np)
def cov(m, y=None, rowvar=1, bias=0, ddof=None):
    # This was copied almost verbatim from np.cov
    # See numpy license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    # or NUMPY_LICENSE.txt within this directory
    if ddof is not None and ddof != int(ddof):
        raise ValueError("ddof must be integer")

    # Handles complex arrays too
    m = asarray(m)
    if y is None:
        dtype = np.result_type(m, np.float64)
    else:
        y = asarray(y)
        dtype = np.result_type(m, y, np.float64)
    X = array(m, ndmin=2, dtype=dtype)

    if X.shape[0] == 1:
        rowvar = 1
    if rowvar:
        N = X.shape[1]
        axis = 0
    else:
        N = X.shape[0]
        axis = 1

    # check ddof
    if ddof is None:
        if bias == 0:
            ddof = 1
        else:
            ddof = 0
    fact = float(N - ddof)
    if fact <= 0:
        warnings.warn("Degrees of freedom <= 0 for slice", RuntimeWarning)
        fact = 0.0

    if y is not None:
        y = array(y, ndmin=2, dtype=dtype)
        X = concatenate((X, y), axis)

    X = X - X.mean(axis=1 - axis, keepdims=True)
    if not rowvar:
        return (dot(X.T, X.conj()) / fact).squeeze()
    else:
        return (dot(X, X.T.conj()) / fact).squeeze()


@derived_from(np)
def corrcoef(x, y=None, rowvar=1):
    c = cov(x, y, rowvar)
    if c.shape == ():
        return c / c
    d = diag(c)
    d = d.reshape((d.shape[0], 1))
    sqr_d = sqrt(d)
    return (c / sqr_d) / sqr_d.T


@implements(np.round)
@derived_from(np)
def round(a, decimals=0):
    return a.map_blocks(np.round, decimals=decimals, dtype=a.dtype)


@implements(np.ndim)
@derived_from(np)
def ndim(a):
    return a.ndim


@implements(np.iscomplexobj)
@derived_from(np)
def iscomplexobj(x):
    return issubclass(x.dtype.type, np.complexfloating)


def _unique_internal(ar, indices, counts, return_inverse=False):
    """
    Helper/wrapper function for :func:`numpy.unique`.

    Uses :func:`numpy.unique` to find the unique values for the array chunk.
    Given this chunk may not represent the whole array, also take the
    ``indices`` and ``counts`` that are in 1-to-1 correspondence to ``ar``
    and reduce them in the same fashion as ``ar`` is reduced. Namely sum
    any counts that correspond to the same value and take the smallest
    index that corresponds to the same value.

    To handle the inverse mapping from the unique values to the original
    array, simply return a NumPy array created with ``arange`` with enough
    values to correspond 1-to-1 to the unique values. While there is more
    work needed to be done to create the full inverse mapping for the
    original array, this provides enough information to generate the
    inverse mapping in Dask.

    Given Dask likes to have one array returned from functions like
    ``blockwise``, some formatting is done to stuff all of the resulting arrays
    into one big NumPy structured array. Dask is then able to handle this
    object and can split it apart into the separate results on the Dask side,
    which then can be passed back to this function in concatenated chunks for
    further reduction or can be return to the user to perform other forms of
    analysis.

    By handling the problem in this way, it does not matter where a chunk
    is in a larger array or how big it is. The chunk can still be computed
    on the same way. Also it does not matter if the chunk is the result of
    other chunks being run through this function multiple times. The end
    result will still be just as accurate using this strategy.
    """

    return_index = indices is not None
    return_counts = counts is not None

    u = np.unique(ar)

    dt = [("values", u.dtype)]
    if return_index:
        dt.append(("indices", np.intp))
    if return_inverse:
        dt.append(("inverse", np.intp))
    if return_counts:
        dt.append(("counts", np.intp))

    r = np.empty(u.shape, dtype=dt)
    r["values"] = u
    if return_inverse:
        r["inverse"] = np.arange(len(r), dtype=np.intp)
    if return_index or return_counts:
        for i, v in enumerate(r["values"]):
            m = ar == v
            if return_index:
                indices[m].min(keepdims=True, out=r["indices"][i : i + 1])
            if return_counts:
                counts[m].sum(keepdims=True, out=r["counts"][i : i + 1])

    return r


def unique_no_structured_arr(
    ar, return_index=False, return_inverse=False, return_counts=False
):
    # A simplified version of `unique`, that allows computing unique for array
    # types that don't support structured arrays (such as cupy.ndarray), but
    # can only compute values at the moment.

    if (
        return_index is not False
        or return_inverse is not False
        or return_counts is not False
    ):
        raise ValueError(
            "dask.array.unique does not support `return_index`, `return_inverse` "
            "or `return_counts` with array types that don't support structured "
            "arrays."
        )

    ar = ar.ravel()

    args = [ar, "i"]
    meta = meta_from_array(ar)

    out = blockwise(np.unique, "i", *args, meta=meta, _nan_chunks=True)
    return new_collection(UniqueAggregate(out))


class UniqueAggregate(ArrayExpr):
    _parameters = ["array", "return_inverse", "dtype", "func"]
    _defaults = {"return_inverse": None, "dtype": None, "func": np.unique}

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array._meta, dtype=self.operand("dtype"))

    @functools.cached_property
    def chunks(self):
        return ((np.nan,),)

    @functools.cached_property
    def out_parts(self):
        return [self.operands[0]] + self.operands[4:]

    def _layer(self) -> dict:
        dsk = {
            (self._name, 0): (
                (self.func,)
                + tuple(
                    (
                        (np.concatenate, o.__dask_keys__())
                        if hasattr(o, "__dask_keys__")
                        else o
                    )
                    for o in self.out_parts
                )
                + ((self.return_inverse,) if self.return_inverse is not None else ())
            )
        }
        return dsk


@derived_from(np)
def unique(ar, return_index=False, return_inverse=False, return_counts=False):
    # Test whether the downstream library supports structured arrays. If the
    # `np.empty_like` call raises a `TypeError`, the downstream library (e.g.,
    # CuPy) doesn't support it. In that case we return the
    # `unique_no_structured_arr` implementation, otherwise (e.g., NumPy) just
    # continue as normal.
    try:
        meta = meta_from_array(ar)
        np.empty_like(meta, dtype=[("a", int), ("b", float)])
    except TypeError:
        return unique_no_structured_arr(
            ar,
            return_index=return_index,
            return_inverse=return_inverse,
            return_counts=return_counts,
        )

    orig_shape = ar.shape
    ar = ar.ravel()

    # Run unique on each chunk and collect results in a Dask Array of
    # unknown size.

    args = [ar, "i"]
    out_dtype = [("values", ar.dtype)]
    if return_index:
        args.extend([arange(ar.shape[0], dtype=np.intp, chunks=ar.chunks[0]), "i"])
        out_dtype.append(("indices", np.intp))
    else:
        args.extend([None, None])
    if return_counts:
        args.extend([ones((ar.shape[0],), dtype=np.intp, chunks=ar.chunks[0]), "i"])
        out_dtype.append(("counts", np.intp))
    else:
        args.extend([None, None])

    out = blockwise(
        _unique_internal,
        "i",
        *args,
        dtype=out_dtype,
        return_inverse=False,
        _nan_chunks=True,
    )

    # Take the results from the unique chunks and do the following.
    #
    # 1. Collect all results as arguments.
    # 2. Concatenate each result into one big array.
    # 3. Pass all results as arguments to the internal unique again.
    #
    # TODO: This should be replaced with a tree reduction using this strategy.
    # xref: https://github.com/dask/dask/issues/2851

    out_parts = [out["values"]]
    if return_index:
        out_parts.append(out["indices"])
    else:
        out_parts.append(None)
    if return_counts:
        out_parts.append(out["counts"])
    else:
        out_parts.append(None)

    out_dtype = [("values", ar.dtype)]
    if return_index:
        out_dtype.append(("indices", np.intp))
    if return_inverse:
        out_dtype.append(("inverse", np.intp))
    if return_counts:
        out_dtype.append(("counts", np.intp))

    out = new_collection(
        UniqueAggregate(
            out_parts[0], return_inverse, out_dtype, _unique_internal, *out_parts[1:]
        )
    )

    # Split out all results to return to the user.

    result = [out["values"]]
    if return_index:
        result.append(out["indices"])
    if return_inverse:
        # Using the returned unique values and arange of unknown length, find
        # each value matching a unique value and replace it with its
        # corresponding index or `0`. There should be only one entry for this
        # index in axis `1` (the one of unknown length). Reduce axis `1`
        # through summing to get an array with known dimensionality and the
        # mapping of the original values.
        matches = (ar[:, None] == out["values"][None, :]).astype(np.intp)
        inverse = (matches * out["inverse"]).sum(axis=1)
        if NUMPY_GE_200:
            inverse = inverse.reshape(orig_shape)
        result.append(inverse)
    if return_counts:
        result.append(out["counts"])

    if len(result) == 1:
        result = result[0]
    else:
        result = tuple(result)

    return result


def _isin_kernel(element, test_elements, assume_unique=False):
    values = np.isin(element.ravel(), test_elements, assume_unique=assume_unique)
    return values.reshape(element.shape + (1,) * test_elements.ndim)


@safe_wraps(getattr(np, "isin", None))
def isin(element, test_elements, assume_unique=False, invert=False):
    element = asarray(element)
    test_elements = asarray(test_elements)
    element_axes = tuple(range(element.ndim))
    test_axes = tuple(i + element.ndim for i in range(test_elements.ndim))
    mapped = blockwise(
        _isin_kernel,
        element_axes + test_axes,
        element,
        element_axes,
        test_elements,
        test_axes,
        adjust_chunks={axis: lambda _: 1 for axis in test_axes},
        dtype=bool,
        assume_unique=assume_unique,
    )

    result = mapped.any(axis=test_axes)
    if invert:
        result = ~result
    return result


@derived_from(np)
def roll(array, shift, axis=None):
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


@derived_from(np)
def shape(array):
    return array.shape


@derived_from(np)
def union1d(ar1, ar2):
    return unique(concatenate((ar1.ravel(), ar2.ravel())))


@derived_from(np)
def expand_dims(a, axis):
    if type(axis) not in (tuple, list):
        axis = (axis,)

    out_ndim = len(axis) + a.ndim
    axis = validate_axis(axis, out_ndim)

    shape_it = iter(a.shape)
    shape = [1 if ax in axis else next(shape_it) for ax in range(out_ndim)]

    return a.reshape(shape)


@derived_from(np)
def squeeze(a, axis=None):
    if axis is None:
        axis = tuple(i for i, d in enumerate(a.shape) if d == 1)
    elif not isinstance(axis, tuple):
        axis = (axis,)

    if any(a.shape[i] != 1 for i in axis):
        raise ValueError("cannot squeeze axis with size other than one")

    axis = validate_axis(axis, a.ndim)

    sl = tuple(0 if i in axis else slice(None) for i, s in enumerate(a.shape))

    # Return 0d Dask Array if all axes are squeezed,
    # to be consistent with NumPy. Ref: https://github.com/dask/dask/issues/9183#issuecomment-1155626619
    if all(s == 0 for s in sl) and all(s == 1 for s in a.shape):
        return a.map_blocks(
            np.squeeze, meta=a._meta, drop_axis=tuple(range(len(a.shape)))
        )

    a = a[sl]

    return a


@derived_from(np)
def extract(condition, arr):
    condition = asarray(condition).astype(bool)
    arr = asarray(arr)
    return compress(condition.ravel(), arr.ravel())


@derived_from(np)
def take(a, indices, axis=0):
    axis = validate_axis(axis, a.ndim)

    if isinstance(a, np.ndarray) and isinstance(indices, Array):
        return _take_dask_array_from_numpy(a, indices, axis)
    else:
        return a[(slice(None),) * axis + (indices,)]


def _take_dask_array_from_numpy(a, indices, axis):
    assert isinstance(a, np.ndarray)
    assert isinstance(indices, Array)

    return indices.map_blocks(
        lambda block: np.take(a, block, axis), chunks=indices.chunks, dtype=a.dtype
    )


@derived_from(np)
def around(x, decimals=0):
    return map_blocks(partial(np.around, decimals=decimals), x, dtype=x.dtype)


def _asarray_isnull(values):
    import pandas as pd

    return np.asarray(pd.isnull(values))


def isnull(values):
    """pandas.isnull for dask arrays"""
    # eagerly raise ImportError, if pandas isn't available
    import pandas as pd  # noqa

    return elemwise(_asarray_isnull, values, dtype="bool")


def notnull(values):
    """pandas.notnull for dask arrays"""
    return ~isnull(values)


@derived_from(np)
def isclose(arr1, arr2, rtol=1e-5, atol=1e-8, equal_nan=False):
    func = partial(np.isclose, rtol=rtol, atol=atol, equal_nan=equal_nan)
    return elemwise(func, arr1, arr2, dtype="bool")


@derived_from(np)
def allclose(arr1, arr2, rtol=1e-5, atol=1e-8, equal_nan=False):
    return isclose(arr1, arr2, rtol=rtol, atol=atol, equal_nan=equal_nan).all()


def variadic_choose(a, *choices):
    return np.choose(a, choices)


@derived_from(np)
def choose(a, choices):
    return elemwise(variadic_choose, a, *choices)


@derived_from(np)
def where(condition, x=None, y=None):
    if (x is None) != (y is None):
        raise ValueError("either both or neither of x and y should be given")
    if (x is None) and (y is None):
        return nonzero(condition)

    if np.isscalar(condition):
        dtype = result_type(x, y)
        x = asarray(x)
        y = asarray(y)

        shape = broadcast_shapes(x.shape, y.shape)
        out = x if condition else y

        return broadcast_to(out, shape).astype(dtype)
    else:
        return elemwise(np.where, condition, x, y)


@derived_from(np)
def count_nonzero(a, axis=None):
    return isnonzero(asarray(a)).astype(np.intp).sum(axis=axis)


@derived_from(np)
def flatnonzero(a):
    return argwhere(asarray(a).ravel())[:, 0]


@derived_from(np)
def nonzero(a):
    ind = argwhere(a)
    if ind.ndim > 1:
        return tuple(ind[:, i] for i in range(ind.shape[1]))
    else:
        return (ind,)


def _unravel_index_kernel(indices, func_kwargs):
    return np.stack(np.unravel_index(indices, **func_kwargs))


@derived_from(np)
def unravel_index(indices, shape, order="C"):
    if shape and indices.size:
        unraveled_indices = tuple(
            indices.map_blocks(
                _unravel_index_kernel,
                dtype=np.intp,
                chunks=(((len(shape),),) + indices.chunks),
                new_axis=0,
                func_kwargs={"shape": shape, "order": order},
            )
        )
    else:
        unraveled_indices = tuple(empty((0,), dtype=np.intp, chunks=1) for i in shape)

    return unraveled_indices


@wraps(np.ravel_multi_index)
def ravel_multi_index(multi_index, dims, mode="raise", order="C"):
    if np.isscalar(dims):
        dims = (dims,)
    if is_dask_collection(dims) or any(is_dask_collection(d) for d in dims):
        raise NotImplementedError(
            f"Dask types are not supported in the `dims` argument: {dims!r}"
        )

    if is_arraylike(multi_index):
        index_stack = asarray(multi_index)
    else:
        multi_index_arrs = broadcast_arrays(*multi_index)
        index_stack = stack(multi_index_arrs)

    if not np.isnan(index_stack.shape).any() and len(index_stack) != len(dims):
        raise ValueError(
            f"parameter multi_index must be a sequence of length {len(dims)}"
        )
    if not np.issubdtype(index_stack.dtype, np.signedinteger):
        raise TypeError("only int indices permitted")
    return index_stack.map_blocks(
        np.ravel_multi_index,
        dtype=np.intp,
        chunks=index_stack.chunks[1:],
        drop_axis=0,
        dims=dims,
        mode=mode,
        order=order,
    )


def _int_piecewise(x, *condlist, **kwargs):
    return np.piecewise(
        x, list(condlist), kwargs["funclist"], *kwargs["func_args"], **kwargs["func_kw"]
    )


@derived_from(np)
def piecewise(x, condlist, funclist, *args, **kw):
    return map_blocks(
        _int_piecewise,
        x,
        *condlist,
        dtype=x.dtype,
        prefix="piecewise",
        funclist=funclist,
        func_args=args,
        func_kw=kw,
    )


def _select(*args, **kwargs):
    """
    This is a version of :func:`numpy.select` that accepts an arbitrary number of arguments and
    splits them in half to create ``condlist`` and ``choicelist`` params.
    """
    split_at = len(args) // 2
    condlist = args[:split_at]
    choicelist = args[split_at:]
    return np.select(condlist, choicelist, **kwargs)


@derived_from(np)
def select(condlist, choicelist, default=0):
    # Making the same checks that np.select
    # Check the size of condlist and choicelist are the same, or abort.
    if len(condlist) != len(choicelist):
        raise ValueError("list of cases must be same length as list of conditions")

    if len(condlist) == 0:
        raise ValueError("select with an empty condition list is not possible")

    choicelist = [asarray(choice) for choice in choicelist]

    try:
        intermediate_dtype = result_type(*choicelist)
    except TypeError as e:
        msg = "Choicelist elements do not have a common dtype."
        raise TypeError(msg) from e

    blockwise_shape = tuple(range(choicelist[0].ndim))

    condargs = [arg for elem in condlist for arg in (elem, blockwise_shape)]
    choiceargs = [arg for elem in choicelist for arg in (elem, blockwise_shape)]

    return blockwise(
        _select,
        blockwise_shape,
        *condargs,
        *choiceargs,
        dtype=intermediate_dtype,
        prefix="select",
        default=default,
    )


def _partition(total: int, divisor: int) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Given a total and a divisor, return two tuples: A tuple containing `divisor`
    repeated the number of times it divides `total`, and length-1 or empty tuple
    containing the remainder when `total` is divided by `divisor`. If `divisor` factors
    `total`, i.e. if the remainder is 0, then `remainder` is empty.
    """
    multiples = (divisor,) * (total // divisor)
    remainder = (total % divisor,) if total % divisor else ()
    return multiples, remainder


def aligned_coarsen_chunks(chunks: list[int], multiple: int) -> tuple[int, ...]:
    """
    Returns a new chunking aligned with the coarsening multiple.
    Any excess is at the end of the array.

    Examples
    --------
    >>> aligned_coarsen_chunks(chunks=(1, 2, 3), multiple=4)
    (np.int64(4), np.int64(2))
    >>> aligned_coarsen_chunks(chunks=(1, 20, 3, 4), multiple=4)
    (np.int64(4), np.int64(20), np.int64(4))
    >>> aligned_coarsen_chunks(chunks=(20, 10, 15, 23, 24), multiple=10)
    (np.int64(20), np.int64(10), np.int64(20), np.int64(20), np.int64(20), np.int64(2))
    """
    overflow = np.array(chunks) % multiple
    excess = overflow.sum()
    new_chunks = np.array(chunks) - overflow
    # valid chunks are those that are already factorizable by `multiple`
    chunk_validity = new_chunks == chunks
    valid_inds, invalid_inds = np.where(chunk_validity)[0], np.where(~chunk_validity)[0]
    # sort the invalid chunks by size (ascending), then concatenate the results of
    # sorting the valid chunks by size (ascending)
    chunk_modification_order = [
        *invalid_inds[np.argsort(new_chunks[invalid_inds])],
        *valid_inds[np.argsort(new_chunks[valid_inds])],
    ]
    partitioned_excess, remainder = _partition(excess, multiple)
    # add elements the partitioned excess to the smallest invalid chunks,
    # then smallest valid chunks if needed.
    for idx, extra in enumerate(partitioned_excess):
        new_chunks[chunk_modification_order[idx]] += extra
    # create excess chunk with remainder, if any remainder exists
    new_chunks = np.array([*new_chunks, *remainder])
    # remove 0-sized chunks
    new_chunks = new_chunks[new_chunks > 0]
    return tuple(new_chunks)


@wraps(chunk.coarsen)
def coarsen(reduction, x, axes, trim_excess=False, **kwargs):
    if not trim_excess and not all(x.shape[i] % div == 0 for i, div in axes.items()):
        msg = f"Coarsening factors {axes} do not align with array shape {x.shape}."
        raise ValueError(msg)

    if reduction.__module__.startswith("dask."):
        reduction = getattr(np, reduction.__name__)

    new_chunks = {}
    for i, div in axes.items():
        aligned = aligned_coarsen_chunks(x.chunks[i], div)
        if aligned != x.chunks[i]:
            new_chunks[i] = aligned
    if new_chunks:
        x = x.rechunk(new_chunks)

    return new_collection(Coarsen(x, reduction, axes, trim_excess, kwargs))


class Coarsen(ArrayExpr):
    _parameters = ["array", "reduction", "axes", "trim_excess", "kwargs"]
    _defaults = {"trim_excess": False}

    @functools.cached_property
    def _meta(self):
        return self.reduction(
            np.empty((1,) * self.array.ndim, dtype=self.array.dtype), **self.kwargs
        )

    @functools.cached_property
    def chunks(self):
        coarsen_dim = lambda dim, ax: int(dim // self.axes.get(ax, 1))
        return tuple(
            tuple(coarsen_dim(bd, i) for bd in bds if coarsen_dim(bd, i) > 0)
            for i, bds in enumerate(self.array.chunks)
        )

    def _layer(self) -> dict:
        dsk = {
            (self._name,)
            + key[1:]: (
                apply,
                chunk.coarsen,
                [self.reduction, key, self.axes, self.trim_excess],
                self.kwargs,
            )
            for key in flatten(self.array.__dask_keys__())
        }
        return dsk


def split_at_breaks(array, breaks, axis=0):
    """Split an array into a list of arrays (using slices) at the given breaks

    >>> split_at_breaks(np.arange(6), [3, 5])
    [array([0, 1, 2]), array([3, 4]), array([5])]
    """
    padded_breaks = concat([[None], breaks, [None]])
    slices = [slice(i, j) for i, j in sliding_window(2, padded_breaks)]
    preslice = (slice(None),) * axis
    split_array = [array[preslice + (s,)] for s in slices]
    return split_array


@derived_from(np)
def insert(arr, obj, values, axis):
    # axis is a required argument here to avoid needing to deal with the numpy
    # default case (which reshapes the array to make it flat)
    axis = validate_axis(axis, arr.ndim)

    if isinstance(obj, slice):
        obj = np.arange(*obj.indices(arr.shape[axis]))
    obj = np.asarray(obj)
    scalar_obj = obj.ndim == 0
    if scalar_obj:
        obj = np.atleast_1d(obj)

    obj = np.where(obj < 0, obj + arr.shape[axis], obj)
    if (np.diff(obj) < 0).any():
        raise NotImplementedError(
            "da.insert only implemented for monotonic ``obj`` argument"
        )

    split_arr = split_at_breaks(arr, np.unique(obj), axis)

    if getattr(values, "ndim", 0) == 0:
        # we need to turn values into a dask array
        values = new_collection(FromArrayDimZero(values))

        values_shape = tuple(
            len(obj) if axis == n else s for n, s in enumerate(arr.shape)
        )
        values = broadcast_to(values, values_shape)
    elif scalar_obj:
        values = values[(slice(None),) * axis + (None,)]

    values_chunks = tuple(
        values_bd if axis == n else arr_bd
        for n, (arr_bd, values_bd) in enumerate(zip(arr.chunks, values.chunks))
    )
    values = values.rechunk(values_chunks)

    counts = np.bincount(obj)[:-1]
    values_breaks = np.cumsum(counts[counts > 0])
    split_values = split_at_breaks(values, values_breaks, axis)

    interleaved = list(interleave([split_arr, split_values]))
    interleaved = [i for i in interleaved if i.nbytes]
    return concatenate(interleaved, axis=axis)


class FromArrayDimZero(ArrayExpr):
    @functools.cached_property
    def _meta(self):
        return meta_from_array(
            self.values, ndim=0, dtype=getattr(self.values, "dtype", type(self.values))
        )

    @functools.cached_property
    def chunks(self):
        return ()

    def _layer(self) -> dict:
        return {(self._name,): self.values}


@derived_from(np)
def delete(arr, obj, axis):
    """
    NOTE: If ``obj`` is a dask array it is implicitly computed when this function
    is called.
    """
    # axis is a required argument here to avoid needing to deal with the numpy
    # default case (which reshapes the array to make it flat)
    axis = validate_axis(axis, arr.ndim)

    if isinstance(obj, slice):
        tmp = np.arange(*obj.indices(arr.shape[axis]))
        obj = tmp[::-1] if obj.step and obj.step < 0 else tmp
    else:
        obj = np.asarray(obj)
        obj = np.where(obj < 0, obj + arr.shape[axis], obj)
        obj = np.unique(obj)

    target_arr = split_at_breaks(arr, obj, axis)

    target_arr = [
        (
            arr[
                tuple(
                    slice(1, None) if axis == n else slice(None)
                    for n in range(arr.ndim)
                )
            ]
            if i != 0
            else arr
        )
        for i, arr in enumerate(target_arr)
    ]
    return concatenate(target_arr, axis=axis)


@derived_from(np)
def append(arr, values, axis=None):
    # based on numpy.append
    arr = asanyarray(arr)
    if axis is None:
        if arr.ndim != 1:
            arr = arr.ravel()
        values = ravel(asanyarray(values))
        axis = arr.ndim - 1
    return concatenate((arr, values), axis=axis)


def _average(
    a, axis=None, weights=None, returned=False, is_masked=False, keepdims=False
):
    # This was minimally modified from numpy.average
    # See numpy license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    # or NUMPY_LICENSE.txt within this directory
    # Wrapper used by da.average or da.ma.average.
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

        # Sanity checks
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

            # setup wgt to broadcast along axis
            wgt = broadcast_to(wgt, (a.ndim - 1) * (1,) + wgt.shape)
            wgt = wgt.swapaxes(-1, axis)
        if is_masked:
            from dask.array.ma import getmaskarray

            wgt = wgt * (~getmaskarray(a))
        scl = wgt.sum(axis=axis, dtype=result_dtype, keepdims=keepdims)
        avg = multiply(a, wgt, dtype=result_dtype).sum(axis, keepdims=keepdims) / scl

    if returned:
        if scl.shape != avg.shape:
            scl = broadcast_to(scl, avg.shape).copy()
        return avg, scl
    else:
        return avg


@derived_from(np)
def average(a, axis=None, weights=None, returned=False, keepdims=False):
    return _average(a, axis, weights, returned, is_masked=False, keepdims=keepdims)


@derived_from(np)
def tril(m, k=0):
    from dask.array.creation import tri

    m = asarray_safe(m, like=m)
    mask = tri(
        *m.shape[-2:],
        k=k,
        dtype=bool,
        chunks=m.chunks[-2:],
        like=meta_from_array(m),
    )

    return where(mask, m, np.zeros_like(m, shape=(1,)))


@derived_from(np)
def triu(m, k=0):
    from dask.array.creation import tri

    m = asarray_safe(m, like=m)
    mask = tri(
        *m.shape[-2:],
        k=k - 1,
        dtype=bool,
        chunks=m.chunks[-2:],
        like=meta_from_array(m),
    )

    return where(mask, np.zeros_like(m, shape=(1,)), m)


@derived_from(np)
def tril_indices(n, k=0, m=None, chunks="auto"):
    from dask.array.creation import tri

    return nonzero(tri(n, m, k=k, dtype=bool, chunks=chunks))


@derived_from(np)
def tril_indices_from(arr, k=0):
    if arr.ndim != 2:
        raise ValueError("input array must be 2-d")
    return tril_indices(arr.shape[-2], k=k, m=arr.shape[-1], chunks=arr.chunks)


@derived_from(np)
def triu_indices(n, k=0, m=None, chunks="auto"):
    from dask.array.creation import tri

    return nonzero(~tri(n, m, k=k - 1, dtype=bool, chunks=chunks))


@derived_from(np)
def triu_indices_from(arr, k=0):
    if arr.ndim != 2:
        raise ValueError("input array must be 2-d")
    return triu_indices(arr.shape[-2], k=k, m=arr.shape[-1], chunks=arr.chunks)

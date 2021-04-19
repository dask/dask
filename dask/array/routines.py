import inspect
import math
import warnings
from collections.abc import Iterable
from functools import partial, wraps
from numbers import Integral, Real
from typing import List, Tuple

import numpy as np
from tlz import concat, interleave, sliding_window

from ..base import is_dask_collection, tokenize
from ..compatibility import apply
from ..core import flatten
from ..delayed import Delayed, unpack_collections
from ..highlevelgraph import HighLevelGraph
from ..utils import derived_from, funcname, is_arraylike
from . import chunk
from .core import (
    Array,
    asanyarray,
    asarray,
    blockwise,
    broadcast_shapes,
    broadcast_to,
    concatenate,
    elemwise,
    implements,
    is_scalar_for_elemwise,
    map_blocks,
    stack,
    tensordot_lookup,
)
from .creation import arange, diag, empty, indices, tri
from .einsumfuncs import einsum  # noqa
from .ufunc import multiply, sqrt
from .utils import (
    array_safe,
    asarray_safe,
    meta_from_array,
    safe_wraps,
    validate_axis,
    zeros_like_safe,
)
from .wrap import ones

# save built-in for histogram functions which use range as a kwarg.
_range = range


@derived_from(np)
def array(x, dtype=None, ndmin=None):
    x = asarray(x)
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
        return new_arys


@derived_from(np)
def vstack(tup, allow_unknown_chunksizes=False):
    tup = tuple(atleast_2d(x) for x in tup)
    return concatenate(tup, axis=0, allow_unknown_chunksizes=allow_unknown_chunksizes)


@derived_from(np)
def hstack(tup, allow_unknown_chunksizes=False):
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


def flip(m, axis):
    """
    Reverse element order along axis.

    Parameters
    ----------
    axis : int
        Axis to reverse element order of.

    Returns
    -------
    reversed array : ndarray
    """

    m = asanyarray(m)

    sl = m.ndim * [slice(None)]
    try:
        sl[axis] = slice(None, None, -1)
    except IndexError as e:
        raise ValueError(
            "`axis` of %s invalid for %s-D array" % (str(axis), str(m.ndim))
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
        raise ValueError(
            "Axes={} out of range for array of ndim={}.".format(axes, m.ndim)
        )

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


alphabet = "abcdefghijklmnopqrstuvwxyz"
ALPHABET = alphabet.upper()


def _tensordot(a, b, axes):
    x = max([a, b], key=lambda x: x.__array_priority__)
    tensordot = tensordot_lookup.dispatch(type(x))
    x = tensordot(a, b, axes=axes)

    if len(axes[0]) != 1:
        ind = [slice(None, None)] * x.ndim
        for a in sorted(axes[0]):
            ind.insert(a, None)
        x = x[tuple(ind)]

    return x


@derived_from(np)
def tensordot(lhs, rhs, axes=2):
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
    if len(left_axes) == 1:
        concatenate = True
    else:
        concatenate = False

    dt = np.promote_types(lhs.dtype, rhs.dtype)

    left_index = list(range(lhs.ndim))
    right_index = list(range(lhs.ndim, lhs.ndim + rhs.ndim))
    out_index = left_index + right_index

    for l, r in zip(left_axes, right_axes):
        out_index.remove(right_index[r])
        right_index[r] = left_index[l]
        if concatenate:
            out_index.remove(left_index[l])

    intermediate = blockwise(
        _tensordot,
        out_index,
        lhs,
        left_index,
        rhs,
        right_index,
        dtype=dt,
        concatenate=concatenate,
        axes=(left_axes, right_axes),
    )

    if concatenate:
        return intermediate
    else:
        return intermediate.sum(axis=left_axes)


@derived_from(np)
def dot(a, b):
    return tensordot(a, b, axes=((a.ndim - 1,), (b.ndim - 2,)))


@derived_from(np)
def vdot(a, b):
    return dot(a.conj().ravel(), b.ravel())


def _matmul(a, b):
    chunk = np.matmul(a, b)
    # Since we have performed the contraction via matmul
    # but blockwise expects all dimensions back, we need
    # to add one dummy dimension back
    return chunk[..., np.newaxis]


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
    # in the blockwise below
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

    # When we perform reduction, we need to worry about the last 2 dimensions
    # which hold the matrices, some care is required to handle chunking in
    # that space.
    contraction_dimension_is_chunked = (
        max(min(a.chunks[-1], b.chunks[-2])) < a.shape[-1]
    )
    b_last_dim_max_chunk = max(b.chunks[-1])
    if contraction_dimension_is_chunked or b_last_dim_max_chunk < b.shape[-1]:
        if b_last_dim_max_chunk > 1:
            # This is the case when both contraction and last dimension axes
            # are chunked
            out = out.reshape(out.shape[:-1] + (1, -1))
            out = out.sum(axis=-3)
            out = out.reshape(out.shape[:-2] + (b.shape[-1],))
        else:
            # Contraction axis is chunked
            out = out.sum(axis=-2)
    else:
        # Neither contraction nor last dimension axes are chunked, we
        # remove the dummy dimension without reduction
        out = out.reshape(out.shape[:-2] + (b.shape[-1],))

    if a_is_1d:
        out = out[..., 0, :]
    if b_is_1d:
        out = out[..., 0]

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
        name=funcname(func1d) + "-along-axis",
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
def diff(a, n=1, axis=-1):
    a = asarray(a)
    n = int(n)
    axis = int(axis)

    sl_1 = a.ndim * [slice(None)]
    sl_2 = a.ndim * [slice(None)]

    sl_1[axis] = slice(1, None)
    sl_2[axis] = slice(None, -1)

    sl_1 = tuple(sl_1)
    sl_2 = tuple(sl_2)

    r = a
    for i in range(n):
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
def gradient(f, *varargs, **kwargs):
    f = asarray(f)

    kwargs["edge_order"] = math.ceil(kwargs.get("edge_order", 1))
    if kwargs["edge_order"] > 2:
        raise ValueError("edge_order must be less than or equal to 2.")

    drop_result_list = False
    axis = kwargs.pop("axis", None)
    if axis is None:
        axis = tuple(range(f.ndim))
    elif isinstance(axis, Integral):
        drop_result_list = True
        axis = (axis,)

    axis = validate_axis(axis, f.ndim)

    if len(axis) != len(set(axis)):
        raise ValueError("duplicate axes not allowed")

    axis = tuple(ax % f.ndim for ax in axis)

    if varargs == ():
        varargs = (1,)
    if len(varargs) == 1:
        varargs = len(axis) * varargs
    if len(varargs) != len(axis):
        raise TypeError(
            "Spacing must either be a single scalar, or a scalar / 1d-array per axis"
        )

    if issubclass(f.dtype.type, (np.bool8, Integral)):
        f = f.astype(float)
    elif issubclass(f.dtype.type, Real) and f.dtype.itemsize < 4:
        f = f.astype(float)

    results = []
    for i, ax in enumerate(axis):
        for c in f.chunks[ax]:
            if np.min(c) < kwargs["edge_order"] + 1:
                raise ValueError(
                    "Chunk size must be larger than edge_order + 1. "
                    "Minimum chunk for axis {} is {}. Rechunk to "
                    "proceed.".format(ax, np.min(c))
                )

        if np.isscalar(varargs[i]):
            array_locs = None
        else:
            if isinstance(varargs[i], Array):
                raise NotImplementedError("dask array coordinated is not supported.")
            # coordinate position for each block taking overlap into account
            chunk = np.array(f.chunks[ax])
            array_loc_stop = np.cumsum(chunk) + 1
            array_loc_start = array_loc_stop - chunk - 2
            array_loc_stop[-1] -= 1
            array_loc_start[0] = 0
            array_locs = (array_loc_start, array_loc_stop)

        results.append(
            f.map_overlap(
                _gradient_kernel,
                dtype=f.dtype,
                depth={j: 1 if j == ax else 0 for j in range(f.ndim)},
                boundary="none",
                coord=varargs[i],
                axis=ax,
                array_locs=array_locs,
                grad_kwargs=kwargs,
            )
        )

    if drop_result_list:
        results = results[0]

    return results


def _bincount_agg(bincounts, dtype, **kwargs):
    if not isinstance(bincounts, list):
        return bincounts

    n = max(map(len, bincounts))
    out = zeros_like_safe(bincounts[0], shape=n, dtype=dtype)
    for b in bincounts:
        out[: len(b)] += b
    return out


@derived_from(np)
def bincount(x, weights=None, minlength=0, split_every=None):
    if x.ndim != 1:
        raise ValueError("Input array must be one dimensional. Try using x.ravel()")
    if weights is not None:
        if weights.chunks != x.chunks:
            raise ValueError("Chunks of input array x and weights must match.")

    token = tokenize(x, weights, minlength)
    args = [x, "i"]
    if weights is not None:
        meta = array_safe(np.bincount([1], weights=[1]), like=meta_from_array(x))
        args.extend([weights, "i"])
    else:
        meta = array_safe(np.bincount([]), like=meta_from_array(x))

    if minlength == 0:
        output_size = (np.nan,)
    else:
        output_size = (minlength,)

    chunked_counts = blockwise(
        partial(np.bincount, minlength=minlength), "i", *args, token=token, meta=meta
    )
    chunked_counts._chunks = (
        output_size * len(chunked_counts.chunks[0]),
        *chunked_counts.chunks[1:],
    )

    from .reductions import _tree_reduce

    output = _tree_reduce(
        chunked_counts,
        aggregate=partial(_bincount_agg, dtype=meta.dtype),
        axis=(0,),
        keepdims=True,
        dtype=meta.dtype,
        split_every=split_every,
        concatenate=False,
    )
    output._chunks = (output_size, *chunked_counts.chunks[1:])
    output._meta = meta
    return output


@derived_from(np)
def digitize(a, bins, right=False):
    bins = asarray_safe(bins, like=meta_from_array(a))
    dtype = np.digitize(asarray_safe([0], like=bins), bins, right=False).dtype
    return a.map_blocks(np.digitize, dtype=dtype, bins=bins, right=right)


# TODO: dask linspace doesn't support delayed values
def _linspace_from_delayed(start, stop, num=50):
    linspace_name = "linspace-" + tokenize(start, stop, num)
    (start_ref, stop_ref, num_ref), deps = unpack_collections([start, stop, num])
    if len(deps) == 0:
        return np.linspace(start, stop, num=num)

    linspace_dsk = {(linspace_name, 0): (np.linspace, start_ref, stop_ref, num_ref)}
    linspace_graph = HighLevelGraph.from_collections(
        linspace_name, linspace_dsk, dependencies=deps
    )

    chunks = ((np.nan,),) if is_dask_collection(num) else ((num,),)
    return Array(linspace_graph, linspace_name, chunks, dtype=float)


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

    token = tokenize(a, bins, range, weights, density)
    name = "histogram-sum-" + token

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

    (bins_ref, range_ref), deps = unpack_collections([bins, range])

    # Map the histogram to all bins, forming a 2D array of histograms, stacked for each chunk
    if weights is None:
        dsk = {
            (name, i, 0): (_block_hist, k, bins_ref, range_ref)
            for i, k in enumerate(flatten(a.__dask_keys__()))
        }
        dtype = np.histogram([])[0].dtype
    else:
        a_keys = flatten(a.__dask_keys__())
        w_keys = flatten(weights.__dask_keys__())
        dsk = {
            (name, i, 0): (_block_hist, k, bins_ref, range_ref, w)
            for i, (k, w) in enumerate(zip(a_keys, w_keys))
        }
        dtype = weights.dtype

    deps = (a,) + deps
    if weights is not None:
        deps += (weights,)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=deps)

    # Turn graph into a 2D Array of shape (nchunks, nbins)
    nchunks = len(list(flatten(a.__dask_keys__())))
    nbins = bins.size - 1  # since `bins` is 1D
    chunks = ((1,) * nchunks, (nbins,))
    mapped = Array(graph, name, chunks, dtype=dtype)

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


def _block_histogramdd(sample, bins, range=None, weights=None):
    """Call numpy.histogramdd for a blocked/chunked calculation.

    Slurps the result into an additional outer axis via [np.newaxis].
    This new axis will be used to stack chunked calls of the numpy
    function and add them together later.

    Returns
    -------
    :py:object:`np.ndarray`
        NumPy array with an additional outer dimension.

    """
    return np.histogramdd(sample, bins, range=range, weights=weights)[0:1]


def histogramdd(sample, bins, range=None, normed=None, weights=None, density=None):
    """Blocked variant of :func:`numpy.histogramdd`.

    Chunking of the input data (``sample``) is only allowed along the
    0th (row) axis (the axis corresponding to the total number of
    samples). Data chunked along the 1st axis (column) axis is not
    compatible with this function. If weights are used, they must be
    chunked along the 0th axis identically to the input sample.

    A proper example setup for a three dimensional histogram, where
    the sample shape is ``(8, 3)`` and weights are shape ``(8,)``,
    sample chunks would be ``((4, 4), (3,))`` and the weights chunks
    would be ``((4, 4),)`` a table of the structure:

    +-------+-----------------------+-----------+
    |       |      sample (8 x 3)   |  weights  |
    +=======+=====+=====+=====+=====+=====+=====+
    | chunk | row | `x` | `y` | `z` | row | `w` |
    +-------+-----+-----+-----+-----+-----+-----+
    |       |   0 |   5 |   6 |   6 |   0 | 0.5 |
    |       +-----+-----+-----+-----+-----+-----+
    |       |   1 |   8 |   9 |   2 |   1 | 0.8 |
    |   0   +-----+-----+-----+-----+-----+-----+
    |       |   2 |   3 |   3 |   1 |   2 | 0.3 |
    |       +-----+-----+-----+-----+-----+-----+
    |       |   3 |   2 |   5 |   6 |   3 | 0.7 |
    +-------+-----+-----+-----+-----+-----+-----+
    |       |   4 |   3 |   1 |   1 |   4 | 0.3 |
    |       +-----+-----+-----+-----+-----+-----+
    |       |   5 |   3 |   2 |   9 |   5 | 1.3 |
    |   1   +-----+-----+-----+-----+-----+-----+
    |       |   6 |   8 |   1 |   5 |   6 | 0.8 |
    |       +-----+-----+-----+-----+-----+-----+
    |       |   7 |   3 |   5 |   3 |   7 | 0.7 |
    +-------+-----+-----+-----+-----+-----+-----+

    If the sample 0th dimension and weight 0th (row) dimension are
    chunked differently, a ``ValueError`` will be raised. If
    coordinate groupings ((x, y, z) trios) are separated by a chunk
    boundry, then a ``ValueError`` will be raised. We suggest that you
    rechunk your data if it is of that form.

    The chunks property of the data (and optional weights) are used to
    check for compatibility with the blocked algorithm (as described
    above); therefore, you must call `to_dask_array` on a collection
    from ``dask.dataframe``, i.e. :class:`dask.dataframe.Series` or
    :class:`dask.dataframe.DataFrame`.

    Parameters
    ----------
    sample : dask.array.Array (N, D) or sequence of dask.array.Array
        Multidimensional data to be histogrammed.

        Note the unusual interpretation of a sample when it is a
        sequence of dask Arrays:

        * When a (N, D) dask Array, each row is an entry in the sample
          (coordinate in D dimensional space).
        * When a sequence of dask Arrays, each element in the sequence
          is the array of values for a single coordinate. This type of
          input will be automatically rechunked along the column axis
          if necessary. This may induce a runtime increase.
    bins : sequence of arrays describing bin edges, int, or sequence of ints
        The bin specification.

        The possible binning configurations are:

        * A sequence of arrays describing the monotonically increasing
          bin edges along each dimension.
        * A single int describing the total number of bins that will
          be used in each dimension (this requires the ``range``
          argument to be defined).
        * A sequence of ints describing the total number of bins to be
          used in each dimension (this requires the ``range`` argument
          to be defined).

        When bins are described by arrays, the rightmost edge is
        included. Bins described by arrays also allows for non-uniform
        bin widths.
    range : sequence of pairs, optional
        A sequence of length D, each a (min, max) tuple giving the
        outer bin edges to be used if the edges are not given
        explicitly in ``bins``. If defined, this argument is required
        to have an entry for each dimension. Unlike
        :func:`numpy.histogramdd`, if `bins` does not define bin
        edges, this argument is required (this function will not
        automatically use the min and max of of the value in a given
        dimension because the input data may be lazy in dask).
    normed : bool, optional
        An alias for the density argument that behaves identically. To
        avoid confusion with the broken argument to `histogram`,
        `density` should be preferred.
    weights : dask.array.Array, optional
        An array of values weighing each sample in the input data. The
        chunks of the weights must be identical to the chunking along
        the 0th (row) axis of the data sample.
    density : bool, optional
        If ``False`` (default), the returned array represents the
        number of samples in each bin. If ``True``, the returned array
        represents the probability density function at each bin.

    See Also
    --------
    histogram

    Examples
    --------
    Computing the histogram in 5 blocks using different bin edges
    along each dimension:

    >>> import dask.array as da
    >>> x = da.random.uniform(0, 1, size=(1000, 3), chunks=(200, 3))
    >>> edges = [
    ...     np.linspace(0, 1, 5), # 4 bins in 1st dim
    ...     np.linspace(0, 1, 6), # 5 in the 2nd
    ...     np.linspace(0, 1, 4), # 3 in the 3rd
    ... ]
    >>> h, edges = da.histogramdd(x, bins=edges)
    >>> result = h.compute()
    >>> result.shape
    (4, 5, 3)

    Defining the bins by total number and their ranges, along with
    using weights:

    >>> bins = (4, 5, 3)
    >>> ranges = ((0, 1),) * 3  # expands to ((0, 1), (0, 1), (0, 1))
    >>> w = da.random.uniform(0, 1, size=(1000,), chunks=x.chunksize[0])
    >>> h, edges = da.histogramdd(x, bins=bins, range=ranges, weights=w)
    >>> np.isclose(h.sum().compute(), w.sum().compute())
    True

    """

    # logic used in numpy.histogramdd to handle normed/density.
    if normed is None:
        if density is None:
            density = False
    elif density is None:
        # an explicit normed argument was passed, alias it to the new name
        density = normed
    else:
        raise TypeError("Cannot specify both 'normed' and 'density'")

    # check if any dask collections (dc) were passed to bins= or
    # range= these are unsupported.
    dc_bins = is_dask_collection(bins)
    if isinstance(bins, (list, tuple)):
        dc_bins = dc_bins or any([is_dask_collection(b) for b in bins])
    dc_range = (
        any([is_dask_collection(r) for r in range]) if range is not None else False
    )
    if dc_bins or dc_range:
        raise NotImplementedError(
            "Passing dask collections to bins=... or range=... is not supported."
        )

    # generate token and name for task
    token = tokenize(sample, bins, range, weights, density)
    name = f"histogramdd-sum-{token}"

    # N == total number of samples
    # D == total number of dimensions
    try:
        # Recommended input ND-array
        N, D = sample.shape
    except (AttributeError, ValueError):
        # If we have a sequence of 1D arrays
        sample = atleast_2d(sample).T
        N, D = sample.shape
        # rechunk if necessary
        if sample.chunksize[1] != D:
            sample = sample.rechunk((sample.chunksize[0], D))

    # Require only Array or Delayed objects for bins, range, and weights.
    for argname, val in [("bins", bins), ("range", range), ("weights", weights)]:
        if not isinstance(bins, (Array, Delayed)) and is_dask_collection(bins):
            raise TypeError(
                "Dask types besides Array and Delayed are not supported "
                "for `histogramdd`. For argument `{}`, got: {!r}".format(argname, val)
            )

    # Require data to be chunked along the first axis only.
    if sample.shape[1:] != sample.chunksize[1:]:
        raise ValueError("Input array can only be chunked along the 0th axis")

    # Require that the chunking of the sample and weights are compatible.
    if weights is not None and weights.chunks[0] != sample.chunks[0]:
        raise ValueError(
            "Input array and weights must have the same shape "
            "and chunk structure along the first dimension."
        )

    # if bins is a list, tuple, then make sure the length is the same
    # as the number dimensions.
    if isinstance(bins, (list, tuple)):
        if len(bins) != D:
            raise ValueError(
                "The dimension of bins must be equal to the dimension of the sample."
            )

    # if range is defined, check that it's the right length and also a
    # sequence of pairs.
    if range is not None:
        if len(range) != D:
            raise ValueError(
                "range argument requires one entry, a min max pair, per dimension."
            )
        if not all(len(r) == 2 for r in range):
            raise ValueError("range argument should be a sequence of pairs")

    # we will return the edges to mimic the NumPy API (we also use the
    # edges later as a way to calculate the total number of bins).
    if isinstance(bins, int):
        bins = (bins,) * D
    if all(isinstance(b, int) for b in bins) and all(len(r) == 2 for r in range):
        edges = [np.linspace(r[0], r[1], b + 1) for b, r in zip(bins, range)]
    else:
        edges = [np.asarray(b) for b in bins]

    # With dsk below, we will construct a (D + 1) dimensional array
    # stacked for each chunk. For example, if the histogram is going
    # to be 3 dimensions, this creates a stack of cubes (1 cube for
    # each sample chunk) that will be collapsed into a final cube (the
    # result).

    # This tuple of zeros represents the chunk index along the columns
    # (we only allow chunking along the rows).
    column_zeros = tuple(0 for _ in _range(D))

    if weights is None:
        dsk = {
            (name, i, *column_zeros): (_block_histogramdd, k, bins, range)
            for i, k in enumerate(flatten(sample.__dask_keys__()))
        }
        dtype = np.histogramdd([])[0].dtype
    else:
        a_keys = flatten(sample.__dask_keys__())
        w_keys = flatten(weights.__dask_keys__())
        dsk = {
            (name, i, *column_zeros): (_block_histogramdd, k, bins, range, w)
            for i, (k, w) in enumerate(zip(a_keys, w_keys))
        }
        dtype = weights.dtype

    deps = (sample,)
    if weights is not None:
        deps += (weights,)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=deps)

    nchunks = len(list(flatten(sample.__dask_keys__())))
    all_nbins = tuple((b.size - 1,) for b in edges)
    stacked_chunks = ((1,) * nchunks, *all_nbins)
    mapped = Array(graph, name, stacked_chunks, dtype=dtype)

    # Finally, sum over chunks providing to get the final D
    # dimensional result array.
    n = mapped.sum(axis=0)

    if density:
        # compute array of values to divide by the bin width along
        # each dimension.
        width_divider = np.ones(n.shape)
        for i in _range(D):
            shape = np.ones(D, int)
            shape[i] = width_divider.shape[i]
            width_divider *= np.diff(edges[i]).reshape(shape)
        width_divider = asarray(width_divider, chunks=n.chunks)
        return n / width_divider / n.sum(), edges

    return n, [asarray(entry) for entry in edges]


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


@implements(np.round, np.round_)
@derived_from(np)
def round(a, decimals=0):
    return a.map_blocks(np.round, decimals=decimals, dtype=a.dtype)


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


@derived_from(np)
def unique(ar, return_index=False, return_inverse=False, return_counts=False):
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

    out = blockwise(_unique_internal, "i", *args, dtype=out_dtype, return_inverse=False)
    out._chunks = tuple((np.nan,) * len(c) for c in out.chunks)

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

    name = "unique-aggregate-" + out.name
    dsk = {
        (name, 0): (
            (_unique_internal,)
            + tuple(
                (np.concatenate, o.__dask_keys__())
                if hasattr(o, "__dask_keys__")
                else o
                for o in out_parts
            )
            + (return_inverse,)
        )
    }
    out_dtype = [("values", ar.dtype)]
    if return_index:
        out_dtype.append(("indices", np.intp))
    if return_inverse:
        out_dtype.append(("inverse", np.intp))
    if return_counts:
        out_dtype.append(("counts", np.intp))

    dependencies = [o for o in out_parts if hasattr(o, "__dask_keys__")]
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=dependencies)
    chunks = ((np.nan,),)
    out = Array(graph, name, chunks, out_dtype)

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
        mtches = (ar[:, None] == out["values"][None, :]).astype(np.intp)
        result.append((mtches * out["inverse"]).sum(axis=1))
    if return_counts:
        result.append(out["counts"])

    if len(result) == 1:
        result = result[0]
    else:
        result = tuple(result)

    return result


def _isin_kernel(element, test_elements, assume_unique=False):
    values = np.in1d(element.ravel(), test_elements, assume_unique=assume_unique)
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
        s = -s
        s %= result.shape[i]

        sl1 = result.ndim * [slice(None)]
        sl2 = result.ndim * [slice(None)]

        sl1[i] = slice(s, None)
        sl2[i] = slice(None, s)

        sl1 = tuple(sl1)
        sl2 = tuple(sl2)

        result = concatenate([result[sl1], result[sl2]], axis=i)

    result = result.reshape(array.shape)

    return result


@derived_from(np)
def shape(array):
    return array.shape


@derived_from(np)
def union1d(ar1, ar2):
    return unique(concatenate((ar1.ravel(), ar2.ravel())))


@derived_from(np)
def ravel(array_like):
    return asanyarray(array_like).reshape((-1,))


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

    a = a[sl]

    return a


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
    """ pandas.isnull for dask arrays """
    # eagerly raise ImportError, if pandas isn't available
    import pandas as pd  # noqa

    return elemwise(_asarray_isnull, values, dtype="bool")


def notnull(values):
    """ pandas.notnull for dask arrays """
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


def _isnonzero_vec(v):
    return bool(np.count_nonzero(v))


_isnonzero_vec = np.vectorize(_isnonzero_vec, otypes=[bool])


def isnonzero(a):
    if a.dtype.kind in {"U", "S"}:
        # NumPy treats all-whitespace strings as falsy (like in `np.nonzero`).
        # but not in `.astype(bool)`. To match the behavior of numpy at least until
        # 1.19, we use `_isnonzero_vec`. When NumPy changes behavior, we should just
        # use the try block below.
        # https://github.com/numpy/numpy/issues/9875
        return a.map_blocks(_isnonzero_vec, dtype=bool)
    try:
        np.zeros(tuple(), dtype=a.dtype).astype(bool)
    except ValueError:
        ######################################################
        # Handle special cases where conversion to bool does #
        # not work correctly.                                #
        #                                                    #
        # xref: https://github.com/numpy/numpy/issues/9479   #
        ######################################################
        return a.map_blocks(_isnonzero_vec, dtype=bool)
    else:
        return a.astype(bool)


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


def _ravel_multi_index_kernel(multi_index, func_kwargs):
    return np.ravel_multi_index(multi_index, **func_kwargs)


@wraps(np.ravel_multi_index)
def ravel_multi_index(multi_index, dims, mode="raise", order="C"):
    return multi_index.map_blocks(
        _ravel_multi_index_kernel,
        dtype=np.intp,
        chunks=(multi_index.shape[-1],),
        drop_axis=0,
        func_kwargs=dict(dims=dims, mode=mode, order=order),
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
        name="piecewise",
        funclist=funclist,
        func_args=args,
        func_kw=kw,
    )


def _partition(total: int, divisor: int) -> Tuple[Tuple[int, ...], Tuple[int, ...]]:
    """
    Given a total and a divisor, return two tuples: A tuple containing `divisor` repeated
    the number of times it divides `total`, and length-1 or empty tuple containing the remainder when
    `total` is divided by `divisor`. If `divisor` factors `total`, i.e. if the remainder is 0, then
    `remainder` is empty.
    """
    multiples = (divisor,) * (total // divisor)
    remainder = ()
    if (total % divisor) > 0:
        remainder = (total % divisor,)
    return (multiples, remainder)


def aligned_coarsen_chunks(chunks: List[int], multiple: int) -> Tuple[int]:
    """
    Returns a new chunking aligned with the coarsening multiple.
    Any excess is at the end of the array.

    Examples
    --------
    >>> aligned_coarsen_chunks(chunks=(1, 2, 3), multiple=4)
    (4, 2)
    >>> aligned_coarsen_chunks(chunks=(1, 20, 3, 4), multiple=4)
    (4, 20, 4)
    >>> aligned_coarsen_chunks(chunks=(20, 10, 15, 23, 24), multiple=10)
    (20, 10, 20, 20, 20, 2)
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

    if "dask" in inspect.getfile(reduction):
        reduction = getattr(np, reduction.__name__)

    new_chunks = {}
    for i, div in axes.items():
        aligned = aligned_coarsen_chunks(x.chunks[i], div)
        if aligned != x.chunks[i]:
            new_chunks[i] = aligned
    if new_chunks:
        x = x.rechunk(new_chunks)

    name = "coarsen-" + tokenize(reduction, x, axes, trim_excess)
    dsk = {
        (name,)
        + key[1:]: (apply, chunk.coarsen, [reduction, key, axes, trim_excess], kwargs)
        for key in flatten(x.__dask_keys__())
    }
    chunks = tuple(
        tuple(int(bd // axes.get(i, 1)) for bd in bds) for i, bds in enumerate(x.chunks)
    )

    meta = reduction(np.empty((1,) * x.ndim, dtype=x.dtype), **kwargs)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[x])
    return Array(graph, name, chunks, meta=meta)


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
        name = "values-" + tokenize(values)
        dtype = getattr(values, "dtype", type(values))
        values = Array({(name,): values}, name, chunks=(), dtype=dtype)

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
        arr[
            tuple(slice(1, None) if axis == n else slice(None) for n in range(arr.ndim))
        ]
        if i != 0
        else arr
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


def _average(a, axis=None, weights=None, returned=False, is_masked=False):
    # This was minimally modified from numpy.average
    # See numpy license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    # or NUMPY_LICENSE.txt within this directory
    # Wrapper used by da.average or da.ma.average.
    a = asanyarray(a)

    if weights is None:
        avg = a.mean(axis)
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
            from .ma import getmaskarray

            wgt = wgt * (~getmaskarray(a))
        scl = wgt.sum(axis=axis, dtype=result_dtype)
        avg = multiply(a, wgt, dtype=result_dtype).sum(axis) / scl

    if returned:
        if scl.shape != avg.shape:
            scl = broadcast_to(scl, avg.shape).copy()
        return avg, scl
    else:
        return avg


@derived_from(np)
def average(a, axis=None, weights=None, returned=False):
    return _average(a, axis, weights, returned, is_masked=False)


@derived_from(np)
def tril(m, k=0):
    m = asarray_safe(m, like=m)
    mask = tri(
        *m.shape[-2:], k=k, dtype=bool, chunks=m.chunks[-2:], like=meta_from_array(m)
    )

    return where(mask, m, zeros_like_safe(m, shape=(1,)))


@derived_from(np)
def triu(m, k=0):
    m = asarray_safe(m, like=m)
    mask = tri(
        *m.shape[-2:],
        k=k - 1,
        dtype=bool,
        chunks=m.chunks[-2:],
        like=meta_from_array(m),
    )

    return where(mask, zeros_like_safe(m, shape=(1,)), m)


@derived_from(np)
def tril_indices(n, k=0, m=None, chunks="auto"):
    return nonzero(tri(n, m, k=k, dtype=bool, chunks=chunks))


@derived_from(np)
def tril_indices_from(arr, k=0):
    if arr.ndim != 2:
        raise ValueError("input array must be 2-d")
    return tril_indices(arr.shape[-2], k=k, m=arr.shape[-1], chunks=arr.chunks)


@derived_from(np)
def triu_indices(n, k=0, m=None, chunks="auto"):
    return nonzero(~tri(n, m, k=k - 1, dtype=bool, chunks=chunks))


@derived_from(np)
def triu_indices_from(arr, k=0):
    if arr.ndim != 2:
        raise ValueError("input array must be 2-d")
    return triu_indices(arr.shape[-2], k=k, m=arr.shape[-1], chunks=arr.chunks)

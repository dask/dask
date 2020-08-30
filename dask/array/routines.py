import inspect
import math
import warnings
from collections.abc import Iterable
from functools import wraps, partial
from numbers import Real, Integral
from distutils.version import LooseVersion
from typing import List

import numpy as np
from tlz import concat, sliding_window, interleave

from ..compatibility import apply
from ..core import flatten
from ..base import tokenize, is_dask_collection
from ..delayed import unpack_collections, Delayed
from ..highlevelgraph import HighLevelGraph
from ..utils import funcname, derived_from, is_arraylike
from . import chunk
from .creation import arange, diag, empty, indices
from .utils import safe_wraps, validate_axis, meta_from_array, zeros_like_safe
from .wrap import ones
from .ufunc import multiply, sqrt

from .core import (
    Array,
    map_blocks,
    elemwise,
    asarray,
    asanyarray,
    concatenate,
    stack,
    blockwise,
    broadcast_shapes,
    is_scalar_for_elemwise,
    broadcast_to,
    tensordot_lookup,
    implements,
)

from .einsumfuncs import einsum  # noqa
from .numpy_compat import _unravel_index_keyword


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
    else:
        axes = tuple(range(a.ndim))[::-1]
    axes = tuple(d + a.ndim if d < 0 else d for d in axes)
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


alphabet = "abcdefghijklmnopqrstuvwxyz"
ALPHABET = alphabet.upper()


def _tensordot(a, b, axes):
    x = max([a, b], key=lambda x: x.__array_priority__)
    tensordot = tensordot_lookup.dispatch(type(x))

    # workaround may be removed when numpy version (currently 1.13.0) is bumped
    a_dims = np.array([a.shape[i] for i in axes[0]])
    b_dims = np.array([b.shape[i] for i in axes[1]])
    if (
        len(a_dims) > 0
        and (a_dims == b_dims).all()
        and a_dims.min() == 0
        and LooseVersion(np.__version__) < LooseVersion("1.14")
    ):
        x = np.zeros(
            tuple(
                [s for i, s in enumerate(a.shape) if i not in axes[0]]
                + [s for i, s in enumerate(b.shape) if i not in axes[1]]
            )
        )
    else:
        x = tensordot(a, b, axes=axes)

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

    dt = np.promote_types(lhs.dtype, rhs.dtype)

    left_index = list(range(lhs.ndim))
    right_index = list(range(lhs.ndim, lhs.ndim + rhs.ndim))
    out_index = left_index + right_index

    for l, r in zip(left_axes, right_axes):
        out_index.remove(right_index[r])
        right_index[r] = left_index[l]

    intermediate = blockwise(
        _tensordot,
        out_index,
        lhs,
        left_index,
        rhs,
        right_index,
        dtype=dt,
        axes=(left_axes, right_axes),
    )

    result = intermediate.sum(axis=left_axes)
    return result


@derived_from(np)
def dot(a, b):
    return tensordot(a, b, axes=((a.ndim - 1,), (b.ndim - 2,)))


@derived_from(np)
def vdot(a, b):
    return dot(a.conj().ravel(), b.ravel())


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

    out = blockwise(
        np.matmul,
        tuple(range(1, a.ndim + 1)),
        a,
        tuple(range(1, a.ndim - 1)) + (a.ndim - 1, 0),
        b,
        tuple(range(1, a.ndim - 1)) + (0, a.ndim),
        dtype=result_type(a, b),
        concatenate=True,
    )

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
    Apply a function to 1-D slices along the given axis. This is
    a blocked variant of :func:`numpy.apply_along_axis` implemented via
    :func:`dask.array.map_blocks`

    Parameters
    ----------
    func1d : callable
        Function to apply to 1-D slices of the array along the given axis
    axis : int
        Axis along which func1d will be applied
    arr : dask array
        Dask array to which ``func1d`` will be applied
    args : any
        Additional arguments to ``func1d``.
    dtype : str or dtype, optional
        The dtype of the output of ``func1d``.
    shape : tuple, optional
        The shape of the output of ``func1d``.
    kwargs : any
        Additional keyword arguments for ``func1d``.

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


def _bincount_sum(bincounts, dtype=int):
    n = max(map(len, bincounts))
    out = zeros_like_safe(bincounts[0], shape=n, dtype=dtype)
    for b in bincounts:
        out[: len(b)] += b
    return out


@derived_from(np)
def bincount(x, weights=None, minlength=0):
    if x.ndim != 1:
        raise ValueError("Input array must be one dimensional. Try using x.ravel()")
    if weights is not None:
        if weights.chunks != x.chunks:
            raise ValueError("Chunks of input array x and weights must match.")

    token = tokenize(x, weights, minlength)
    name = "bincount-" + token
    final_name = "bincount-sum" + token
    # Call np.bincount on each block, possibly with weights
    if weights is not None:
        dsk = {
            (name, i): (np.bincount, (x.name, i), (weights.name, i), minlength)
            for i, _ in enumerate(x.__dask_keys__())
        }
        dtype = np.bincount([1], weights=[1]).dtype
    else:
        dsk = {
            (name, i): (np.bincount, (x.name, i), None, minlength)
            for i, _ in enumerate(x.__dask_keys__())
        }
        dtype = np.bincount([]).dtype

    dsk[(final_name, 0)] = (_bincount_sum, list(dsk), dtype)
    graph = HighLevelGraph.from_collections(
        final_name, dsk, dependencies=[x] if weights is None else [x, weights]
    )

    if minlength == 0:
        chunks = ((np.nan,),)
    else:
        chunks = ((minlength,),)

    meta = meta_from_array(x, 1, dtype=dtype)

    return Array(graph, final_name, chunks, meta=meta)


@derived_from(np)
def digitize(a, bins, right=False):
    bins = np.asarray(bins)
    dtype = np.digitize([0], bins, right=False).dtype
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
    a : array_like
        Input data. The histogram is computed over the flattened array.
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
    weights : array_like, optional
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
def ravel(array):
    return array.reshape((-1,))


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


def _int_piecewise(x, *condlist, **kwargs):
    return np.piecewise(
        x, list(condlist), kwargs["funclist"], *kwargs["func_args"], **kwargs["func_kw"]
    )


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
                func_kwargs={_unravel_index_keyword: shape, "order": order},
            )
        )
    else:
        unraveled_indices = tuple(empty((0,), dtype=np.intp, chunks=1) for i in shape)

    return unraveled_indices


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


def aligned_coarsen_chunks(chunks: List[int], multiple: int) -> List[int]:
    """
    Returns a new chunking aligned with the coarsening multiple.
    Any excess is at the end of the array.

    Examples
    --------
    >>> aligned_coarsen_chunks(chunks=(1, 2, 3), multiple=4)
    (4, 2)
    >>> aligned_coarsen_chunks(chunks=(1, 20, 3, 4), multiple=4)
    (20, 4, 4)
    >>> aligned_coarsen_chunks(chunks=(20, 10, 15, 23, 24), multiple=10)
    (20, 10, 20, 20, 20, 2)
    """

    def choose_new_size(multiple, q, left):
        """
        See if multiple * q is a good choice when 'left' elements are remaining.
        Else return multiple * (q-1)
        """
        possible = multiple * q
        if (left - possible) > 0:
            return possible
        else:
            return multiple * (q - 1)

    newchunks = []
    left = sum(chunks) - sum(newchunks)
    chunkgen = (c for c in chunks)
    while left > 0:
        if left < multiple:
            newchunks.append(left)
            break

        chunk_size = next(chunkgen, 0)
        if chunk_size == 0:
            chunk_size = multiple

        q, r = divmod(chunk_size, multiple)
        if q == 0:
            continue
        elif r == 0:
            newchunks.append(chunk_size)
        elif r >= 5:
            newchunks.append(choose_new_size(multiple, q + 1, left))
        else:
            newchunks.append(choose_new_size(multiple, q, left))

        left = sum(chunks) - sum(newchunks)

    return tuple(newchunks)


@wraps(chunk.coarsen)
def coarsen(reduction, x, axes, trim_excess=False, **kwargs):
    if not trim_excess and not all(x.shape[i] % div == 0 for i, div in axes.items()):
        msg = "Coarsening factor does not align with block dimensions"
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

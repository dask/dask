from __future__ import annotations

import math
from numbers import Integral, Real

import numpy as np

from functools import partial

from dask.array._array_expr._collection import (
    Array,
    asarray,
    asanyarray,
    concatenate,
    elemwise,
    ravel,
)
from dask.array._array_expr._overlap import map_overlap
from dask.array.utils import validate_axis
from dask.base import is_dask_collection
from dask.utils import derived_from


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


def gradient(f, *varargs, axis=None, **kwargs):
    """
    Return the gradient of an N-dimensional array.

    This docstring was copied from numpy.gradient.

    Some inconsistencies with the Dask version may exist.

    The gradient is computed using second order accurate central differences
    in the interior points and either first or second order accurate one-sides
    (forward or backwards) differences at the boundaries.
    The returned gradient hence has the same shape as the input array.

    Parameters
    ----------
    f : array_like
        An N-dimensional array containing samples of a scalar function.
    varargs : list of scalar or array, optional
        Spacing between f values. Default unitary spacing for all dimensions.
        Spacing can be specified using:

        1. single scalar to specify a sample distance for all dimensions.
        2. N scalars to specify a constant sample distance for each dimension.
           i.e. `dx`, `dy`, `dz`, ...
        3. N arrays to specify the coordinates of the values along each
           dimension of F. The length of the array must match the size of
           the corresponding dimension
        4. Any combination of N scalars/arrays with the meaning of 2. and 3.

        If `axis` is given, the number of varargs must equal the number of axes.
        Default: 1.
    axis : None or int or tuple of ints, optional
        Gradient is calculated only along the given axis or axes.
        The default (axis = None) is to calculate the gradient for all the axes
        of the input array. axis may be negative, in which case it counts from
        the last to the first axis.

    Returns
    -------
    gradient : ndarray or list of ndarray
        A list of ndarrays (or a single ndarray if there is only one dimension)
        corresponding to the derivatives of f with respect to each dimension.
        Each derivative has the same shape as f.

    Other Parameters
    ----------------
    edge_order : {1, 2}, optional
        Gradient is calculated using N-th order accurate differences
        at the boundaries. Default: 1.
    """
    f = asarray(f)

    kwargs["edge_order"] = math.ceil(kwargs.get("edge_order", 1))
    if kwargs["edge_order"] > 2:
        raise ValueError("edge_order must be less than or equal to 2.")

    drop_result_list = False
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

    if issubclass(f.dtype.type, (np.bool_, Integral)):
        f = f.astype(float)
    elif issubclass(f.dtype.type, Real) and f.dtype.itemsize < 4:
        f = f.astype(float)

    results = []
    for i, ax in enumerate(axis):
        for c in f.chunks[ax]:
            if np.min(c) < kwargs["edge_order"] + 1:
                raise ValueError(
                    "Chunk size must be larger than edge_order + 1. "
                    f"Minimum chunk for axis {ax} is {np.min(c)}. Rechunk to "
                    "proceed."
                )

        if np.isscalar(varargs[i]):
            array_locs = None
        else:
            if is_dask_collection(varargs[i]):
                raise NotImplementedError("dask array coordinated is not supported.")
            # coordinate position for each block taking overlap into account
            chunk = np.array(f.chunks[ax])
            array_loc_stop = np.cumsum(chunk) + 1
            array_loc_start = array_loc_stop - chunk - 2
            array_loc_stop[-1] -= 1
            array_loc_start[0] = 0
            array_locs = (array_loc_start, array_loc_stop)

        results.append(
            map_overlap(
                _gradient_kernel,
                f,
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


def compress(condition, a, axis=None):
    """
    Return selected slices of an array along given axis.

    This docstring was copied from numpy.compress.

    Some inconsistencies with the Dask version may exist.

    Parameters
    ----------
    condition : 1-D array of bools
        Array that selects which entries to return. If len(condition)
        is less than the size of a along the given axis, then output is
        truncated to the length of the condition array.
    a : array_like
        Array from which to extract a part.
    axis : int, optional
        Axis along which to take slices. If None (default), work on the
        flattened array.

    Returns
    -------
    compressed_array : ndarray
        A copy of a without the slices along axis for which condition
        is false.
    """
    from dask.array.utils import is_arraylike

    if not is_arraylike(condition):
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


def _searchsorted_block(x, y, side):
    res = np.searchsorted(x, y, side=side)
    # 0 is only correct for the first block of a, but blockwise doesn't have a way
    # of telling which block is being operated on (unlike map_blocks),
    # so set all 0 values to a special value and set back at the end of searchsorted
    res[res == 0] = -1
    return res[np.newaxis, :]


def searchsorted(a, v, side="left", sorter=None):
    """
    Find indices where elements should be inserted to maintain order.

    This docstring was copied from numpy.searchsorted.

    Some inconsistencies with the Dask version may exist.

    Find the indices into a sorted array `a` such that, if the
    corresponding elements in `v` were inserted before the indices, the
    order of `a` would be preserved.

    Parameters
    ----------
    a : 1-D array_like
        Input array. If `sorter` is None, then it must be sorted in
        ascending order, otherwise `sorter` must be an array of indices
        that sort it.
    v : array_like
        Values to insert into `a`.
    side : {'left', 'right'}, optional
        If 'left', the index of the first suitable location found is given.
        If 'right', return the last such index.  If there is no suitable
        index, return either 0 or N (where N is the length of `a`).
    sorter : 1-D array_like, optional
        Optional array of integer indices that sort array a into ascending
        order. They are typically the result of argsort.

    Returns
    -------
    indices : int or array of ints
        Array of insertion points with the same shape as `v`,
        or an integer if `v` is a scalar.
    """
    from dask.array._array_expr._collection import blockwise, where
    from dask.array.utils import meta_from_array

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
    from dask.array.utils import array_safe

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


def outer(a, b):
    """
    Compute the outer product of two vectors.

    This docstring was copied from numpy.outer.

    Some inconsistencies with the Dask version may exist.

    Given two vectors, ``a = [a0, a1, ..., aM]`` and
    ``b = [b0, b1, ..., bN]``,
    the outer product is::

      [[a0*b0  a0*b1 ... a0*bN ]
       [a1*b0    .
       [ ...          .
       [aM*b0            aM*bN ]]

    Parameters
    ----------
    a : (M,) array_like
        First input vector.  Input is flattened if not already 1-dimensional.
    b : (N,) array_like
        Second input vector.  Input is flattened if not already 1-dimensional.

    Returns
    -------
    out : (M, N) ndarray
        ``out[i, j] = a[i] * b[j]``
    """
    from dask.array._array_expr._collection import blockwise

    a = asarray(a).flatten()
    b = asarray(b).flatten()

    dtype = np.outer(a.dtype.type(), b.dtype.type()).dtype

    return blockwise(np.outer, "ij", a, "i", b, "j", dtype=dtype)


@derived_from(np)
def round(a, decimals=0):
    """Round an array to the given number of decimals."""
    a = asarray(a)
    return a.map_blocks(np.round, decimals=decimals, dtype=a.dtype)


@derived_from(np)
def around(x, decimals=0):
    """Evenly round to the given number of decimals."""
    return round(x, decimals=decimals)


def _asarray_isnull(values):
    import pandas as pd

    return np.asarray(pd.isnull(values))


def isnull(values):
    """pandas.isnull for dask arrays"""
    # eagerly raise ImportError, if pandas isn't available
    import pandas as pd  # noqa: F401

    return elemwise(_asarray_isnull, values, dtype="bool")


def notnull(values):
    """pandas.notnull for dask arrays"""
    return ~isnull(values)


@derived_from(np)
def isclose(arr1, arr2, rtol=1e-5, atol=1e-8, equal_nan=False):
    """Returns a boolean array where two arrays are element-wise equal within a tolerance."""
    func = partial(np.isclose, rtol=rtol, atol=atol, equal_nan=equal_nan)
    return elemwise(func, arr1, arr2, dtype="bool")


@derived_from(np)
def allclose(arr1, arr2, rtol=1e-5, atol=1e-8, equal_nan=False):
    """Returns True if two arrays are element-wise equal within a tolerance."""
    return isclose(arr1, arr2, rtol=rtol, atol=atol, equal_nan=equal_nan).all()


@derived_from(np)
def append(arr, values, axis=None):
    """Append values to the end of an array."""
    arr = asanyarray(arr)
    if axis is None:
        if arr.ndim != 1:
            arr = ravel(arr)
        values = ravel(asanyarray(values))
        axis = arr.ndim - 1
    return concatenate((arr, values), axis=axis)


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
def count_nonzero(a, axis=None):
    """Counts the number of non-zero values in the array."""
    return isnonzero(asarray(a)).astype(np.intp).sum(axis=axis)


@derived_from(np)
def ndim(a):
    """Return the number of dimensions of an array."""
    a = asarray(a)
    return a.ndim


@derived_from(np)
def shape(a):
    """Return the shape of an array."""
    a = asarray(a)
    return a.shape


def result_type(*arrays_and_dtypes):
    """Returns the type that results from applying the NumPy type promotion rules."""
    args = []
    for a in arrays_and_dtypes:
        if isinstance(a, Array):
            args.append(a.dtype)
        else:
            args.append(a)
    return np.result_type(*args)


def unify_chunks(*args, **kwargs):
    """
    Unify chunks across a sequence of arrays

    This utility function is used within other common operations like
    :func:`dask.array.core.map_blocks` and :func:`dask.array.core.blockwise`.
    It is not commonly used by end-users directly.

    Parameters
    ----------
    *args: sequence of Array, index pairs
        Sequence like (x, 'ij', y, 'jk', z, 'i')

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones(10, chunks=((5, 2, 3),))
    >>> y = da.ones(10, chunks=((2, 3, 5),))
    >>> chunkss, arrays = unify_chunks(x, 'i', y, 'i')
    >>> chunkss
    {'i': (2, 3, 2, 3)}

    Returns
    -------
    chunkss : dict
        Map like {index: chunks}.
    arrays : list
        List of rechunked arrays.
    """
    from dask._collections import new_collection
    from dask.array._array_expr._expr import unify_chunks_expr
    from toolz import partition

    if not args:
        return {}, []

    arginds = [
        (asanyarray(a) if ind is not None else a, ind) for a, ind in partition(2, args)
    ]

    arrays, inds = zip(*arginds)
    if all(ind is None for ind in inds):
        return {}, list(arrays)

    # Convert to expression-level args
    expr_args = []
    for a, ind in arginds:
        if ind is not None:
            expr_args.extend([a.expr, ind])
        else:
            expr_args.extend([a, ind])

    chunkss, expr_arrays, _ = unify_chunks_expr(*expr_args)

    # Convert back to collections
    result_arrays = []
    for a, orig_a_ind in zip(expr_arrays, arginds):
        orig_a, ind = orig_a_ind
        if ind is None:
            result_arrays.append(orig_a)
        else:
            result_arrays.append(new_collection(a))

    return chunkss, result_arrays


@derived_from(np)
def broadcast_arrays(*args, subok=False):
    """Broadcast any number of arrays against each other."""
    from dask.array._array_expr._collection import broadcast_to
    from dask.array.core import broadcast_shapes, broadcast_chunks
    from dask.array.numpy_compat import NUMPY_GE_200
    from toolz import concat

    subok = bool(subok)

    to_array = asanyarray if subok else asarray
    args = tuple(to_array(e) for e in args)

    if not args:
        if NUMPY_GE_200:
            return ()
        return []

    # Unify uneven chunking
    inds = [list(reversed(range(x.ndim))) for x in args]
    uc_args = list(concat(zip(args, inds)))
    _, args = unify_chunks(*uc_args, warn=False)

    shape = broadcast_shapes(*(e.shape for e in args))
    chunks = broadcast_chunks(*(e.chunks for e in args))

    if NUMPY_GE_200:
        result = tuple(broadcast_to(e, shape=shape, chunks=chunks) for e in args)
    else:
        result = [broadcast_to(e, shape=shape, chunks=chunks) for e in args]

    return result


@derived_from(np)
def take(a, indices, axis=0):
    """
    Take elements from an array along an axis.

    This docstring was copied from numpy.take.

    Parameters
    ----------
    a : dask array
        The source array.
    indices : array_like
        The indices of the values to extract.
    axis : int, optional
        The axis over which to select values.

    Returns
    -------
    out : dask array
        The returned array has the same type as a.
    """
    a = asarray(a)
    axis = validate_axis(axis, a.ndim)

    if isinstance(a, np.ndarray) and isinstance(indices, Array):
        return _take_dask_array_from_numpy(a, indices, axis)
    else:
        return a[(slice(None),) * axis + (indices,)]


def _take_dask_array_from_numpy(a, indices, axis):
    """Take from a numpy array using a dask array of indices."""
    from dask.array._array_expr._map_blocks import map_blocks

    assert isinstance(a, np.ndarray)
    assert isinstance(indices, Array)

    return map_blocks(
        lambda block: np.take(a, block, axis), indices, chunks=indices.chunks, dtype=a.dtype
    )


@derived_from(np)
def tril(m, k=0):
    from dask.array._array_expr._collection import where
    from dask.array._array_expr._creation import tri
    from dask.array.utils import meta_from_array

    m = asarray(m)
    mask = tri(
        *m.shape[-2:],
        k=k,
        dtype=bool,
        chunks=m.chunks[-2:],
        like=meta_from_array(m),
    )

    return where(mask, m, np.zeros_like(m._meta, shape=(1,)))


@derived_from(np)
def triu(m, k=0):
    from dask.array._array_expr._collection import where
    from dask.array._array_expr._creation import tri
    from dask.array.utils import meta_from_array

    m = asarray(m)
    mask = tri(
        *m.shape[-2:],
        k=k - 1,
        dtype=bool,
        chunks=m.chunks[-2:],
        like=meta_from_array(m),
    )

    return where(mask, np.zeros_like(m._meta, shape=(1,)), m)

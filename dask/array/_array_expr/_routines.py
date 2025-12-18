from __future__ import annotations

import itertools
import math
import warnings
from functools import cached_property, partial
from numbers import Integral, Number, Real

import numpy as np

from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._collection import (
    Array,
    array,
    asarray,
    asanyarray,
    broadcast_to,
    concatenate,
    elemwise,
    new_collection,
    ravel,
    stack,
)
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr._overlap import map_overlap
from dask.array.utils import meta_from_array, validate_axis
from dask.base import is_dask_collection
from dask.utils import derived_from, funcname


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
    from dask.array._array_expr._collection import blockwise
    from dask.array._array_expr.routines._where import where
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
    return elemwise(np.round, a, dtype=a.dtype, decimals=decimals)


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
        return elemwise(_isnonzero, a, dtype=bool)
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


def _take_constant(indices, a, axis):
    """Take from a constant array using indices."""
    return np.take(a, indices, axis)


def _take_dask_array_from_numpy(a, indices, axis):
    """Take from a numpy array using a dask array of indices."""
    assert isinstance(a, np.ndarray)
    assert isinstance(indices, Array)

    return elemwise(_take_constant, indices, dtype=a.dtype, a=a, axis=axis)


@derived_from(np)
def tril(m, k=0):
    from dask.array._array_expr._creation import tri
    from dask.array._array_expr.routines._where import where
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
    from dask.array._array_expr._creation import tri
    from dask.array._array_expr.routines._where import where
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


@derived_from(np)
def tril_indices(n, k=0, m=None, chunks="auto"):
    from dask.array._array_expr._creation import tri

    return nonzero(tri(n, m, k=k, dtype=bool, chunks=chunks))


@derived_from(np)
def tril_indices_from(arr, k=0):
    if arr.ndim != 2:
        raise ValueError("input array must be 2-d")
    return tril_indices(arr.shape[-2], k=k, m=arr.shape[-1], chunks=arr.chunks)


@derived_from(np)
def triu_indices(n, k=0, m=None, chunks="auto"):
    from dask.array._array_expr._creation import tri

    return nonzero(~tri(n, m, k=k - 1, dtype=bool, chunks=chunks))


@derived_from(np)
def triu_indices_from(arr, k=0):
    if arr.ndim != 2:
        raise ValueError("input array must be 2-d")
    return triu_indices(arr.shape[-2], k=k, m=arr.shape[-1], chunks=arr.chunks)


@derived_from(np)
def digitize(a, bins, right=False):
    """Return the indices of the bins to which each value in input array belongs.

    Parameters
    ----------
    a : dask array
        Input array to be binned.
    bins : array_like
        Array of bins. Must be 1-dimensional and monotonic.
    right : bool, optional
        Indicating whether the intervals include the right or left bin edge.

    Returns
    -------
    indices : dask array of ints
        Output array of indices.
    """
    bins = np.asarray(bins)
    if bins.ndim != 1:
        raise ValueError("bins must be 1-dimensional")

    dtype = np.digitize(np.asarray([0], like=bins), bins, right=right).dtype
    return elemwise(np.digitize, a, dtype=dtype, bins=bins, right=right)


def _variadic_choose(a, *choices):
    return np.choose(a, choices)


@derived_from(np)
def choose(a, choices):
    a = asarray(a)
    choices = [asarray(c) for c in choices]
    return elemwise(_variadic_choose, a, *choices)


@derived_from(np)
def extract(condition, arr):
    condition = asarray(condition).astype(bool)
    arr = asarray(arr)
    return compress(condition.ravel(), arr.ravel())


def _int_piecewise(x, *condlist, funclist=None, func_args=(), func_kw=None):
    return np.piecewise(x, list(condlist), funclist, *func_args, **(func_kw or {}))


@derived_from(np)
def piecewise(x, condlist, funclist, *args, **kw):
    x = asarray(x)
    return elemwise(
        _int_piecewise,
        x,
        *condlist,
        dtype=x.dtype,
        name="piecewise",
        funclist=funclist,
        func_args=args,
        func_kw=kw,
    )


def _select(*args, **kwargs):
    split_at = len(args) // 2
    condlist = args[:split_at]
    choicelist = args[split_at:]
    return np.select(condlist, choicelist, **kwargs)


@derived_from(np)
def select(condlist, choicelist, default=0):
    from dask.array._array_expr._collection import blockwise

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
        default=default,
    )


def _isin_kernel(element, test_elements, assume_unique=False):
    values = np.isin(element.ravel(), test_elements, assume_unique=assume_unique)
    return values.reshape(element.shape + (1,) * test_elements.ndim)


@derived_from(np)
def isin(element, test_elements, assume_unique=False, invert=False):
    from dask.array._array_expr._collection import blockwise

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
def argwhere(a):
    """Find the indices of array elements that are non-zero."""
    from dask.array._array_expr._creation import indices

    a = asarray(a)

    nz = isnonzero(a).flatten()

    ind = indices(a.shape, dtype=np.intp, chunks=a.chunks)
    if ind.ndim > 1:
        ind = stack([ind[i].ravel() for i in range(len(ind))], axis=1)
    ind = compress(nz, ind, axis=0)

    return ind


@derived_from(np)
def flatnonzero(a):
    """Return indices that are non-zero in the flattened array."""
    return argwhere(asarray(a).ravel())[:, 0]


@derived_from(np)
def nonzero(a):
    """Return the indices of the elements that are non-zero."""
    ind = argwhere(a)
    if ind.ndim > 1:
        return tuple(ind[:, i] for i in range(ind.shape[1]))
    else:
        return (ind,)


def _bincount_chunk(x, weights, minlength):
    """Apply bincount to a single chunk, wrapping result in extra dimension."""
    if weights is not None:
        result = np.bincount(x, weights=weights, minlength=minlength)
    else:
        result = np.bincount(x, minlength=minlength)
    # Add outer dimension for stacking/summing
    return result[np.newaxis]


def _bincount_sum(bincounts, axis, keepdims, dtype=None):
    """Sum bincount results, handling variable lengths when minlength=0."""
    # bincounts is a list of 2D arrays, each with shape (1, varying_length)
    if not isinstance(bincounts, list):
        bincounts = [bincounts]

    # Handle variable-length outputs when minlength=0
    n = max(b.shape[1] for b in bincounts)
    out = np.zeros((1, n), dtype=bincounts[0].dtype)
    for b in bincounts:
        out[0, :b.shape[1]] += b[0]

    if not keepdims:
        return out[0]
    return out


class BincountChunked(ArrayExpr):
    """Expression for per-chunk bincount computation.

    Creates a 2D array of shape (nchunks, output_size) where each row
    is the bincount of one input chunk. Use .sum(axis=0) or tree reduce
    to get the final result.
    """

    _parameters = ["x", "weights", "minlength", "output_size", "meta_provided"]
    _defaults = {"weights": None}

    @cached_property
    def _meta(self):
        return np.empty((0, 0), dtype=self.meta_provided.dtype)

    @cached_property
    def chunks(self):
        nchunks = len(self.x.chunks[0])
        return ((1,) * nchunks, (self.output_size,))

    @cached_property
    def _name(self):
        return f"bincount-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        minlen = self.minlength
        for i in range(len(self.x.chunks[0])):
            key = (self._name, i, 0)
            x_ref = TaskRef((self.x._name, i))
            w_ref = TaskRef((self.weights._name, i)) if self.weights is not None else None
            dsk[key] = Task(key, _bincount_chunk, x_ref, w_ref, minlen)
        return dsk

    @property
    def _dependencies(self):
        deps = [self.x]
        if self.weights is not None:
            deps.append(self.weights)
        return deps


@derived_from(np)
def bincount(x, weights=None, minlength=0, split_every=None):
    """Count number of occurrences of each value in array of non-negative ints."""
    from dask.array._array_expr._reductions import _tree_reduce

    x = asarray(x)
    if x.ndim != 1:
        raise ValueError("Input array must be one dimensional. Try using x.ravel()")
    if weights is not None:
        weights = asarray(weights)
        if weights.chunks != x.chunks:
            raise ValueError("Chunks of input array x and weights must match.")

    if weights is not None:
        meta = np.bincount([1], weights=np.array([1], dtype=weights.dtype))
    else:
        meta = np.bincount([])

    if minlength == 0:
        output_size = np.nan
    else:
        output_size = minlength

    chunked_counts = new_collection(BincountChunked(x, weights, minlength, output_size, meta_provided=meta))

    # Use sum along axis 0 to combine chunk results
    # For minlength>0 this works directly; for minlength=0 we need tree reduce
    if minlength > 0:
        return chunked_counts.sum(axis=0)
    else:
        # For unknown output size, use tree reduce with custom aggregation
        return _tree_reduce(
            chunked_counts,
            aggregate=partial(_bincount_sum, dtype=meta.dtype),
            axis=(0,),
            keepdims=False,
            dtype=meta.dtype,
            split_every=split_every,
            concatenate=False,
        )


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
    from dask.array._array_expr._linalg import dot
    from dask.array._array_expr._ufunc import true_divide

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
    from dask.array._array_expr._creation import diag
    from dask.array._array_expr._ufunc import sqrt

    c = cov(x, y, rowvar)

    if c.shape == ():
        return c / c
    d = diag(c)
    d = d.reshape((d.shape[0], 1))
    sqr_d = sqrt(d)
    return (c / sqr_d) / sqr_d.T


def _unravel_index_kernel(indices, func_kwargs):
    return np.stack(np.unravel_index(indices, **func_kwargs))


@derived_from(np)
def unravel_index(indices, shape, order="C"):
    from dask.array._array_expr._creation import empty

    indices = asarray(indices)
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


@derived_from(np)
def ravel_multi_index(multi_index, dims, mode="raise", order="C"):
    if np.isscalar(dims):
        dims = (dims,)
    if is_dask_collection(dims) or any(is_dask_collection(d) for d in dims):
        raise NotImplementedError(
            f"Dask types are not supported in the `dims` argument: {dims!r}"
        )

    if hasattr(multi_index, "ndim") and multi_index.ndim > 0:
        # It's an array-like
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


def _partition(n, size):
    """Partition n into evenly distributed sizes, returning quotients and remainder.

    Examples
    --------
    >>> _partition(10, 3)
    ([3, 3, 3], [1])
    >>> _partition(9, 3)
    ([3, 3, 3], [])
    >>> _partition(7, 3)
    ([3, 3], [1])
    """
    quotient, remainder = divmod(n, size)
    return [size] * quotient, [remainder] if remainder else []


def aligned_coarsen_chunks(chunks, multiple):
    """Returns a new chunking aligned with the coarsening multiple.

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
    chunks = np.asarray(chunks)
    overflow = chunks % multiple
    excess = overflow.sum()
    new_chunks = chunks - overflow
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


class Coarsen(ArrayExpr):
    """Expression class for coarsen operation.

    x should already be rechunked to align with coarsening factors.
    """

    _parameters = ["x", "reduction", "axes", "trim_excess", "kwargs"]
    _defaults = {"trim_excess": False, "kwargs": None}

    @cached_property
    def _reduction(self):
        """Get the numpy reduction function."""
        reduction = self.reduction
        if reduction.__module__.startswith("dask."):
            return getattr(np, reduction.__name__)
        return reduction

    @cached_property
    def _kwargs(self):
        return self.kwargs or {}

    @cached_property
    def _meta(self):
        x = self.x
        meta = self._reduction(np.empty((1,) * x.ndim, dtype=x.dtype), **self._kwargs)
        return meta_from_array(meta, ndim=x.ndim)

    @cached_property
    def chunks(self):
        x = self.x
        axes = self.axes
        coarsen_dim = lambda dim, ax: int(dim // axes.get(ax, 1))
        return tuple(
            tuple(coarsen_dim(bd, i) for bd in bds if coarsen_dim(bd, i) > 0)
            for i, bds in enumerate(x.chunks)
        )

    def _layer(self):
        from dask.array import chunk

        x = self.x
        axes = self.axes

        name = self._name
        dsk = {}

        # Create one task for each input block (matching traditional implementation)
        # The chunks property will filter out 0-sized outputs
        in_ranges = [range(len(c)) for c in x.chunks]

        for in_idx in itertools.product(*in_ranges):
            in_key = (x._name,) + in_idx
            out_key = (name,) + in_idx

            # Use partial to bind all non-data arguments
            func = partial(
                chunk.coarsen,
                self._reduction,
                axes=axes,
                trim_excess=self.trim_excess,
                **self._kwargs,
            )
            dsk[out_key] = Task(out_key, func, TaskRef(in_key))

        return dsk


def coarsen(reduction, x, axes, trim_excess=False, **kwargs):
    """Coarsen array by applying reduction to fixed size neighborhoods.

    Parameters
    ----------
    reduction : callable
        Function like np.sum, np.mean, etc.
    x : dask array
        Array to be coarsened.
    axes : dict
        Mapping of axis to coarsening factor.
    trim_excess : bool, optional
        If True, trim any excess elements that don't fit evenly.
    **kwargs
        Additional arguments passed to the reduction function.

    Returns
    -------
    dask array
        Coarsened array.
    """
    x = asarray(x)

    if not trim_excess and not all(x.shape[i] % div == 0 for i, div in axes.items()):
        msg = f"Coarsening factors {axes} do not align with array shape {x.shape}."
        raise ValueError(msg)

    # Rechunk to align with coarsening factors before creating expression
    new_chunks = {}
    for i, div in axes.items():
        aligned = aligned_coarsen_chunks(x.chunks[i], div)
        if aligned != x.chunks[i]:
            new_chunks[i] = aligned
    if new_chunks:
        x = x.rechunk(new_chunks)

    return new_collection(Coarsen(x, reduction, axes, trim_excess, kwargs or None))


@derived_from(np)
def ediff1d(ary, to_end=None, to_begin=None):
    """Compute the differences between consecutive elements of an array."""
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


def _unique_internal(ar, indices, counts, return_inverse=False):
    """
    Helper/wrapper function for np.unique.

    Uses np.unique to find the unique values for the array chunk.
    Given this chunk may not represent the whole array, also take the
    indices and counts that are in 1-to-1 correspondence to ar
    and reduce them in the same fashion as ar is reduced. Namely sum
    any counts that correspond to the same value and take the smallest
    index that corresponds to the same value.

    To handle the inverse mapping from the unique values to the original
    array, simply return a NumPy array created with arange with enough
    values to correspond 1-to-1 to the unique values. While there is more
    work needed to be done to create the full inverse mapping for the
    original array, this provides enough information to generate the
    inverse mapping in Dask.

    Given Dask likes to have one array returned from functions like
    blockwise, some formatting is done to stuff all of the resulting arrays
    into one big NumPy structured array. Dask is then able to handle this
    object and can split it apart into the separate results on the Dask side,
    which then can be passed back to this function in concatenated chunks for
    further reduction or can be return to the user to perform other forms of
    analysis.
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
    """Apply a function repeatedly over multiple axes."""
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


class UniqueChunked(ArrayExpr):
    """Expression for per-chunk unique computation.

    Creates a 1D structured array of unique values (and optionally indices/counts)
    for each chunk. Use UniqueAggregate to combine chunk results.
    """

    _parameters = ["x", "indices", "counts", "out_dtype"]
    _defaults = {"indices": None, "counts": None}

    @cached_property
    def _meta(self):
        return np.empty((0,), dtype=self.out_dtype)

    @cached_property
    def chunks(self):
        # Unknown chunk sizes since unique output length varies
        nchunks = len(self.x.chunks[0])
        return ((np.nan,) * nchunks,)

    @cached_property
    def _name(self):
        return f"unique-chunk-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        for i in range(len(self.x.chunks[0])):
            key = (self._name, i)
            x_ref = TaskRef((self.x._name, i))
            idx_ref = TaskRef((self.indices._name, i)) if self.indices is not None else None
            cnt_ref = TaskRef((self.counts._name, i)) if self.counts is not None else None
            dsk[key] = Task(key, _unique_internal, x_ref, idx_ref, cnt_ref, False)
        return dsk

    @property
    def _dependencies(self):
        deps = [self.x]
        if self.indices is not None:
            deps.append(self.indices)
        if self.counts is not None:
            deps.append(self.counts)
        return deps


class UniqueAggregate(ArrayExpr):
    """Expression for aggregating unique results from all chunks.

    Concatenates all chunk results and runs _unique_internal to get final unique values.
    """

    _parameters = ["chunked", "return_inverse", "out_dtype"]
    _defaults = {"return_inverse": False}

    @cached_property
    def _meta(self):
        return np.empty((0,), dtype=self.out_dtype)

    @cached_property
    def chunks(self):
        return ((np.nan,),)

    @cached_property
    def _name(self):
        return f"unique-aggregate-{self.deterministic_token}"

    def _layer(self):
        # Gather all chunk keys and concatenate + aggregate in one task
        chunk_keys = [(self.chunked._name, i) for i in range(len(self.chunked.chunks[0]))]
        key = (self._name, 0)
        # Use List from task_spec to wrap TaskRefs so they get resolved
        chunks_list = List(*[TaskRef(k) for k in chunk_keys])
        dsk = {
            key: Task(
                key,
                _unique_aggregate_func,
                chunks_list,
                self.return_inverse,
            )
        }
        return dsk

    @property
    def _dependencies(self):
        return [self.chunked]


def _unique_aggregate_func(chunks, return_inverse):
    """Aggregate unique results from multiple chunks."""
    # Concatenate all structured arrays
    combined = np.concatenate(chunks)

    # Run unique_internal on the combined result
    return_index = "indices" in combined.dtype.names
    return_counts = "counts" in combined.dtype.names

    return _unique_internal(
        combined["values"],
        combined["indices"] if return_index else None,
        combined["counts"] if return_counts else None,
        return_inverse=return_inverse,
    )


def unique_no_structured_arr(
    ar, return_index=False, return_inverse=False, return_counts=False
):
    """Simplified version of unique for array types that don't support structured arrays."""
    from dask.array._array_expr._blockwise import Blockwise
    from dask.array._array_expr._reductions import _tree_reduce

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

    ar = ravel(ar)

    # Per-chunk unique via blockwise
    out = Blockwise(
        np.unique,
        "i",
        ar,
        "i",
        dtype=ar.dtype,
    )
    chunked = new_collection(out)
    # Override chunks to unknown
    from dask.array._array_expr._expr import ChunksOverride
    chunked = new_collection(ChunksOverride(chunked.expr, ((np.nan,) * len(ar.chunks[0]),)))

    def _unique_agg(arrays, axis, keepdims):
        if not isinstance(arrays, list):
            arrays = [arrays]
        return np.unique(np.concatenate(arrays))

    result = _tree_reduce(
        chunked.expr,
        aggregate=_unique_agg,
        axis=(0,),
        keepdims=False,
        dtype=ar.dtype,
        concatenate=False,
    )
    return result


@derived_from(np)
def unique(ar, return_index=False, return_inverse=False, return_counts=False):
    """Find the unique elements of an array."""
    from dask.array._array_expr._creation import arange, ones
    from dask.array.numpy_compat import NUMPY_GE_200

    # Test whether the downstream library supports structured arrays
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
    ar = ravel(ar)

    # Build output dtype and prepare indices/counts arrays
    out_dtype = [("values", ar.dtype)]
    indices_arr = None
    counts_arr = None

    if return_index:
        indices_arr = arange(ar.shape[0], dtype=np.intp, chunks=ar.chunks[0])
        out_dtype.append(("indices", np.intp))
    if return_counts:
        counts_arr = ones((ar.shape[0],), dtype=np.intp, chunks=ar.chunks[0])
        out_dtype.append(("counts", np.intp))

    out_dtype = np.dtype(out_dtype)

    # Create chunked unique expression
    chunked = UniqueChunked(
        ar.expr,
        indices_arr.expr if indices_arr is not None else None,
        counts_arr.expr if counts_arr is not None else None,
        out_dtype,
    )

    # Build final dtype (with inverse field if requested)
    final_dtype = out_dtype if not return_inverse else np.dtype(list(out_dtype.descr) + [("inverse", np.intp)])

    # Aggregate all chunks into final result
    aggregated = new_collection(UniqueAggregate(chunked, return_inverse, final_dtype))

    # Extract results from structured array
    result = [aggregated["values"]]
    if return_index:
        result.append(aggregated["indices"])
    if return_inverse:
        # Compute inverse: broadcast compare ar with unique values, multiply by inverse indices, sum
        matches = (ar[:, None] == aggregated["values"][None, :]).astype(np.intp)
        inverse = (matches * aggregated["inverse"]).sum(axis=1)
        if NUMPY_GE_200:
            from dask.array._array_expr._reshape import reshape
            inverse = reshape(inverse, orig_shape)
        result.append(inverse)
    if return_counts:
        result.append(aggregated["counts"])

    if len(result) == 1:
        return result[0]
    else:
        return tuple(result)


@derived_from(np)
def union1d(ar1, ar2):
    """Find the union of two arrays."""
    ar1 = asarray(ar1)
    ar2 = asarray(ar2)
    return unique(concatenate((ravel(ar1), ravel(ar2))))


def topk(a, k, axis=-1, split_every=None):
    """Extract the k largest elements from a on the given axis.

    Returns them sorted from largest to smallest. If k is negative,
    extract the -k smallest elements instead, and return them sorted
    from smallest to largest.

    Parameters
    ----------
    a : Array
        Data being sorted
    k : int
        Number of elements to extract
    axis : int, optional
        Axis along which to find topk elements
    split_every : int >= 2, optional
        Controls depth of recursive aggregation

    Returns
    -------
    Array with size abs(k) along the given axis
    """
    from dask.array import chunk
    from dask.array._array_expr._reductions import reduction

    a = asarray(a)
    axis = validate_axis(axis, a.ndim)

    chunk_combine = partial(chunk.topk, k=k)
    aggregate = partial(chunk.topk_aggregate, k=k)

    return reduction(
        a,
        chunk=chunk_combine,
        combine=chunk_combine,
        aggregate=aggregate,
        axis=axis,
        keepdims=True,
        dtype=a.dtype,
        split_every=split_every,
        output_size=abs(k),
    )


def argtopk(a, k, axis=-1, split_every=None):
    """Extract the indices of the k largest elements from a on the given axis.

    Returns them sorted from largest to smallest. If k is negative,
    extract the indices of the -k smallest elements instead, and return
    them sorted from smallest to largest.

    Parameters
    ----------
    a : Array
        Data being sorted
    k : int
        Number of elements to extract
    axis : int, optional
        Axis along which to find topk elements
    split_every : int >= 2, optional
        Controls depth of recursive aggregation

    Returns
    -------
    Array of np.intp indices with size abs(k) along the given axis
    """
    from dask.array import chunk
    from dask.array._array_expr._creation import arange
    from dask.array._array_expr._reductions import reduction

    a = asarray(a)
    axis = validate_axis(axis, a.ndim)

    # Generate nodes where every chunk is a tuple of (a, original index of a)
    idx = arange(a.shape[axis], chunks=(a.chunks[axis],), dtype=np.intp)
    idx = idx[tuple(slice(None) if i == axis else np.newaxis for i in range(a.ndim))]
    a_plus_idx = a.map_blocks(chunk.argtopk_preprocess, idx, dtype=object)

    chunk_combine = partial(chunk.argtopk, k=k)
    aggregate = partial(chunk.argtopk_aggregate, k=k)

    if isinstance(axis, Number):
        naxis = 1
    else:
        naxis = len(axis)

    meta = a._meta.astype(np.intp).reshape((0,) * (a.ndim - naxis + 1))

    return reduction(
        a_plus_idx,
        chunk=chunk_combine,
        combine=chunk_combine,
        aggregate=aggregate,
        axis=axis,
        keepdims=True,
        dtype=np.intp,
        split_every=split_every,
        concatenate=False,
        output_size=abs(k),
        meta=meta,
    )

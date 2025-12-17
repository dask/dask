from __future__ import annotations

import math
from numbers import Integral, Real

import numpy as np

from dask.array._array_expr._collection import Array, asarray
from dask.array._array_expr._overlap import map_overlap
from dask.array.utils import validate_axis
from dask.base import is_dask_collection


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

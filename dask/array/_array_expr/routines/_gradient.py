"""Gradient implementation for array-expr."""

from __future__ import annotations

import math
from numbers import Integral, Real

import numpy as np

from dask.array._array_expr._collection import asarray
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

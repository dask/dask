"""Search functions for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asarray
from dask.utils import derived_from


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
    from dask.array.utils import array_safe, meta_from_array

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

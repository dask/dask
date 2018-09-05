""" A set of NumPy functions to apply per chunk """
from __future__ import absolute_import, division, print_function

from functools import wraps

from toolz import concat
import numpy as np
from . import numpy_compat as npcompat

from ..compatibility import getargspec, Container, Iterable, Sequence
from ..core import flatten
from ..utils import ignoring

try:
    from numpy import broadcast_to
except ImportError:  # pragma: no cover
    broadcast_to = npcompat.broadcast_to

try:
    from numpy import take_along_axis
except ImportError:  # pragma: no cover
    take_along_axis = npcompat.take_along_axis


def keepdims_wrapper(a_callable):
    """
    A wrapper for functions that don't provide keepdims to ensure that they do.
    """

    if "keepdims" in getargspec(a_callable).args:
        return a_callable

    @wraps(a_callable)
    def keepdims_wrapped_callable(x, axis=None, keepdims=None, *args, **kwargs):
        r = a_callable(x, axis=axis, *args, **kwargs)

        if not keepdims:
            return r

        axes = axis

        if axes is None:
            axes = range(x.ndim)

        if not isinstance(axes, (Container, Iterable, Sequence)):
            axes = [axes]

        r_slice = tuple()
        for each_axis in range(x.ndim):
            if each_axis in axes:
                r_slice += (None,)
            else:
                r_slice += (slice(None),)

        r = r[r_slice]

        return r

    return keepdims_wrapped_callable


# Wrap NumPy functions to ensure they provide keepdims.
sum = keepdims_wrapper(np.sum)
prod = keepdims_wrapper(np.prod)
min = keepdims_wrapper(np.min)
max = keepdims_wrapper(np.max)
argmin = keepdims_wrapper(np.argmin)
nanargmin = keepdims_wrapper(np.nanargmin)
argmax = keepdims_wrapper(np.argmax)
nanargmax = keepdims_wrapper(np.nanargmax)
any = keepdims_wrapper(np.any)
all = keepdims_wrapper(np.all)
nansum = keepdims_wrapper(np.nansum)
nanprod = keepdims_wrapper(np.nanprod)

try:
    from numpy import nancumprod, nancumsum
except ImportError:  # pragma: no cover
    nancumprod = npcompat.nancumprod
    nancumsum = npcompat.nancumsum

nancumprod = keepdims_wrapper(nancumprod)
nancumsum = keepdims_wrapper(nancumsum)

nanmin = keepdims_wrapper(np.nanmin)
nanmax = keepdims_wrapper(np.nanmax)
mean = keepdims_wrapper(np.mean)

with ignoring(AttributeError):
    nanmean = keepdims_wrapper(np.nanmean)

var = keepdims_wrapper(np.var)

with ignoring(AttributeError):
    nanvar = keepdims_wrapper(np.nanvar)

std = keepdims_wrapper(np.std)

with ignoring(AttributeError):
    nanstd = keepdims_wrapper(np.nanstd)


def coarsen(reduction, x, axes, trim_excess=False):
    """ Coarsen array by applying reduction to fixed size neighborhoods

    Parameters
    ----------
    reduction: function
        Function like np.sum, np.mean, etc...
    x: np.ndarray
        Array to be coarsened
    axes: dict
        Mapping of axis to coarsening factor

    Examples
    --------
    >>> x = np.array([1, 2, 3, 4, 5, 6])
    >>> coarsen(np.sum, x, {0: 2})
    array([ 3,  7, 11])
    >>> coarsen(np.max, x, {0: 3})
    array([3, 6])

    Provide dictionary of scale per dimension

    >>> x = np.arange(24).reshape((4, 6))
    >>> x
    array([[ 0,  1,  2,  3,  4,  5],
           [ 6,  7,  8,  9, 10, 11],
           [12, 13, 14, 15, 16, 17],
           [18, 19, 20, 21, 22, 23]])

    >>> coarsen(np.min, x, {0: 2, 1: 3})
    array([[ 0,  3],
           [12, 15]])

    You must avoid excess elements explicitly

    >>> x = np.array([1, 2, 3, 4, 5, 6, 7, 8])
    >>> coarsen(np.min, x, {0: 3}, trim_excess=True)
    array([1, 4])
    """
    # Insert singleton dimensions if they don't exist already
    for i in range(x.ndim):
        if i not in axes:
            axes[i] = 1

    if trim_excess:
        ind = tuple(slice(0, -(d % axes[i]))
                    if d % axes[i] else
                    slice(None, None) for i, d in enumerate(x.shape))
        x = x[ind]

    # (10, 10) -> (5, 2, 5, 2)
    newshape = tuple(concat([(x.shape[i] // axes[i], axes[i])
                             for i in range(x.ndim)]))

    return reduction(x.reshape(newshape), axis=tuple(range(1, x.ndim * 2, 2)))


def trim(x, axes=None, boundary=None, block_info=None):
    """ Trim boundaries off of array

    >>> x = np.arange(24).reshape((4, 6))
    >>> trim(x, axes={0: 0, 1: 1})
    array([[ 1,  2,  3,  4],
           [ 7,  8,  9, 10],
           [13, 14, 15, 16],
           [19, 20, 21, 22]])

    >>> trim(x, axes={0: 1, 1: 1})
    array([[ 7,  8,  9, 10],
           [13, 14, 15, 16]])
    """
    if isinstance(axes, int):
        axes = [axes] * x.ndim
    if isinstance(axes, dict):
        axes = [axes.get(i, 0) for i in range(x.ndim)]

    if boundary is not None:
        if block_info is None:
            raise ValueError('``block_info`` must be provided if '
                             'boundary condition is provided.')
        if isinstance(boundary, str):
            boundary = dict(zip(range(len(x.ndim)), boundary))

        trim_front = [
            False if (chunk_location == 0 and bdy == 'none') else True
            for bdy, chunk_location in zip(
                boundary, block_info[0]['chunk-location'])]
        trim_back = [
            False if (chunk_location != chunks-1 and bdy == 'none') else True
            for bdy, chunks, chunk_location in zip(
                boundary,
                block_info[0]['num-chunks'],
                block_info[0]['chunk-location'])]
    else:
        trim_front = [True] * len(axes)
        trim_back = [True] * len(axes)
    return x[tuple(slice(ax if front else 0, -ax if back and ax else None)
                   for ax, front, back in zip(axes, trim_front, trim_back))]


def topk(a, k, axis, keepdims):
    """ Chunk and combine function of topk

    Extract the k largest elements from a on the given axis.
    If k is negative, extract the -k smallest elements instead.
    Note that, unlike in the parent function, the returned elements
    are not sorted internally.
    """
    assert keepdims is True
    axis = axis[0]
    if abs(k) >= a.shape[axis]:
        return a

    a = np.partition(a, -k, axis=axis)
    k_slice = slice(-k, None) if k > 0 else slice(-k)
    return a[tuple(k_slice if i == axis else slice(None)
                   for i in range(a.ndim))]


def topk_aggregate(a, k, axis, keepdims):
    """ Final aggregation function of topk

    Invoke topk one final time and then sort the results internally.
    """
    assert keepdims is True
    a = topk(a, k, axis, keepdims)
    axis = axis[0]
    a = np.sort(a, axis=axis)
    if k < 0:
        return a
    return a[tuple(slice(None, None, -1) if i == axis else slice(None)
                   for i in range(a.ndim))]


def argtopk_preprocess(a, idx):
    """ Preparatory step for argtopk

    Put data together with its original indices in a tuple.
    """
    return a, idx


def argtopk(a_plus_idx, k, axis, keepdims):
    """ Chunk and combine function of argtopk

    Extract the indices of the k largest elements from a on the given axis.
    If k is negative, extract the indices of the -k smallest elements instead.
    Note that, unlike in the parent function, the returned elements
    are not sorted internally.
    """
    assert keepdims is True
    axis = axis[0]

    if isinstance(a_plus_idx, list):
        a_plus_idx = list(flatten(a_plus_idx))
        a = np.concatenate([ai for ai, _ in a_plus_idx], axis)
        idx = np.concatenate([broadcast_to(idxi, ai.shape)
                              for ai, idxi in a_plus_idx], axis)
    else:
        a, idx = a_plus_idx

    if abs(k) >= a.shape[axis]:
        return a_plus_idx

    idx2 = np.argpartition(a, -k, axis=axis)
    k_slice = slice(-k, None) if k > 0 else slice(-k)
    idx2 = idx2[tuple(k_slice if i == axis else slice(None)
                      for i in range(a.ndim))]
    return take_along_axis(a, idx2, axis), take_along_axis(idx, idx2, axis)


def argtopk_aggregate(a_plus_idx, k, axis, keepdims):
    """ Final aggregation function of argtopk

    Invoke argtopk one final time, sort the results internally, drop the data
    and return the index only.
    """
    assert keepdims is True
    a, idx = argtopk(a_plus_idx, k, axis, keepdims)
    axis = axis[0]

    idx2 = np.argsort(a, axis=axis)
    idx = take_along_axis(idx, idx2, axis)
    if k < 0:
        return idx
    return idx[tuple(slice(None, None, -1) if i == axis else slice(None)
                     for i in range(idx.ndim))]


def arange(start, stop, step, length, dtype):
    res = np.arange(start, stop, step, dtype)
    return res[:-1] if len(res) > length else res


def astype(x, astype_dtype=None, **kwargs):
    return x.astype(astype_dtype, **kwargs)


def view(x, dtype, order='C'):
    if order == 'C':
        x = np.ascontiguousarray(x)
        return x.view(dtype)
    else:
        x = np.asfortranarray(x)
        return x.T.view(dtype).T


def einsum(*operands, **kwargs):
    subscripts = kwargs.pop('subscripts')
    ncontract_inds = kwargs.pop('ncontract_inds')
    dtype = kwargs.pop('kernel_dtype')
    chunk = np.einsum(subscripts, *operands, dtype=dtype, **kwargs)

    # Avoid concatenate=True in atop by adding 1's
    # for the contracted dimensions
    return chunk.reshape(chunk.shape + (1,) * ncontract_inds)


def slice_with_int_dask_array(x, idx, offset, x_size, axis):
    """ Chunk function of `slice_with_int_dask_array_on_axis`.
    Slice one chunk of x by one chunk of idx.

    Parameters
    ----------
    x: ndarray, any dtype, any shape
        i-th chunk of x
    idx: ndarray, ndim=1, dtype=any integer
        j-th chunk of idx (cartesian product with the chunks of x)
    offset: ndarray, shape=(1, ), dtype=int64
        Index of the first element along axis of the current chunk of x
    x_size: int
        Total size of the x da.Array along axis
    axis: int
        normalized axis to take elements from (0 <= axis < x.ndim)

    Returns
    -------
    x sliced along axis, using only the elements of idx that fall inside the
    current chunk.
    """
    # Needed when idx is unsigned
    idx = idx.astype(np.int64)

    # Normalize negative indices
    idx = np.where(idx < 0, idx + x_size, idx)

    # A chunk of the offset dask Array is a numpy array with shape (1, ).
    # It indicates the index of the first element along axis of the current
    # chunk of x.
    idx = idx - offset

    # Drop elements of idx that do not fall inside the current chunk of x
    idx_filter = (idx >= 0) & (idx < x.shape[axis])
    idx = idx[idx_filter]

    # np.take does not support slice indices
    # return np.take(x, idx, axis)
    return x[tuple(
        idx if i == axis else slice(None)
        for i in range(x.ndim)
    )]


def slice_with_int_dask_array_aggregate(idx, chunk_outputs, x_chunks, axis):
    """ Final aggregation function of `slice_with_int_dask_array_on_axis`.
    Aggregate all chunks of x by one chunk of idx, reordering the output of
    `slice_with_int_dask_array`.

    Note that there is no combine function, as a recursive aggregation (e.g.
    with split_every) would not give any benefit.

    Parameters
    ----------
    idx: ndarray, ndim=1, dtype=any integer
        j-th chunk of idx
    chunk_outputs: ndarray
        concatenation along axis of the outputs of `slice_with_int_dask_array`
        for all chunks of x and the j-th chunk of idx
    x_chunks: tuple
        dask chunks of the x da.Array along axis, e.g. ``(3, 3, 2)``
    axis: int
        normalized axis to take elements from (0 <= axis < x.ndim)

    Returns
    -------
    Selection from all chunks of x for the j-th chunk of idx, in the correct
    order
    """
    # Needed when idx is unsigned
    idx = idx.astype(np.int64)

    # Normalize negative indices
    idx = np.where(idx < 0, idx + sum(x_chunks), idx)

    x_chunk_offset = 0
    chunk_output_offset = 0

    # Assemble the final index that picks from the output of the previous
    # kernel by adding together one layer per chunk of x
    # FIXME: this could probably be reimplemented with a faster search-based
    # algorithm
    idx_final = np.zeros_like(idx)
    for x_chunk in x_chunks:
        idx_filter = (idx >= x_chunk_offset) & (idx < x_chunk_offset + x_chunk)
        idx_cum = np.cumsum(idx_filter)
        idx_final += np.where(idx_filter, idx_cum - 1 + chunk_output_offset, 0)
        x_chunk_offset += x_chunk
        if idx_cum.size > 0:
            chunk_output_offset += idx_cum[-1]

    # np.take does not support slice indices
    # return np.take(chunk_outputs, idx_final, axis)
    return chunk_outputs[tuple(
        idx_final if i == axis else slice(None)
        for i in range(chunk_outputs.ndim)
    )]

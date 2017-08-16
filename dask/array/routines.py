from __future__ import division, print_function, absolute_import

import inspect
import warnings
from collections import Iterable
from functools import wraps, partial
from itertools import product
from numbers import Integral
from operator import getitem

import numpy as np
from toolz import concat, sliding_window, interleave

from .. import sharedict
from ..core import flatten
from ..base import tokenize
from ..utils import package_of
from . import numpy_compat, chunk

from .core import (Array, map_blocks, elemwise, from_array, asarray,
                   concatenate, stack, atop, broadcast_shapes,
                   is_scalar_for_elemwise, broadcast_to)


@wraps(np.array)
def array(x, dtype=None, ndmin=None):
    while ndmin is not None and x.ndim < ndmin:
        x = x[None, :]
    if dtype is not None and x.dtype != dtype:
        x = x.astype(dtype)
    return x


@wraps(np.result_type)
def result_type(*args):
    args = [a if is_scalar_for_elemwise(a) else a.dtype for a in args]
    return np.result_type(*args)


def atleast_3d(x):
    if x.ndim == 1:
        return x[None, :, None]
    elif x.ndim == 2:
        return x[:, :, None]
    elif x.ndim > 2:
        return x
    else:
        raise NotImplementedError()


def atleast_2d(x):
    if x.ndim == 1:
        return x[None, :]
    elif x.ndim > 1:
        return x
    else:
        raise NotImplementedError()


@wraps(np.vstack)
def vstack(tup):
    tup = tuple(atleast_2d(x) for x in tup)
    return concatenate(tup, axis=0)


@wraps(np.hstack)
def hstack(tup):
    if all(x.ndim == 1 for x in tup):
        return concatenate(tup, axis=0)
    else:
        return concatenate(tup, axis=1)


@wraps(np.dstack)
def dstack(tup):
    tup = tuple(atleast_3d(x) for x in tup)
    return concatenate(tup, axis=2)


@wraps(np.swapaxes)
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

    return atop(np.swapaxes, out, a, ind, axis1=axis1, axis2=axis2,
                dtype=a.dtype)


@wraps(np.transpose)
def transpose(a, axes=None):
    if axes:
        if len(axes) != a.ndim:
            raise ValueError("axes don't match array")
    else:
        axes = tuple(range(a.ndim))[::-1]
    axes = tuple(d + a.ndim if d < 0 else d for d in axes)
    return atop(np.transpose, axes, a, tuple(range(a.ndim)),
                dtype=a.dtype, axes=axes)


alphabet = 'abcdefghijklmnopqrstuvwxyz'
ALPHABET = alphabet.upper()


def _tensordot(a, b, axes):
    x = max([a, b], key=lambda x: x.__array_priority__)
    module = package_of(type(x)) or np
    x = module.tensordot(a, b, axes=axes)
    ind = [slice(None, None)] * x.ndim
    for a in sorted(axes[0]):
        ind.insert(a, None)
    x = x[tuple(ind)]
    return x


@wraps(np.tensordot)
def tensordot(lhs, rhs, axes=2):
    if isinstance(axes, Iterable):
        left_axes, right_axes = axes
    else:
        left_axes = tuple(range(lhs.ndim - 1, lhs.ndim - axes - 1, -1))
        right_axes = tuple(range(0, axes))

    if isinstance(left_axes, int):
        left_axes = (left_axes,)
    if isinstance(right_axes, int):
        right_axes = (right_axes,)
    if isinstance(left_axes, list):
        left_axes = tuple(left_axes)
    if isinstance(right_axes, list):
        right_axes = tuple(right_axes)

    dt = np.promote_types(lhs.dtype, rhs.dtype)

    left_index = list(alphabet[:lhs.ndim])
    right_index = list(ALPHABET[:rhs.ndim])
    out_index = left_index + right_index

    for l, r in zip(left_axes, right_axes):
        out_index.remove(right_index[r])
        right_index[r] = left_index[l]

    intermediate = atop(_tensordot, out_index,
                        lhs, left_index,
                        rhs, right_index, dtype=dt,
                        axes=(left_axes, right_axes))

    result = intermediate.sum(axis=left_axes)
    return result


@wraps(np.dot)
def dot(a, b):
    return tensordot(a, b, axes=((a.ndim - 1,), (b.ndim - 2,)))


@wraps(np.diff)
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


@wraps(np.ediff1d)
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


@wraps(np.bincount)
def bincount(x, weights=None, minlength=None):
    if minlength is None:
        raise TypeError("Must specify minlength argument in da.bincount")
    assert x.ndim == 1
    if weights is not None:
        assert weights.chunks == x.chunks

    # Call np.bincount on each block, possibly with weights
    token = tokenize(x, weights, minlength)
    name = 'bincount-' + token
    if weights is not None:
        dsk = dict(((name, i),
                   (np.bincount, (x.name, i), (weights.name, i), minlength))
                   for i, _ in enumerate(x._keys()))
        dtype = np.bincount([1], weights=[1]).dtype
    else:
        dsk = dict(((name, i), (np.bincount, (x.name, i), None, minlength))
                   for i, _ in enumerate(x._keys()))
        dtype = np.bincount([]).dtype

    # Sum up all of the intermediate bincounts per block
    name = 'bincount-sum-' + token
    dsk[(name, 0)] = (np.sum, list(dsk), 0)

    chunks = ((minlength,),)

    dsk = sharedict.merge((name, dsk), x.dask)
    if weights is not None:
        dsk.update(weights.dask)

    return Array(dsk, name, chunks, dtype)


@wraps(np.digitize)
def digitize(a, bins, right=False):
    bins = np.asarray(bins)
    dtype = np.digitize([0], bins, right=False).dtype
    return a.map_blocks(np.digitize, dtype=dtype, bins=bins, right=right)


def histogram(a, bins=None, range=None, normed=False, weights=None, density=None):
    """
    Blocked variant of numpy.histogram.

    Follows the signature of numpy.histogram exactly with the following
    exceptions:

    - Either an iterable specifying the ``bins`` or the number of ``bins``
      and a ``range`` argument is required as computing ``min`` and ``max``
      over blocked arrays is an expensive operation that must be performed
      explicitly.

    - ``weights`` must be a dask.array.Array with the same block structure
      as ``a``.

    Examples
    --------
    Using number of bins and range:

    >>> import dask.array as da
    >>> import numpy as np
    >>> x = da.from_array(np.arange(10000), chunks=10)
    >>> h, bins = da.histogram(x, bins=10, range=[0, 10000])
    >>> bins
    array([     0.,   1000.,   2000.,   3000.,   4000.,   5000.,   6000.,
             7000.,   8000.,   9000.,  10000.])
    >>> h.compute()
    array([1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000])

    Explicitly specifying the bins:

    >>> h, bins = da.histogram(x, bins=np.array([0, 5000, 10000]))
    >>> bins
    array([    0,  5000, 10000])
    >>> h.compute()
    array([5000, 5000])
    """
    if bins is None or (range is None and bins is None):
        raise ValueError('dask.array.histogram requires either bins '
                         'or bins and range to be defined.')

    if weights is not None and weights.chunks != a.chunks:
        raise ValueError('Input array and weights must have the same '
                         'chunked structure')

    if not np.iterable(bins):
        bin_token = bins
        mn, mx = range
        if mn == mx:
            mn -= 0.5
            mx += 0.5

        bins = np.linspace(mn, mx, bins + 1, endpoint=True)
    else:
        bin_token = bins
    token = tokenize(a, bin_token, range, normed, weights, density)

    nchunks = len(list(flatten(a._keys())))
    chunks = ((1,) * nchunks, (len(bins) - 1,))

    name = 'histogram-sum-' + token

    # Map the histogram to all bins
    def block_hist(x, weights=None):
        return np.histogram(x, bins, weights=weights)[0][np.newaxis]

    if weights is None:
        dsk = dict(((name, i, 0), (block_hist, k))
                   for i, k in enumerate(flatten(a._keys())))
        dtype = np.histogram([])[0].dtype
    else:
        a_keys = flatten(a._keys())
        w_keys = flatten(weights._keys())
        dsk = dict(((name, i, 0), (block_hist, k, w))
                   for i, (k, w) in enumerate(zip(a_keys, w_keys)))
        dtype = weights.dtype

    all_dsk = sharedict.merge(a.dask, (name, dsk))
    if weights is not None:
        all_dsk.update(weights.dask)

    mapped = Array(all_dsk, name, chunks, dtype=dtype)
    n = mapped.sum(axis=0)

    # We need to replicate normed and density options from numpy
    if density is not None:
        if density:
            db = from_array(np.diff(bins).astype(float), chunks=n.chunks)
            return n / db / n.sum(), bins
        else:
            return n, bins
    else:
        # deprecated, will be removed from Numpy 2.0
        if normed:
            db = from_array(np.diff(bins).astype(float), chunks=n.chunks)
            return n / (n * db).sum(), bins
        else:
            return n, bins


@wraps(np.cov)
def cov(m, y=None, rowvar=1, bias=0, ddof=None):
    # This was copied almost verbatim from np.cov
    # See numpy license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    # or NUMPY_LICENSE.txt within this directory
    if ddof is not None and ddof != int(ddof):
        raise ValueError(
            "ddof must be integer")

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


@wraps(np.corrcoef)
def corrcoef(x, y=None, rowvar=1):

    from .ufunc import sqrt
    from .creation import diag

    c = cov(x, y, rowvar)
    if c.shape == ():
        return c / c
    d = diag(c)
    d = d.reshape((d.shape[0], 1))
    sqr_d = sqrt(d)
    return (c / sqr_d) / sqr_d.T


@wraps(np.round)
def round(a, decimals=0):
    return a.map_blocks(np.round, decimals=decimals, dtype=a.dtype)


@wraps(np.unique)
def unique(x):
    name = 'unique-' + x.name
    dsk = dict(((name, i), (np.unique, key)) for i, key in enumerate(x._keys()))
    parts = Array._get(sharedict.merge((name, dsk), x.dask), list(dsk.keys()))
    return np.unique(np.concatenate(parts))


@wraps(np.roll)
def roll(array, shift, axis=None):
    result = array

    if axis is None:
        result = ravel(result)

        if not isinstance(shift, Integral):
            raise TypeError(
                "Expect `shift` to be an instance of Integral"
                " when `axis` is None."
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


@wraps(np.ravel)
def ravel(array):
    return array.reshape((-1,))


@wraps(np.squeeze)
def squeeze(a, axis=None):
    if 1 not in a.shape:
        return a
    if axis is None:
        axis = tuple(i for i, d in enumerate(a.shape) if d == 1)
    b = a.map_blocks(partial(np.squeeze, axis=axis), dtype=a.dtype)
    chunks = tuple(bd for bd in b.chunks if bd != (1,))

    name = 'squeeze-' + tokenize(a, axis)
    old_keys = list(product([b.name], *[range(len(bd)) for bd in b.chunks]))
    new_keys = list(product([name], *[range(len(bd)) for bd in chunks]))

    dsk = {n: b.dask[o] for o, n in zip(old_keys, new_keys)}

    return Array(sharedict.merge(b.dask, (name, dsk)), name, chunks, dtype=a.dtype)


def topk(k, x):
    """ The top k elements of an array

    Returns the k greatest elements of the array in sorted order.  Only works
    on arrays of a single dimension.

    This assumes that ``k`` is small.  All results will be returned in a single
    chunk.

    Examples
    --------

    >>> x = np.array([5, 1, 3, 6])
    >>> d = from_array(x, chunks=2)
    >>> d.topk(2).compute()
    array([6, 5])
    """
    if x.ndim != 1:
        raise ValueError("Topk only works on arrays of one dimension")

    token = tokenize(k, x)
    name = 'chunk.topk-' + token
    dsk = dict(((name, i), (chunk.topk, k, key))
               for i, key in enumerate(x._keys()))
    name2 = 'topk-' + token
    dsk[(name2, 0)] = (getitem, (np.sort, (np.concatenate, list(dsk))),
                       slice(-1, -k - 1, -1))
    chunks = ((k,),)

    return Array(sharedict.merge((name2, dsk), x.dask), name2, chunks, dtype=x.dtype)


@wraps(np.compress)
def compress(condition, a, axis=None):
    if axis is None:
        a = a.ravel()
        axis = 0
    if not -a.ndim <= axis < a.ndim:
        raise ValueError('axis=(%s) out of bounds' % axis)
    if axis < 0:
        axis += a.ndim

    # Only coerce non-lazy values to numpy arrays
    if not isinstance(condition, Array):
        condition = np.array(condition, dtype=bool)
    if condition.ndim != 1:
        raise ValueError("Condition must be one dimensional")

    if isinstance(condition, Array):
        if len(condition) < a.shape[axis]:
            a = a[tuple(slice(None, len(condition))
                        if i == axis else slice(None)
                        for i in range(a.ndim))]
        inds = tuple(range(a.ndim))
        out = atop(np.compress, inds, condition, (inds[axis],), a, inds,
                   axis=axis, dtype=a.dtype)
        out._chunks = tuple((np.NaN,) * len(c) if i == axis else c
                            for i, c in enumerate(out.chunks))
        return out
    else:
        # Optimized case when condition is known
        if len(condition) < a.shape[axis]:
            condition = condition.copy()
            condition.resize(a.shape[axis])

        slc = ((slice(None),) * axis + (condition, ) +
               (slice(None),) * (a.ndim - axis - 1))
        return a[slc]


@wraps(np.extract)
def extract(condition, arr):
    if not isinstance(condition, Array):
        condition = np.array(condition, dtype=bool)
    return compress(condition.ravel(), arr.ravel())


@wraps(np.take)
def take(a, indices, axis=0):
    if not -a.ndim <= axis < a.ndim:
        raise ValueError('axis=(%s) out of bounds' % axis)
    if axis < 0:
        axis += a.ndim
    if isinstance(a, np.ndarray) and isinstance(indices, Array):
        return _take_dask_array_from_numpy(a, indices, axis)
    else:
        return a[(slice(None),) * axis + (indices,)]


def _take_dask_array_from_numpy(a, indices, axis):
    assert isinstance(a, np.ndarray)
    assert isinstance(indices, Array)

    return indices.map_blocks(lambda block: np.take(a, block, axis),
                              chunks=indices.chunks,
                              dtype=a.dtype)


@wraps(np.around)
def around(x, decimals=0):
    return map_blocks(partial(np.around, decimals=decimals), x, dtype=x.dtype)


def isnull(values):
    """ pandas.isnull for dask arrays """
    import pandas as pd
    return elemwise(pd.isnull, values, dtype='bool')


def notnull(values):
    """ pandas.notnull for dask arrays """
    return ~isnull(values)


@wraps(numpy_compat.isclose)
def isclose(arr1, arr2, rtol=1e-5, atol=1e-8, equal_nan=False):
    func = partial(numpy_compat.isclose, rtol=rtol, atol=atol, equal_nan=equal_nan)
    return elemwise(func, arr1, arr2, dtype='bool')


def variadic_choose(a, *choices):
    return np.choose(a, choices)


@wraps(np.choose)
def choose(a, choices):
    return elemwise(variadic_choose, a, *choices)


def _isnonzero_vec(v):
    return bool(np.count_nonzero(v))


_isnonzero_vec = np.vectorize(_isnonzero_vec, otypes=[bool])


def isnonzero(a):
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


@wraps(np.argwhere)
def argwhere(a):
    from .creation import indices

    a = asarray(a)

    nz = isnonzero(a).flatten()

    ind = indices(a.shape, dtype=np.int64, chunks=a.chunks)
    if ind.ndim > 1:
        ind = stack([ind[i].ravel() for i in range(len(ind))], axis=1)
    ind = compress(nz, ind, axis=0)

    return ind


@wraps(np.where)
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


@wraps(np.count_nonzero)
def count_nonzero(a, axis=None):
    return isnonzero(asarray(a)).astype(np.int64).sum(axis=axis)


@wraps(np.flatnonzero)
def flatnonzero(a):
    return argwhere(asarray(a).ravel())[:, 0]


@wraps(np.nonzero)
def nonzero(a):
    ind = argwhere(a)
    if ind.ndim > 1:
        return tuple(ind[:, i] for i in range(ind.shape[1]))
    else:
        return (ind,)


@wraps(chunk.coarsen)
def coarsen(reduction, x, axes, trim_excess=False):
    if (not trim_excess and
        not all(bd % div == 0 for i, div in axes.items()
                for bd in x.chunks[i])):
        msg = "Coarsening factor does not align with block dimensions"
        raise ValueError(msg)

    if 'dask' in inspect.getfile(reduction):
        reduction = getattr(np, reduction.__name__)

    name = 'coarsen-' + tokenize(reduction, x, axes, trim_excess)
    dsk = dict(((name,) + key[1:], (chunk.coarsen, reduction, key, axes,
                                    trim_excess))
               for key in flatten(x._keys()))
    chunks = tuple(tuple(int(bd // axes.get(i, 1)) for bd in bds)
                   for i, bds in enumerate(x.chunks))

    dt = reduction(np.empty((1,) * x.ndim, dtype=x.dtype)).dtype
    return Array(sharedict.merge(x.dask, (name, dsk)), name, chunks, dtype=dt)


def split_at_breaks(array, breaks, axis=0):
    """ Split an array into a list of arrays (using slices) at the given breaks

    >>> split_at_breaks(np.arange(6), [3, 5])
    [array([0, 1, 2]), array([3, 4]), array([5])]
    """
    padded_breaks = concat([[None], breaks, [None]])
    slices = [slice(i, j) for i, j in sliding_window(2, padded_breaks)]
    preslice = (slice(None),) * axis
    split_array = [array[preslice + (s,)] for s in slices]
    return split_array


@wraps(np.insert)
def insert(arr, obj, values, axis):
    # axis is a required argument here to avoid needing to deal with the numpy
    # default case (which reshapes the array to make it flat)
    if not -arr.ndim <= axis < arr.ndim:
        raise IndexError('axis %r is out of bounds for an array of dimension '
                         '%s' % (axis, arr.ndim))
    if axis < 0:
        axis += arr.ndim

    if isinstance(obj, slice):
        obj = np.arange(*obj.indices(arr.shape[axis]))
    obj = np.asarray(obj)
    scalar_obj = obj.ndim == 0
    if scalar_obj:
        obj = np.atleast_1d(obj)

    obj = np.where(obj < 0, obj + arr.shape[axis], obj)
    if (np.diff(obj) < 0).any():
        raise NotImplementedError(
            'da.insert only implemented for monotonic ``obj`` argument')

    split_arr = split_at_breaks(arr, np.unique(obj), axis)

    if getattr(values, 'ndim', 0) == 0:
        # we need to turn values into a dask array
        name = 'values-' + tokenize(values)
        dtype = getattr(values, 'dtype', type(values))
        values = Array({(name,): values}, name, chunks=(), dtype=dtype)

        values_shape = tuple(len(obj) if axis == n else s
                             for n, s in enumerate(arr.shape))
        values = broadcast_to(values, values_shape)
    elif scalar_obj:
        values = values[(slice(None),) * axis + (None,)]

    values_chunks = tuple(values_bd if axis == n else arr_bd
                          for n, (arr_bd, values_bd)
                          in enumerate(zip(arr.chunks,
                                           values.chunks)))
    values = values.rechunk(values_chunks)

    counts = np.bincount(obj)[:-1]
    values_breaks = np.cumsum(counts[counts > 0])
    split_values = split_at_breaks(values, values_breaks, axis)

    interleaved = list(interleave([split_arr, split_values]))
    interleaved = [i for i in interleaved if i.nbytes]
    return concatenate(interleaved, axis=axis)

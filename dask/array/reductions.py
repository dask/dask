from __future__ import absolute_import, division, print_function

from functools import partial, wraps
from itertools import product
from math import factorial, log, ceil

import numpy as np
from toolz import compose, partition_all, merge, get

from . import chunk
from .core import _concatenate2, Array, atop, sqrt, elemwise, lol_tuples
from .numpy_compat import divide
from .slicing import insert_many
from ..compatibility import getargspec, builtins
from ..core import flatten
from ..base import tokenize
from ..utils import ignoring


def reduction(x, chunk, aggregate, axis=None, keepdims=None, dtype=None,
              max_leaves=None, combine=None):
    """ General version of reductions

    >>> reduction(my_array, np.sum, np.sum, axis=0, keepdims=False)  # doctest: +SKIP
    """
    if axis is None:
        axis = tuple(range(x.ndim))
    if isinstance(axis, int):
        axis = (axis,)
    axis = tuple(i if i >= 0 else x.ndim + i for i in axis)

    if dtype and 'dtype' in getargspec(chunk).args:
        chunk = partial(chunk, dtype=dtype)
    if dtype and 'dtype' in getargspec(aggregate).args:
        aggregate = partial(aggregate, dtype=dtype)

    # Normalize axes
    if max_leaves is None:
        max_leaves = dict(zip(range(x.ndim), x.numblocks))
    if isinstance(max_leaves, int):
        n = builtins.max(int(len(axis) ** (1/max_leaves)), 2)
        max_leaves = dict.fromkeys(axis, n)
    max_leaves = dict((k, v) for (k, v) in max_leaves.items() if k in axis)

    # Map chunk across all blocks
    inds = tuple(range(x.ndim))
    tmp = atop(partial(chunk, axis=axis, keepdims=True), inds, x, inds)
    tmp._chunks = tuple((1,)*len(c) if i in axis else c for (i, c)
                        in enumerate(tmp.chunks))

    # Reduce across intermediates
    depth = 1
    for i, n in enumerate(tmp.numblocks):
        if i in max_leaves and max_leaves[i] != 1:
            depth = int(builtins.max(depth, ceil(log(n, max_leaves[i]))))
    func = compose(partial(combine or aggregate, axis=axis, keepdims=True),
                   partial(_concatenate2, axes=axis))
    for i in range(depth - 1):
        tmp = partial_reduce(func, tmp, max_leaves, True, None)
    func = compose(partial(aggregate, axis=axis, keepdims=keepdims),
                   partial(_concatenate2, axes=axis))
    return partial_reduce(func, tmp, max_leaves, keepdims, dtype)


def partial_reduce(func, x, max_leaves, keepdims=False, dtype=None):
    """Partial reduction across multiple axes.

    Parameters
    ----------
    func : function
    x : Array
    max_leaves : dict
        Maximum reduction block sizes in each dimension.

    Example
    -------
    Reduce across axis 0 and 2, merging a maximum of 1 block in the 0th
    dimension, and 3 blocks in the 2nd dimension:

    >>> partial_reduce(np.min, x, {0: 1, 2: 3})    # doctest: +SKIP
    """
    name = tokenize(func, x, max_leaves, keepdims, dtype)
    parts = [list(partition_all(max_leaves.get(i, 1), range(n))) for (i, n)
             in enumerate(x.numblocks)]
    keys = product(*map(range, map(len, parts)))
    out_chunks = [tuple(1 for p in partition_all(max_leaves[i], c)) if i
                  in max_leaves else c for (i, c) in enumerate(x.chunks)]
    if not keepdims:
        out_axis = [i for i in range(x.ndim) if i not in max_leaves]
        getter = lambda k: get(out_axis, k)
        keys = map(getter, keys)
        out_chunks = list(getter(out_chunks))
    dsk = {}
    for k, p in zip(keys, product(*parts)):
        decided = dict((i, j[0]) for (i, j) in enumerate(p) if len(j) == 1)
        dummy = dict(i for i in enumerate(p) if i[0] not in decided)
        g = lol_tuples((x.name,), range(x.ndim), decided, dummy)
        dsk[(name,) + k] = (func, g)
    return Array(merge(dsk, x.dask), name, out_chunks, dtype=dtype)


@wraps(chunk.sum)
def sum(a, axis=None, dtype=None, keepdims=False, max_leaves=None):
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = np.empty((1,), dtype=a._dtype).sum().dtype
    else:
        dt = None
    return reduction(a, chunk.sum, chunk.sum, axis=axis, keepdims=keepdims,
                     dtype=dt, max_leaves=max_leaves)


@wraps(chunk.prod)
def prod(a, axis=None, dtype=None, keepdims=False, max_leaves=None):
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = np.empty((1,), dtype=a._dtype).prod().dtype
    else:
        dt = None
    return reduction(a, chunk.prod, chunk.prod, axis=axis, keepdims=keepdims,
                     dtype=dt, max_leaves=max_leaves)


@wraps(chunk.min)
def min(a, axis=None, keepdims=False, max_leaves=None):
    return reduction(a, chunk.min, chunk.min, axis=axis, keepdims=keepdims,
                     dtype=a._dtype, max_leaves=max_leaves)


@wraps(chunk.max)
def max(a, axis=None, keepdims=False, max_leaves=None):
    return reduction(a, chunk.max, chunk.max, axis=axis, keepdims=keepdims,
                     dtype=a._dtype, max_leaves=max_leaves)


@wraps(chunk.argmin)
def argmin(a, axis=None):
    return arg_reduction(a, chunk.min, chunk.argmin, axis=axis, dtype='i8')


@wraps(chunk.nanargmin)
def nanargmin(a, axis=None):
    return arg_reduction(a, chunk.nanmin, chunk.nanargmin, axis=axis,
                         dtype='i8')


@wraps(chunk.argmax)
def argmax(a, axis=None):
    return arg_reduction(a, chunk.max, chunk.argmax, axis=axis, dtype='i8')


@wraps(chunk.nanargmax)
def nanargmax(a, axis=None):
    return arg_reduction(a, chunk.nanmax, chunk.nanargmax, axis=axis,
                         dtype='i8')


@wraps(chunk.any)
def any(a, axis=None, keepdims=False, max_leaves=None):
    return reduction(a, chunk.any, chunk.any, axis=axis, keepdims=keepdims,
                     dtype='bool', max_leaves=max_leaves)


@wraps(chunk.all)
def all(a, axis=None, keepdims=False, max_leaves=None):
    return reduction(a, chunk.all, chunk.all, axis=axis, keepdims=keepdims,
                     dtype='bool', max_leaves=max_leaves)


@wraps(chunk.nansum)
def nansum(a, axis=None, dtype=None, keepdims=False, max_leaves=None):
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = chunk.nansum(np.empty((1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, chunk.nansum, chunk.sum, axis=axis, keepdims=keepdims,
                     dtype=dt, max_leaves=max_leaves)


with ignoring(AttributeError):
    @wraps(chunk.nanprod)
    def nanprod(a, axis=None, dtype=None, keepdims=False, max_leaves=None):
        if dtype is not None:
            dt = dtype
        elif a._dtype is not None:
            dt = np.empty((1,), dtype=a._dtype).nanprod().dtype
        else:
            dt = None
        return reduction(a, chunk.nanprod, chunk.prod, axis=axis,
                         keepdims=keepdims, dtype=dt, max_leaves=max_leaves)


@wraps(chunk.nanmin)
def nanmin(a, axis=None, keepdims=False, max_leaves=None):
    return reduction(a, chunk.nanmin, chunk.nanmin, axis=axis,
                     keepdims=keepdims, dtype=a._dtype, max_leaves=max_leaves)


@wraps(chunk.nanmax)
def nanmax(a, axis=None, keepdims=False, max_leaves=None):
    return reduction(a, chunk.nanmax, chunk.nanmax, axis=axis,
                     keepdims=keepdims, dtype=a._dtype, max_leaves=max_leaves)


def numel(x, **kwargs):
    """ A reduction to count the number of elements """
    return chunk.sum(np.ones_like(x), **kwargs)


def nannumel(x, **kwargs):
    """ A reduction to count the number of elements """
    return chunk.sum(~np.isnan(x), **kwargs)


def mean_chunk(x, sum=chunk.sum, numel=numel, dtype='f8', **kwargs):
    n = numel(x, dtype=dtype, **kwargs)
    total = sum(x, dtype=dtype, **kwargs)
    result = np.empty(shape=n.shape,
              dtype=[('total', total.dtype), ('n', n.dtype)])
    result['n'] = n
    result['total'] = total
    return result


def mean_combine(pair, sum=chunk.sum, numel=numel, dtype='f8', **kwargs):
    n = sum(pair['n'], **kwargs)
    total = sum(pair['total'], **kwargs)
    result = np.empty(shape=n.shape, dtype=pair.dtype)
    result['n'] = n
    result['total'] = total
    return result


def mean_agg(pair, dtype='f8', **kwargs):
    return divide(pair['total'].sum(dtype=dtype, **kwargs),
                  pair['n'].sum(dtype=dtype, **kwargs), dtype=dtype)


@wraps(chunk.mean)
def mean(a, axis=None, dtype=None, keepdims=False, max_leaves=None):
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = np.mean(np.empty(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, mean_chunk, mean_agg, axis=axis, keepdims=keepdims,
                     dtype=dt, max_leaves=max_leaves, combine=mean_combine)


def nanmean(a, axis=None, dtype=None, keepdims=False, max_leaves=None):
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = np.mean(np.empty(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, partial(mean_chunk, sum=chunk.nansum, numel=nannumel),
                     mean_agg, axis=axis, keepdims=keepdims, dtype=dt,
                     max_leaves=max_leaves,
                     combine=partial(mean_combine, sum=chunk.nansum, numel=nannumel))

with ignoring(AttributeError):
    nanmean = wraps(chunk.nanmean)(nanmean)


def moment_chunk(A, order=2, sum=chunk.sum, numel=numel, dtype='f8', **kwargs):
    total = sum(A, dtype=dtype, **kwargs)
    n = numel(A, **kwargs)
    u = total/n
    M = np.empty(shape=n.shape + (order - 1,), dtype=dtype)
    for i in range(2, order + 1):
        M[..., i - 2] = sum((A - u)**i, dtype=dtype, **kwargs)
    result = np.empty(shape=n.shape, dtype=[('total', total.dtype),
                                            ('n', n.dtype),
                                            ('M', M.dtype, (order-1,))])
    result['total'] = total
    result['n'] = n
    result['M'] = M
    return result


def moment_agg(data, order=2, ddof=0, dtype='f8', sum=np.sum, **kwargs):
    totals = data['total']
    ns = data['n']
    Ms = data['M']

    kwargs['dtype'] = dtype
    # To properly handle ndarrays, the original dimensions need to be kept for
    # part of the calculation.
    keepdim_kw = kwargs.copy()
    keepdim_kw['keepdims'] = True

    n = sum(ns, **keepdim_kw)
    mu = divide(totals.sum(**keepdim_kw), n, dtype=dtype)
    inner_term = divide(totals, ns, dtype=dtype) - mu

    result = Ms[..., -1].sum(**kwargs)

    for k in range(1, order - 1):
        coeff = factorial(order)/(factorial(k)*factorial(order - k))
        result += coeff * sum(Ms[..., order - k - 2] * inner_term**k, **kwargs)

    result += sum(ns * inner_term**order, **kwargs)
    result = divide(result, sum(n, **kwargs) - ddof, dtype=dtype)
    return result


def moment(a, order, axis=None, dtype=None, keepdims=False, ddof=0):
    if not isinstance(order, int) or order < 2:
        raise ValueError("Order must be an integer >= 2")
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = np.var(np.ones(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, partial(moment_chunk, order=order), partial(moment_agg,
                     order=order, ddof=ddof), axis=axis, keepdims=keepdims,
                     dtype=dt)


@wraps(chunk.var)
def var(a, axis=None, dtype=None, keepdims=False, ddof=0):
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = np.var(np.ones(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, moment_chunk, partial(moment_agg, ddof=ddof), axis=axis,
                     keepdims=keepdims, dtype=dt)


def nanvar(a, axis=None, dtype=None, keepdims=False, ddof=0):
    if dtype is not None:
        dt = dtype
    elif a._dtype is not None:
        dt = np.var(np.ones(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, partial(moment_chunk, sum=chunk.nansum, numel=nannumel),
                     partial(moment_agg, sum=np.nansum, ddof=ddof), axis=axis,
                     keepdims=keepdims, dtype=dt)

with ignoring(AttributeError):
    nanvar = wraps(chunk.nanvar)(nanvar)

@wraps(chunk.std)
def std(a, axis=None, dtype=None, keepdims=False, ddof=0):
    result = sqrt(a.var(axis=axis, dtype=dtype, keepdims=keepdims, ddof=ddof))
    if dtype and dtype != result.dtype:
        result = result.astype(dtype)
    return result


def nanstd(a, axis=None, dtype=None, keepdims=False, ddof=0):
    result = sqrt(nanvar(a, axis=axis, dtype=dtype, keepdims=keepdims, ddof=ddof))
    if dtype and dtype != result.dtype:
        result = result.astype(dtype)
    return result

with ignoring(AttributeError):
    nanstd = wraps(chunk.nanstd)(nanstd)


def vnorm(a, ord=None, axis=None, dtype=None, keepdims=False, max_leaves=None):
    """ Vector norm

    See np.linalg.norm
    """
    if ord is None or ord == 'fro':
        ord = 2
    if ord == np.inf:
        return max(abs(a), axis=axis, keepdims=keepdims, max_leaves=max_leaves)
    elif ord == -np.inf:
        return min(abs(a), axis=axis, keepdims=keepdims, max_leaves=max_leaves)
    elif ord == 1:
        return sum(abs(a), axis=axis, dtype=dtype, keepdims=keepdims,
                   max_leaves=max_leaves)
    elif ord % 2 == 0:
        return sum(a**ord, axis=axis, dtype=dtype, keepdims=keepdims,
                   max_leaves=max_leaves)**(1./ord)
    else:
        return sum(abs(a)**ord, axis=axis, dtype=dtype, keepdims=keepdims,
                   max_leaves=max_leaves)**(1./ord)


def arg_aggregate(func, argfunc, dims, pairs):
    """

    >>> pairs = [([4, 3, 5], [10, 11, 12]),
    ...          ([3, 5, 1], [1, 2, 3])]
    >>> arg_aggregate(np.min, np.argmin, (100, 100), pairs)
    array([101,  11, 103])
    """
    pairs = list(pairs)
    mins, argmins = zip(*pairs)
    mins = np.array(mins)
    argmins = np.array(argmins)
    args = argfunc(mins, axis=0)

    offsets = np.add.accumulate([0] + list(dims)[:-1])
    offsets = offsets.reshape((len(offsets),) + (1,) * (argmins.ndim - 1))
    return np.choose(args, argmins + offsets)


def arg_reduction(a, func, argfunc, axis=0, dtype=None):
    """ General version of argmin/argmax

    >>> arg_reduction(my_array, np.min, axis=0)  # doctest: +SKIP
    """
    if not isinstance(axis, int):
        raise ValueError("Must specify integer axis= keyword argument.\n"
                "For example:\n"
                "  Before:  x.argmin()\n"
                "  After:   x.argmin(axis=0)\n")

    if axis < 0:
        axis = a.ndim + axis

    def argreduce(x):
        """ Get both min/max and argmin/argmax of each block """
        return (func(x, axis=axis), argfunc(x, axis=axis))

    a2 = elemwise(argreduce, a)

    return atop(partial(arg_aggregate, func, argfunc, a.chunks[axis]),
                [i for i in range(a.ndim) if i != axis],
                a2, list(range(a.ndim)), dtype=dtype)

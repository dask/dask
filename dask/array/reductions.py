from __future__ import absolute_import, division, print_function

import numpy as np
from functools import partial, wraps
from toolz import compose, curry

from .core import (_concatenate2, insert_many, Array, atop, names, sqrt,
        elemwise)
from ..core import flatten
from . import chunk
from ..utils import ignoring


def reduction(x, chunk, aggregate, axis=None, keepdims=None, dtype=None):
    """ General version of reductions

    >>> reduction(my_array, np.sum, np.sum, axis=0, keepdims=False)  # doctest: +SKIP
    """
    if axis is None:
        axis = tuple(range(x.ndim))
    if isinstance(axis, int):
        axis = (axis,)

    chunk2 = partial(chunk, axis=axis, keepdims=True)
    aggregate2 = partial(aggregate, axis=axis, keepdims=keepdims)

    inds = tuple(range(x.ndim))
    tmp = atop(chunk2, next(names), inds, x, inds)

    inds2 = tuple(i for i in inds if i not in axis)

    result = atop(compose(aggregate2, curry(_concatenate2, axes=axis)),
                  next(names), inds2, tmp, inds, dtype=dtype)

    if keepdims:
        dsk = result.dask.copy()
        for k in flatten(result._keys()):
            k2 = (k[0],) + insert_many(k[1:], axis, 0)
            dsk[k2] = dsk.pop(k)
        chunks = insert_many(result.chunks, axis, [1])
        return Array(dsk, result.name, chunks=chunks, dtype=dtype)
    else:
        return result


@wraps(chunk.sum)
def sum(a, axis=None, keepdims=False):
    if a._dtype is not None:
        dt = np.empty((1,), dtype=a._dtype).sum().dtype
    else:
        dt = None
    return reduction(a, chunk.sum, chunk.sum, axis=axis, keepdims=keepdims,
                     dtype=dt)


@wraps(chunk.prod)
def prod(a, axis=None, keepdims=False):
    if a._dtype is not None:
        dt = np.empty((1,), dtype=a._dtype).prod().dtype
    else:
        dt = None
    return reduction(a, chunk.prod, chunk.prod, axis=axis, keepdims=keepdims,
                     dtype=dt)


@wraps(chunk.min)
def min(a, axis=None, keepdims=False):
    return reduction(a, chunk.min, chunk.min, axis=axis, keepdims=keepdims,
                     dtype=a._dtype)


@wraps(chunk.max)
def max(a, axis=None, keepdims=False):
    return reduction(a, chunk.max, chunk.max, axis=axis, keepdims=keepdims,
                     dtype=a._dtype)


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
def any(a, axis=None, keepdims=False):
    return reduction(a, chunk.any, chunk.any, axis=axis, keepdims=keepdims,
                     dtype='bool')


@wraps(chunk.all)
def all(a, axis=None, keepdims=False):
    return reduction(a, chunk.all, chunk.all, axis=axis, keepdims=keepdims,
                     dtype='bool')


@wraps(chunk.nansum)
def nansum(a, axis=None, keepdims=False):
    if a._dtype is not None:
        dt = chunk.nansum(np.empty((1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, chunk.nansum, chunk.sum, axis=axis, keepdims=keepdims,
                     dtype=dt)


with ignoring(AttributeError):
    @wraps(chunk.nanprod)
    def nanprod(a, axis=None, keepdims=False):
        if a._dtype is not None:
            dt = np.empty((1,), dtype=a._dtype).nanprod().dtype
        else:
            dt = None
        return reduction(a, chunk.nanprod, chunk.prod, axis=axis,
                         keepdims=keepdims, dtype=dt)


@wraps(chunk.nanmin)
def nanmin(a, axis=None, keepdims=False):
    return reduction(a, chunk.nanmin, chunk.min, axis=axis, keepdims=keepdims,
                     dtype=a._dtype)


@wraps(chunk.nanmax)
def nanmax(a, axis=None, keepdims=False):
    return reduction(a, chunk.nanmax, chunk.max, axis=axis, keepdims=keepdims,
                     dtype=a._dtype)


def numel(x, **kwargs):
    """ A reduction to count the number of elements """
    return chunk.sum(np.ones_like(x), **kwargs)


def nannumel(x, **kwargs):
    """ A reduction to count the number of elements """
    return chunk.sum(~np.isnan(x), **kwargs)


def mean_chunk(x, sum=chunk.sum, numel=numel, **kwargs):
    n = numel(x, **kwargs)
    total = sum(x, **kwargs)
    result = np.empty(shape=n.shape,
              dtype=[('total', total.dtype), ('n', n.dtype)])
    result['n'] = n
    result['total'] = total
    return result

def mean_agg(pair, **kwargs):
    return pair['total'].sum(**kwargs) / pair['n'].sum(**kwargs)


@wraps(chunk.mean)
def mean(a, axis=None, keepdims=False):
    if a._dtype is not None:
        dt = np.mean(np.empty(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, mean_chunk, mean_agg, axis=axis, keepdims=keepdims,
                     dtype=dt)


def nanmean(a, axis=None, keepdims=False):
    if a._dtype is not None:
        dt = np.mean(np.empty(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, partial(mean_chunk, sum=chunk.nansum, numel=nannumel),
                     mean_agg, axis=axis, keepdims=keepdims, dtype=dt)
with ignoring(AttributeError):
    nanmean = wraps(chunk.nanmean)(nanmean)


def var_chunk(A, sum=chunk.sum, numel=numel, **kwargs):
    n = numel(A, **kwargs)
    x = sum(A, dtype='f8', **kwargs)
    x2 = sum(A**2, dtype='f8', **kwargs)
    result = np.empty(shape=n.shape, dtype=[('x', x.dtype),
                                            ('x2', x2.dtype),
                                            ('n', n.dtype)])
    result['x'] = x
    result['x2'] = x2
    result['n'] = n
    return result

def var_agg(A, ddof=None, **kwargs):
    x = A['x'].sum(**kwargs)
    x2 = A['x2'].sum(**kwargs)
    n = A['n'].sum(**kwargs)
    result = (x2 / n) - (x / n)**2
    if ddof:
        result = result * n / (n - ddof)
    return result


@wraps(chunk.var)
def var(a, axis=None, keepdims=False, ddof=0):
    if a._dtype is not None:
        dt = np.var(np.empty(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, var_chunk, partial(var_agg, ddof=ddof), axis=axis,
                     keepdims=keepdims, dtype=dt)


def nanvar(a, axis=None, keepdims=False, ddof=0):
    if a._dtype is not None:
        dt = np.var(np.empty(shape=(1,), dtype=a._dtype)).dtype
    else:
        dt = None
    return reduction(a, partial(var_chunk, sum=chunk.nansum, numel=nannumel),
                     partial(var_agg, ddof=ddof), axis=axis, keepdims=keepdims,
                     dtype=dt)
with ignoring(AttributeError):
    nanvar = wraps(chunk.nanvar)(nanvar)

@wraps(chunk.std)
def std(a, axis=None, keepdims=False, ddof=0):
    return sqrt(a.var(axis=axis, keepdims=keepdims, ddof=ddof))


def nanstd(a, axis=None, keepdims=False, ddof=0):
    return sqrt(nanvar(a, axis=axis, keepdims=keepdims, ddof=ddof))
with ignoring(AttributeError):
    nanstd = wraps(chunk.nanstd)(nanstd)


def vnorm(a, ord=None, axis=None, keepdims=False):
    """ Vector norm

    See np.linalg.norm
    """
    if ord is None or ord == 'fro':
        ord = 2
    if ord == np.inf:
        return max(abs(a), axis=axis, keepdims=keepdims)
    elif ord == -np.inf:
        return min(abs(a), axis=axis, keepdims=keepdims)
    elif ord == 1:
        return sum(abs(a), axis=axis, keepdims=keepdims)
    elif ord % 2 == 0:
        return sum(a**ord, axis=axis, keepdims=keepdims)**(1./ord)
    else:
        return sum(abs(a)**ord, axis=axis, keepdims=keepdims)**(1./ord)


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

    def argreduce(x):
        """ Get both min/max and argmin/argmax of each block """
        return (func(x, axis=axis), argfunc(x, axis=axis))

    a2 = elemwise(argreduce, a)

    return atop(partial(arg_aggregate, func, argfunc, a.chunks[axis]),
                next(names), [i for i in range(a.ndim) if i != axis],
                a2, list(range(a.ndim)), dtype=dtype)

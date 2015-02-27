from __future__ import absolute_import, division, print_function

import numpy as np
from functools import partial, wraps
from toolz import compose, curry

from .core import (_concatenate2, insert_many, Array, atop, names, sqrt,
        elemwise)
from ..core import flatten
from ..utils import ignoring


def reduction(x, chunk, aggregate, axis=None, keepdims=None):
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
                  next(names), inds2, tmp, inds)

    if keepdims:
        dsk = result.dask.copy()
        for k in flatten(result._keys()):
            k2 = (k[0],) + insert_many(k[1:], axis, 0)
            dsk[k2] = dsk.pop(k)
        blockdims = insert_many(result.blockdims, axis, [1])
        return Array(dsk, result.name, blockdims=blockdims)
    else:
        return result


@wraps(np.sum)
def sum(a, axis=None, keepdims=False):
    return reduction(a, np.sum, np.sum, axis=axis, keepdims=keepdims)


@wraps(np.prod)
def prod(a, axis=None, keepdims=False):
    return reduction(a, np.prod, np.prod, axis=axis, keepdims=keepdims)


@wraps(np.min)
def min(a, axis=None, keepdims=False):
    return reduction(a, np.min, np.min, axis=axis, keepdims=keepdims)


@wraps(np.max)
def max(a, axis=None, keepdims=False):
    return reduction(a, np.max, np.max, axis=axis, keepdims=keepdims)


@wraps(np.argmin)
def argmin(a, axis=None):
    return arg_reduction(a, np.min, np.argmin, axis=axis)


@wraps(np.nanargmin)
def nanargmin(a, axis=None):
    return arg_reduction(a, np.nanmin, np.nanargmin, axis=axis)


@wraps(np.argmax)
def argmax(a, axis=None):
    return arg_reduction(a, np.max, np.argmax, axis=axis)


@wraps(np.nanargmax)
def nanargmax(a, axis=None):
    return arg_reduction(a, np.nanmax, np.nanargmax, axis=axis)


@wraps(np.any)
def any(a, axis=None, keepdims=False):
    return reduction(a, np.any, np.any, axis=axis, keepdims=keepdims)


@wraps(np.all)
def all(a, axis=None, keepdims=False):
    return reduction(a, np.all, np.all, axis=axis, keepdims=keepdims)


@wraps(np.nansum)
def nansum(a, axis=None, keepdims=False):
    return reduction(a, np.nansum, np.sum, axis=axis, keepdims=keepdims)


with ignoring(AttributeError):
    @wraps(np.nanprod)
    def nanprod(a, axis=None, keepdims=False):
        return reduction(a, np.nanprod, np.prod, axis=axis, keepdims=keepdims)


@wraps(np.nanmin)
def nanmin(a, axis=None, keepdims=False):
    return reduction(a, np.nanmin, np.min, axis=axis, keepdims=keepdims)


@wraps(np.nanmax)
def nanmax(a, axis=None, keepdims=False):
    return reduction(a, np.nanmax, np.max, axis=axis, keepdims=keepdims)


def numel(x, **kwargs):
    """ A reduction to count the number of elements """
    return np.sum(np.ones_like(x), **kwargs)


def nannumel(x, **kwargs):
    """ A reduction to count the number of elements """
    return np.sum(~np.isnan(x), **kwargs)


def mean_chunk(x, sum=np.sum, numel=numel, **kwargs):
    n = numel(x, **kwargs)
    total = sum(x, **kwargs)
    result = np.empty(shape=n.shape,
              dtype=[('total', total.dtype), ('n', n.dtype)])
    result['n'] = n
    result['total'] = total
    return result

def mean_agg(pair, **kwargs):
    return pair['total'].sum(**kwargs) / pair['n'].sum(**kwargs)


@wraps(np.mean)
def mean(a, axis=None, keepdims=False):
    return reduction(a, mean_chunk, mean_agg, axis=axis, keepdims=keepdims)


@wraps(np.nanmean)
def nanmean(a, axis=None, keepdims=False):
    return reduction(a, partial(mean_chunk, sum=np.nansum, numel=nannumel),
                     mean_agg, axis=axis, keepdims=keepdims)

def var_chunk(A, sum=np.sum, numel=numel, **kwargs):
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


@wraps(np.var)
def var(a, axis=None, keepdims=False, ddof=0):
    return reduction(a, var_chunk, partial(var_agg, ddof=ddof), axis=axis, keepdims=keepdims)


@wraps(np.nanvar)
def nanvar(a, axis=None, keepdims=False, ddof=0):
    return reduction(a, partial(var_chunk, sum=np.nansum, numel=nannumel),
                     partial(var_agg, ddof=ddof), axis=axis, keepdims=keepdims)

@wraps(np.std)
def std(a, axis=None, keepdims=False, ddof=0):
    return sqrt(a.var(axis=axis, keepdims=keepdims, ddof=ddof))


@wraps(np.nanstd)
def nanstd(a, axis=None, keepdims=False, ddof=0):
    return sqrt(nanvar(a, axis=axis, keepdims=keepdims, ddof=ddof))


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


def arg_reduction(a, func, argfunc, axis=0):
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

    return atop(partial(arg_aggregate, func, argfunc, a.blockdims[axis]),
                next(names), [i for i in range(a.ndim) if i != axis],
                a2, list(range(a.ndim)))

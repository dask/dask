from __future__ import absolute_import, division, print_function

import operator
from functools import partial, wraps
from itertools import product, repeat
from math import factorial, log, ceil

import numpy as np
from numbers import Integral

from toolz import compose, partition_all, get, accumulate, pluck

from . import chunk
from .core import _concatenate2, Array, atop, lol_tuples, handle_out
from .creation import arange
from .ufunc import sqrt
from .utils import validate_axis
from .wrap import zeros, ones
from .numpy_compat import ma_divide, divide as np_divide
from ..compatibility import getargspec, builtins
from ..base import tokenize
from ..utils import ignoring, funcname, Dispatch
from .. import config, sharedict

# Generic functions to support chunks of different types
empty_lookup = Dispatch('empty')
empty_lookup.register((object, np.ndarray), np.empty)
empty_lookup.register(np.ma.masked_array, np.ma.empty)
divide_lookup = Dispatch('divide')
divide_lookup.register((object, np.ndarray), np_divide)
divide_lookup.register(np.ma.masked_array, ma_divide)


def divide(a, b, dtype=None):
    key = lambda x: getattr(x, '__array_priority__', float('-inf'))
    f = divide_lookup.dispatch(type(builtins.max(a, b, key=key)))
    return f(a, b, dtype=dtype)


def reduction(x, chunk, aggregate, axis=None, keepdims=False, dtype=None,
              split_every=None, combine=None, name=None, out=None,
              concatenate=True, output_size=1):
    """ General version of reductions

    Parameters
    ----------
    x: Array
        Data being reduced along one or more axes
    chunk: callable(x_chunk, axis, keepdims)
        First function to be executed when resolving the dask graph.
        This function is applied in parallel to all original chunks of x.
        See below for function parameters.
    combine: callable(x_chunk, axis, keepdims), optional
        Function used for intermediate recursive aggregation (see
        split_every below). If omitted, it defaults to aggregate.
        If the reduction can be performed in less than 3 steps, it will not
        be invoked at all.
    aggregate: callable(x_chunk, axis, keepdims)
        Last function to be executed when resolving the dask graph,
        producing the final output. It is always invoked, even when the reduced
        Array counts a single chunk along the reduced axes.
    axis: int or sequence of ints, optional
        Axis or axes to aggregate upon. If omitted, aggregate along all axes.
    keepdims: boolean, optional
        Whether the reduction function should preserve the reduced axes,
        leaving them at size ``output_size``, or remove them.
    dtype: np.dtype, optional
        Force output dtype. Defaults to x.dtype if omitted.
    split_every: int >= 2 or dict(axis: int), optional
        Determines the depth of the recursive aggregation. If set to or more
        than the number of input chunks, the aggregation will be performed in
        two steps, one ``chunk`` function per input chunk and a single
        ``aggregate`` function at the end. If set to less than that, an
        intermediate ``combine`` function will be used, so that any one
        ``combine`` or ``aggregate`` function has no more than ``split_every``
        inputs. The depth of the aggregation graph will be
        :math:`log_{split_every}(input chunks along reduced axes)`. Setting to
        a low value can reduce cache size and network transfers, at the cost of
        more CPU and a larger dask graph.

        Omit to let dask heuristically decide a good default. A default can
        also be set globally with the ``split_every`` key in
        :mod:`dask.config`.
    name: str, optional
        Prefix of the keys of the intermediate and output nodes. If omitted it
        defaults to the function names.
    out: Array, optional
        Another dask array whose contents will be replaced. Omit to create a
        new one. Note that, unlike in numpy, this setting gives no performance
        benefits whatsoever, but can still be useful  if one needs to preserve
        the references to a previously existing Array.
    concatenate: bool, optional
        If True (the default), the outputs of the ``chunk``/``combine``
        functions are concatenated into a single np.array before being passed
        to the ``combine``/``aggregate`` functions. If False, the input of
        ``combine`` and ``aggregate`` will be either a list of the raw outputs
        of the previous step or a single output, and the function will have to
        concatenate it itself. It can be useful to set this to False if the
        chunk and/or combine steps do not produce np.arrays.
    output_size: int >= 1, optional
        Size of the output of the ``aggregate`` function along the reduced
        axes. Ignored if keepdims is False.

    Returns
    -------
    dask array

    **Function Parameters**

    x_chunk: numpy.ndarray
        Individual input chunk. For ``chunk`` functions, it is one of the
        original chunks of x. For ``combine`` and ``aggregate`` functions, it's
        the concatenation of the outputs produced by the previous ``chunk`` or
        ``combine`` functions. If concatenate=False, it's a list of the raw
        outputs from the previous functions.
    axis: tuple
        Normalized list of axes to reduce upon, e.g. ``(0, )``
        Scalar, negative, and None axes have been normalized away.
        Note that some numpy reduction functions cannot reduce along multiple
        axes at once and strictly require an int in input. Such functions have
        to be wrapped to cope.
    keepdims: bool
        Whether the reduction function should preserve the reduced axes or
        remove them.
    """
    if axis is None:
        axis = tuple(range(x.ndim))
    if isinstance(axis, Integral):
        axis = (axis,)
    axis = validate_axis(axis, x.ndim)

    if dtype is None:
        raise ValueError("Must specify dtype")
    if 'dtype' in getargspec(chunk).args:
        chunk = partial(chunk, dtype=dtype)
    if 'dtype' in getargspec(aggregate).args:
        aggregate = partial(aggregate, dtype=dtype)

    # Map chunk across all blocks
    inds = tuple(range(x.ndim))
    # The dtype of `tmp` doesn't actually matter, and may be incorrect.
    tmp = atop(chunk, inds, x, inds, axis=axis, keepdims=True, dtype=x.dtype)
    tmp._chunks = tuple((output_size, ) * len(c) if i in axis else c
                        for i, c in enumerate(tmp.chunks))
    result = _tree_reduce(tmp, aggregate, axis, keepdims, dtype, split_every,
                          combine, name=name, concatenate=concatenate)
    if keepdims and output_size != 1:
        result._chunks = tuple((output_size, ) if i in axis else c
                               for i, c in enumerate(tmp.chunks))
    return handle_out(out, result)


def _tree_reduce(x, aggregate, axis, keepdims, dtype, split_every=None,
                 combine=None, name=None, concatenate=True):
    """ Perform the tree reduction step of a reduction.

    Lower level, users should use ``reduction`` or ``arg_reduction`` directly.
    """
    # Normalize split_every
    split_every = split_every or config.get('split_every', 4)
    if isinstance(split_every, dict):
        split_every = dict((k, split_every.get(k, 2)) for k in axis)
    elif isinstance(split_every, Integral):
        n = builtins.max(int(split_every ** (1 / (len(axis) or 1))), 2)
        split_every = dict.fromkeys(axis, n)
    else:
        raise ValueError("split_every must be a int or a dict")

    # Reduce across intermediates
    depth = 1
    for i, n in enumerate(x.numblocks):
        if i in split_every and split_every[i] != 1:
            depth = int(builtins.max(depth, ceil(log(n, split_every[i]))))
    func = partial(combine or aggregate, axis=axis, keepdims=True)
    if concatenate:
        func = compose(func, partial(_concatenate2, axes=axis))
    for i in range(depth - 1):
        x = partial_reduce(func, x, split_every, True, dtype=dtype,
                           name=(name or funcname(combine or aggregate)) + '-partial')
    func = partial(aggregate, axis=axis, keepdims=keepdims)
    if concatenate:
        func = compose(func, partial(_concatenate2, axes=axis))
    return partial_reduce(func, x, split_every, keepdims=keepdims, dtype=dtype,
                          name=(name or funcname(aggregate)) + '-aggregate')


def partial_reduce(func, x, split_every, keepdims=False, dtype=None, name=None):
    """ Partial reduction across multiple axes.

    Parameters
    ----------
    func : function
    x : Array
    split_every : dict
        Maximum reduction block sizes in each dimension.

    Examples
    --------
    Reduce across axis 0 and 2, merging a maximum of 1 block in the 0th
    dimension, and 3 blocks in the 2nd dimension:

    >>> partial_reduce(np.min, x, {0: 1, 2: 3})    # doctest: +SKIP
    """
    name = (name or funcname(func)) + '-' + tokenize(func, x, split_every,
                                                     keepdims, dtype)
    parts = [list(partition_all(split_every.get(i, 1), range(n))) for (i, n)
             in enumerate(x.numblocks)]
    keys = product(*map(range, map(len, parts)))
    out_chunks = [tuple(1 for p in partition_all(split_every[i], c)) if i
                  in split_every else c for (i, c) in enumerate(x.chunks)]
    if not keepdims:
        out_axis = [i for i in range(x.ndim) if i not in split_every]
        getter = lambda k: get(out_axis, k)
        keys = map(getter, keys)
        out_chunks = list(getter(out_chunks))
    dsk = {}
    for k, p in zip(keys, product(*parts)):
        decided = dict((i, j[0]) for (i, j) in enumerate(p) if len(j) == 1)
        dummy = dict(i for i in enumerate(p) if i[0] not in decided)
        g = lol_tuples((x.name,), range(x.ndim), decided, dummy)
        dsk[(name,) + k] = (func, g)
    return Array(sharedict.merge(x.dask, (name, dsk)), name, out_chunks, dtype=dtype)


@wraps(chunk.sum)
def sum(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.empty((1,), dtype=a.dtype).sum(), 'dtype', object)
    return reduction(a, chunk.sum, chunk.sum, axis=axis, keepdims=keepdims,
                     dtype=dt, split_every=split_every, out=out)


@wraps(chunk.prod)
def prod(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.empty((1,), dtype=a.dtype).prod(), 'dtype', object)
    return reduction(a, chunk.prod, chunk.prod, axis=axis, keepdims=keepdims,
                     dtype=dt, split_every=split_every, out=out)


@wraps(chunk.min)
def min(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(a, chunk.min, chunk.min, axis=axis, keepdims=keepdims,
                     dtype=a.dtype, split_every=split_every, out=out)


@wraps(chunk.max)
def max(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(a, chunk.max, chunk.max, axis=axis, keepdims=keepdims,
                     dtype=a.dtype, split_every=split_every, out=out)


@wraps(chunk.any)
def any(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(a, chunk.any, chunk.any, axis=axis, keepdims=keepdims,
                     dtype='bool', split_every=split_every, out=out)


@wraps(chunk.all)
def all(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(a, chunk.all, chunk.all, axis=axis, keepdims=keepdims,
                     dtype='bool', split_every=split_every, out=out)


@wraps(chunk.nansum)
def nansum(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(chunk.nansum(np.empty((1,), dtype=a.dtype)), 'dtype', object)
    return reduction(a, chunk.nansum, chunk.sum, axis=axis, keepdims=keepdims,
                     dtype=dt, split_every=split_every, out=out)


with ignoring(AttributeError):
    @wraps(chunk.nanprod)
    def nanprod(a, axis=None, dtype=None, keepdims=False, split_every=None,
                out=None):
        if dtype is not None:
            dt = dtype
        else:
            dt = getattr(chunk.nansum(np.empty((1,), dtype=a.dtype)), 'dtype', object)
        return reduction(a, chunk.nanprod, chunk.prod, axis=axis,
                         keepdims=keepdims, dtype=dt, split_every=split_every,
                         out=out)

    @wraps(chunk.nancumsum)
    def nancumsum(x, axis, dtype=None, out=None):
        return cumreduction(chunk.nancumsum, operator.add, 0, x, axis, dtype,
                            out=out)

    @wraps(chunk.nancumprod)
    def nancumprod(x, axis, dtype=None, out=None):
        return cumreduction(chunk.nancumprod, operator.mul, 1, x, axis, dtype,
                            out=out)


@wraps(chunk.nanmin)
def nanmin(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(a, chunk.nanmin, chunk.nanmin, axis=axis,
                     keepdims=keepdims, dtype=a.dtype, split_every=split_every,
                     out=out)


@wraps(chunk.nanmax)
def nanmax(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(a, chunk.nanmax, chunk.nanmax, axis=axis,
                     keepdims=keepdims, dtype=a.dtype, split_every=split_every,
                     out=out)


def numel(x, **kwargs):
    """ A reduction to count the number of elements """
    return chunk.sum(np.ones_like(x), **kwargs)


def nannumel(x, **kwargs):
    """ A reduction to count the number of elements """
    return chunk.sum(~np.isnan(x), **kwargs)


def mean_chunk(x, sum=chunk.sum, numel=numel, dtype='f8', **kwargs):
    n = numel(x, dtype=dtype, **kwargs)
    total = sum(x, dtype=dtype, **kwargs)
    empty = empty_lookup.dispatch(type(n))
    result = empty(n.shape, dtype=[('total', total.dtype), ('n', n.dtype)])
    result['n'] = n
    result['total'] = total
    return result


def mean_combine(pair, sum=chunk.sum, numel=numel, dtype='f8', **kwargs):
    n = sum(pair['n'], **kwargs)
    total = sum(pair['total'], **kwargs)
    empty = empty_lookup.dispatch(type(n))
    result = empty(n.shape, dtype=pair.dtype)
    result['n'] = n
    result['total'] = total
    return result


def mean_agg(pair, dtype='f8', **kwargs):
    return divide(pair['total'].sum(dtype=dtype, **kwargs),
                  pair['n'].sum(dtype=dtype, **kwargs), dtype=dtype)


@wraps(chunk.mean)
def mean(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.mean(np.empty(shape=(1,), dtype=a.dtype)), 'dtype', object)
    return reduction(a, mean_chunk, mean_agg, axis=axis, keepdims=keepdims,
                     dtype=dt, split_every=split_every, combine=mean_combine,
                     out=out)


def nanmean(a, axis=None, dtype=None, keepdims=False, split_every=None,
            out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.mean(np.empty(shape=(1,), dtype=a.dtype)), 'dtype', object)
    return reduction(a, partial(mean_chunk, sum=chunk.nansum, numel=nannumel),
                     mean_agg, axis=axis, keepdims=keepdims, dtype=dt,
                     split_every=split_every, out=out,
                     combine=partial(mean_combine, sum=chunk.nansum, numel=nannumel))


with ignoring(AttributeError):
    nanmean = wraps(chunk.nanmean)(nanmean)


def moment_chunk(A, order=2, sum=chunk.sum, numel=numel, dtype='f8', **kwargs):
    total = sum(A, dtype=dtype, **kwargs)
    n = numel(A, **kwargs).astype(np.int64)
    u = total / n
    empty = empty_lookup.dispatch(type(n))
    M = empty(n.shape + (order - 1,), dtype=dtype)
    for i in range(2, order + 1):
        M[..., i - 2] = sum((A - u)**i, dtype=dtype, **kwargs)
    result = empty(n.shape, dtype=[('total', total.dtype),
                                   ('n', n.dtype),
                                   ('M', M.dtype, (order - 1,))])
    result['total'] = total
    result['n'] = n
    result['M'] = M
    return result


def _moment_helper(Ms, ns, inner_term, order, sum, kwargs):
    M = Ms[..., order - 2].sum(**kwargs) + sum(ns * inner_term ** order, **kwargs)
    for k in range(1, order - 1):
        coeff = factorial(order) / (factorial(k) * factorial(order - k))
        M += coeff * sum(Ms[..., order - k - 2] * inner_term**k, **kwargs)
    return M


def moment_combine(data, order=2, ddof=0, dtype='f8', sum=np.sum, **kwargs):
    kwargs['dtype'] = dtype
    kwargs['keepdims'] = True

    totals = data['total']
    ns = data['n']
    Ms = data['M']
    total = totals.sum(**kwargs)
    n = sum(ns, **kwargs)
    mu = divide(total, n, dtype=dtype)
    inner_term = divide(totals, ns, dtype=dtype) - mu
    empty = empty_lookup.dispatch(type(n))
    M = empty(n.shape + (order - 1,), dtype=dtype)

    for o in range(2, order + 1):
        M[..., o - 2] = _moment_helper(Ms, ns, inner_term, o, sum, kwargs)

    result = empty(n.shape, dtype=[('total', total.dtype),
                                   ('n', n.dtype),
                                   ('M', Ms.dtype, (order - 1,))])
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

    M = _moment_helper(Ms, ns, inner_term, order, sum, kwargs)
    return divide(M, sum(n, **kwargs) - ddof, dtype=dtype)


def moment(a, order, axis=None, dtype=None, keepdims=False, ddof=0,
           split_every=None, out=None):
    if not isinstance(order, Integral) or order < 0:
        raise ValueError("Order must be an integer >= 0")

    if order < 2:
        reduced = a.sum(axis=axis)   # get reduced shape and chunks
        if order == 0:
            # When order equals 0, the result is 1, by definition.
            return ones(reduced.shape, chunks=reduced.chunks, dtype='f8')
        # By definition the first order about the mean is 0.
        return zeros(reduced.shape, chunks=reduced.chunks, dtype='f8')

    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.var(np.ones(shape=(1,), dtype=a.dtype)), 'dtype', object)
    return reduction(a, partial(moment_chunk, order=order),
                     partial(moment_agg, order=order, ddof=ddof),
                     axis=axis, keepdims=keepdims,
                     dtype=dt, split_every=split_every, out=out,
                     combine=partial(moment_combine, order=order))


@wraps(chunk.var)
def var(a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None,
        out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.var(np.ones(shape=(1,), dtype=a.dtype)), 'dtype', object)
    return reduction(a, moment_chunk, partial(moment_agg, ddof=ddof), axis=axis,
                     keepdims=keepdims, dtype=dt, split_every=split_every,
                     combine=moment_combine, name='var', out=out)


def nanvar(a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None,
           out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.var(np.ones(shape=(1,), dtype=a.dtype)), 'dtype', object)
    return reduction(a, partial(moment_chunk, sum=chunk.nansum, numel=nannumel),
                     partial(moment_agg, sum=np.nansum, ddof=ddof), axis=axis,
                     keepdims=keepdims, dtype=dt, split_every=split_every,
                     combine=partial(moment_combine, sum=np.nansum), out=out)


with ignoring(AttributeError):
    nanvar = wraps(chunk.nanvar)(nanvar)


@wraps(chunk.std)
def std(a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None,
        out=None):
    result = sqrt(a.var(axis=axis, dtype=dtype, keepdims=keepdims, ddof=ddof,
                        split_every=split_every, out=out))
    if dtype and dtype != result.dtype:
        result = result.astype(dtype)
    return result


def nanstd(a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None,
           out=None):
    result = sqrt(nanvar(a, axis=axis, dtype=dtype, keepdims=keepdims,
                         ddof=ddof, split_every=split_every, out=out))
    if dtype and dtype != result.dtype:
        result = result.astype(dtype)
    return result


with ignoring(AttributeError):
    nanstd = wraps(chunk.nanstd)(nanstd)


def _arg_combine(data, axis, argfunc, keepdims=False):
    """ Merge intermediate results from ``arg_*`` functions"""
    axis = None if len(axis) == data.ndim or data.ndim == 1 else axis[0]
    vals = data['vals']
    arg = data['arg']
    if axis is None:
        local_args = argfunc(vals, axis=axis, keepdims=keepdims)
        vals = vals.ravel()[local_args]
        arg = arg.ravel()[local_args]
    else:
        local_args = argfunc(vals, axis=axis)
        inds = np.ogrid[tuple(map(slice, local_args.shape))]
        inds.insert(axis, local_args)
        inds = tuple(inds)
        vals = vals[inds]
        arg = arg[inds]
        if keepdims:
            vals = np.expand_dims(vals, axis)
            arg = np.expand_dims(arg, axis)
    return arg, vals


def arg_chunk(func, argfunc, x, axis, offset_info):
    arg_axis = None if len(axis) == x.ndim or x.ndim == 1 else axis[0]
    vals = func(x, axis=arg_axis, keepdims=True)
    arg = argfunc(x, axis=arg_axis, keepdims=True)
    if arg_axis is None:
        offset, total_shape = offset_info
        ind = np.unravel_index(arg.ravel()[0], x.shape)
        total_ind = tuple(o + i for (o, i) in zip(offset, ind))
        arg[:] = np.ravel_multi_index(total_ind, total_shape)
    else:
        arg += offset_info

    if isinstance(vals, np.ma.masked_array):
        if 'min' in argfunc.__name__:
            fill_value = np.ma.minimum_fill_value(vals)
        else:
            fill_value = np.ma.maximum_fill_value(vals)
        vals = np.ma.filled(vals, fill_value)

    result = np.empty(shape=vals.shape, dtype=[('vals', vals.dtype),
                                               ('arg', arg.dtype)])
    result['vals'] = vals
    result['arg'] = arg
    return result


def arg_combine(func, argfunc, data, axis=None, **kwargs):
    arg, vals = _arg_combine(data, axis, argfunc, keepdims=True)
    result = np.empty(shape=vals.shape, dtype=[('vals', vals.dtype),
                                               ('arg', arg.dtype)])
    result['vals'] = vals
    result['arg'] = arg
    return result


def arg_agg(func, argfunc, data, axis=None, **kwargs):
    return _arg_combine(data, axis, argfunc, keepdims=False)[0]


def nanarg_agg(func, argfunc, data, axis=None, **kwargs):
    arg, vals = _arg_combine(data, axis, argfunc, keepdims=False)
    if np.any(np.isnan(vals)):
        raise ValueError("All NaN slice encountered")
    return arg


def arg_reduction(x, chunk, combine, agg, axis=None, split_every=None, out=None):
    """ Generic function for argreduction.

    Parameters
    ----------
    x : Array
    chunk : callable
        Partialed ``arg_chunk``.
    combine : callable
        Partialed ``arg_combine``.
    agg : callable
        Partialed ``arg_agg``.
    axis : int, optional
    split_every : int or dict, optional
    """
    if axis is None:
        axis = tuple(range(x.ndim))
        ravel = True
    elif isinstance(axis, Integral):
        axis = validate_axis(axis, x.ndim)
        axis = (axis,)
        ravel = x.ndim == 1
    else:
        raise TypeError("axis must be either `None` or int, "
                        "got '{0}'".format(axis))

    # Map chunk across all blocks
    name = 'arg-reduce-{0}'.format(tokenize(axis, x, chunk,
                                            combine, split_every))
    old = x.name
    keys = list(product(*map(range, x.numblocks)))
    offsets = list(product(*(accumulate(operator.add, bd[:-1], 0)
                             for bd in x.chunks)))
    if ravel:
        offset_info = zip(offsets, repeat(x.shape))
    else:
        offset_info = pluck(axis[0], offsets)

    chunks = tuple((1, ) * len(c) if i in axis else c for (i, c)
                   in enumerate(x.chunks))
    dsk = dict(((name,) + k, (chunk, (old,) + k, axis, off)) for (k, off)
               in zip(keys, offset_info))
    # The dtype of `tmp` doesn't actually matter, just need to provide something
    tmp = Array(sharedict.merge(x.dask, (name, dsk)), name, chunks, dtype=x.dtype)
    dtype = np.argmin([1]).dtype
    result = _tree_reduce(tmp, agg, axis, False, dtype, split_every, combine)
    return handle_out(out, result)


def make_arg_reduction(func, argfunc, is_nan_func=False):
    """ Create an argreduction callable

    Parameters
    ----------
    func : callable
        The reduction (e.g. ``min``)
    argfunc : callable
        The argreduction (e.g. ``argmin``)
    """
    chunk = partial(arg_chunk, func, argfunc)
    combine = partial(arg_combine, func, argfunc)
    if is_nan_func:
        agg = partial(nanarg_agg, func, argfunc)
    else:
        agg = partial(arg_agg, func, argfunc)

    @wraps(argfunc)
    def _(x, axis=None, split_every=None, out=None):
        return arg_reduction(x, chunk, combine, agg, axis,
                             split_every=split_every, out=out)

    return _


def _nanargmin(x, axis, **kwargs):
    try:
        return chunk.nanargmin(x, axis, **kwargs)
    except ValueError:
        return chunk.nanargmin(np.where(np.isnan(x), np.inf, x), axis, **kwargs)


def _nanargmax(x, axis, **kwargs):
    try:
        return chunk.nanargmax(x, axis, **kwargs)
    except ValueError:
        return chunk.nanargmax(np.where(np.isnan(x), -np.inf, x), axis, **kwargs)


argmin = make_arg_reduction(chunk.min, chunk.argmin)
argmax = make_arg_reduction(chunk.max, chunk.argmax)
nanargmin = make_arg_reduction(chunk.nanmin, _nanargmin, True)
nanargmax = make_arg_reduction(chunk.nanmax, _nanargmax, True)


def cumreduction(func, binop, ident, x, axis=None, dtype=None, out=None):
    """ Generic function for cumulative reduction

    Parameters
    ----------
    func: callable
        Cumulative function like np.cumsum or np.cumprod
    binop: callable
        Associated binary operator like ``np.cumsum->add`` or ``np.cumprod->mul``
    ident: Number
        Associated identity like ``np.cumsum->0`` or ``np.cumprod->1``
    x: dask Array
    axis: int
    dtype: dtype

    Returns
    -------
    dask array

    See also
    --------
    cumsum
    cumprod
    """
    if axis is None:
        x = x.flatten()
        axis = 0
    if dtype is None:
        dtype = getattr(func(np.empty((0,), dtype=x.dtype)), 'dtype', object)
    assert isinstance(axis, Integral)
    axis = validate_axis(axis, x.ndim)

    m = x.map_blocks(func, axis=axis, dtype=dtype)

    name = '{0}-{1}'.format(func.__name__, tokenize(func, axis, binop,
                                                    ident, x, dtype))
    n = x.numblocks[axis]
    full = slice(None, None, None)
    slc = (full,) * axis + (slice(-1, None),) + (full,) * (x.ndim - axis - 1)

    indices = list(product(*[range(nb) if i != axis else [0]
                             for i, nb in enumerate(x.numblocks)]))
    dsk = dict()
    for ind in indices:
        shape = tuple(x.chunks[i][ii] if i != axis else 1
                      for i, ii in enumerate(ind))
        dsk[(name, 'extra') + ind] = (np.full, shape, ident, m.dtype)
        dsk[(name,) + ind] = (m.name,) + ind

    for i in range(1, n):
        last_indices = indices
        indices = list(product(*[range(nb) if ii != axis else [i]
                                 for ii, nb in enumerate(x.numblocks)]))
        for old, ind in zip(last_indices, indices):
            this_slice = (name, 'extra') + ind
            dsk[this_slice] = (binop, (name, 'extra') + old,
                                      (operator.getitem, (m.name,) + old, slc))
            dsk[(name,) + ind] = (binop, this_slice, (m.name,) + ind)

    result = Array(sharedict.merge(m.dask, (name, dsk)), name, x.chunks, m.dtype)
    return handle_out(out, result)


def _cumsum_merge(a, b):
    if isinstance(a, np.ma.masked_array) or isinstance(b, np.ma.masked_array):
        values = np.ma.getdata(a) + np.ma.getdata(b)
        return np.ma.masked_array(values, mask=np.ma.getmaskarray(b))
    return a + b


def _cumprod_merge(a, b):
    if isinstance(a, np.ma.masked_array) or isinstance(b, np.ma.masked_array):
        values = np.ma.getdata(a) * np.ma.getdata(b)
        return np.ma.masked_array(values, mask=np.ma.getmaskarray(b))
    return a * b


@wraps(np.cumsum)
def cumsum(x, axis=None, dtype=None, out=None):
    return cumreduction(np.cumsum, _cumsum_merge, 0, x, axis, dtype, out=out)


@wraps(np.cumprod)
def cumprod(x, axis=None, dtype=None, out=None):
    return cumreduction(np.cumprod, _cumprod_merge, 1, x, axis, dtype, out=out)


def topk(a, k, axis=-1, split_every=None):
    """ Extract the k largest elements from a on the given axis,
    and return them sorted from largest to smallest.
    If k is negative, extract the -k smallest elements instead,
    and return them sorted from smallest to largest.

    This performs best when ``k`` is much smaller than the chunk size. All
    results will be returned in a single chunk along the given axis.

    Parameters
    ----------
    x: Array
        Data being sorted
    k: int
    axis: int, optional
    split_every: int >=2, optional
        See :func:`reduce`. This parameter becomes very important when k is
        on the same order of magnitude of the chunk size or more, as it
        prevents getting the whole or a significant portion of the input array
        in memory all at once, with a negative impact on network transfer
        too when running on distributed.

    Returns
    -------
    Selection of x with size abs(k) along the given axis.

    Examples
    --------
    >>> import dask.array as da
    >>> x = np.array([5, 1, 3, 6])
    >>> d = da.from_array(x, chunks=2)
    >>> d.topk(2).compute()
    array([6, 5])
    >>> d.topk(-2).compute()
    array([1, 3])
    """
    axis = validate_axis(axis, a.ndim)

    # chunk and combine steps of the reduction, which recursively invoke
    # np.partition to pick the top/bottom k elements from the previous step.
    # The selection is not sorted internally.
    chunk_combine = partial(chunk.topk, k=k)
    # aggregate step of the reduction. Internally invokes the chunk/combine
    # function, then sorts the results internally.
    aggregate = partial(chunk.topk_aggregate, k=k)

    return reduction(
        a, chunk=chunk_combine, combine=chunk_combine, aggregate=aggregate,
        axis=axis, keepdims=True, dtype=a.dtype, split_every=split_every,
        output_size=abs(k))


def argtopk(a, k, axis=-1, split_every=None):
    """ Extract the indices of the k largest elements from a on the given axis,
    and return them sorted from largest to smallest. If k is negative, extract
    the indices of the -k smallest elements instead, and return them sorted
    from smallest to largest.

    This performs best when ``k`` is much smaller than the chunk size. All
    results will be returned in a single chunk along the given axis.

    Parameters
    ----------
    x: Array
        Data being sorted
    k: int
    axis: int, optional
    split_every: int >=2, optional
        See :func:`topk`. The performance considerations for topk also apply
        here.

    Returns
    -------
    Selection of np.intp indices of x with size abs(k) along the given axis.

    Examples
    --------
    >>> import dask.array as da
    >>> x = np.array([5, 1, 3, 6])
    >>> d = da.from_array(x, chunks=2)
    >>> d.argtopk(2).compute()
    array([3, 0])
    >>> d.argtopk(-2).compute()
    array([1, 2])
    """
    axis = validate_axis(axis, a.ndim)

    # Generate nodes where every chunk is a tuple of (a, original index of a)
    idx = arange(a.shape[axis], chunks=(a.chunks[axis], ), dtype=np.intp)
    idx = idx[tuple(slice(None) if i == axis else np.newaxis
                    for i in range(a.ndim))]
    a_plus_idx = a.map_blocks(chunk.argtopk_preprocess, idx,
                              dtype=object)

    # chunk and combine steps of the reduction. They acquire in input a tuple
    # of (a, original indices of a) and return another tuple containing the top
    # k elements of a and the matching original indices. The selection is not
    # sorted internally, as in np.argpartition.
    chunk_combine = partial(chunk.argtopk, k=k)
    # aggregate step of the reduction. Internally invokes the chunk/combine
    # function, then sorts the results internally, drops a and returns the
    # index only.
    aggregate = partial(chunk.argtopk_aggregate, k=k)

    return reduction(
        a_plus_idx, chunk=chunk_combine, combine=chunk_combine,
        aggregate=aggregate, axis=axis, keepdims=True, dtype=np.intp,
        split_every=split_every, concatenate=False, output_size=abs(k))

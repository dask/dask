from __future__ import annotations

import builtins
import math
from functools import partial
from itertools import product
from numbers import Integral, Number

import numpy as np
from dask import config
from dask.array import chunk
from dask.array.core import _concatenate2, asanyarray, broadcast_to, implements
from dask.array.dispatch import divide_lookup, nannumel_lookup, numel_lookup
from dask.array.reductions import array_safe
from dask.array.utils import compute_meta, is_arraylike, validate_axis
from dask.base import tokenize
from dask.blockwise import lol_tuples
from dask.utils import (
    cached_property,
    deepmap,
    derived_from,
    funcname,
    getargspec,
    is_series_like,
)
from tlz import compose, get, partition_all

from dask_expr.array.core import Array


# TODO: it would be good to have a higher level reduction operation that
# lowered down into the partial reduces below.
# It might also make sense to wrap all of the partial reduces into a single
# TreeReduce object.  They pollute the expression a bit.
def reduction(
    x,
    chunk,
    aggregate,
    axis=None,
    keepdims=False,
    dtype=None,
    split_every=None,
    combine=None,
    name=None,
    out=None,
    concatenate=True,
    output_size=1,
    meta=None,
    weights=None,
):
    """General version of reductions

    Parameters
    ----------
    x: Array
        Data being reduced along one or more axes
    chunk: callable(x_chunk, [weights_chunk=None], axis, keepdims)
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
    dtype: np.dtype
        data type of output. This argument was previously optional, but
        leaving as ``None`` will now raise an exception.
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
    weights : array_like, optional
        Weights to be used in the reduction of `x`. Will be
        automatically broadcast to the shape of `x`, and so must have
        a compatible shape. For instance, if `x` has shape ``(3, 4)``
        then acceptable shapes for `weights` are ``(3, 4)``, ``(4,)``,
        ``(3, 1)``, ``(1, 1)``, ``(1)``, and ``()``.

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
    weights_chunk: numpy.ndarray, optional
        Only applicable to the ``chunk`` function. Weights, with the
        same shape as `x_chunk`, to be applied during the reduction of
        the individual input chunk. If ``weights`` have not been
        provided then the function may omit this parameter. When
        `weights_chunk` is included then it must occur immediately
        after the `x_chunk` parameter, and must also have a default
        value for cases when ``weights`` are not provided.
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
    if "dtype" in getargspec(chunk).args:
        chunk = partial(chunk, dtype=dtype)
    if "dtype" in getargspec(aggregate).args:
        aggregate = partial(aggregate, dtype=dtype)
    if is_series_like(x):
        x = x.values

    # Map chunk across all blocks
    inds = tuple(range(x.ndim))

    args = (x, inds)

    # TODO: I'm ignoring this for now
    if weights is not None:
        # Broadcast weights to x and add to args
        wgt = asanyarray(weights)
        try:
            wgt = broadcast_to(wgt, x.shape)
        except ValueError:
            raise ValueError(
                f"Weights with shape {wgt.shape} are not broadcastable "
                f"to x with shape {x.shape}"
            )

        args += (wgt, inds)

    # The dtype of `tmp` doesn't actually matter, and may be incorrect.
    tmp = blockwise(
        chunk, inds, *args, axis=axis, keepdims=True, token=name, dtype=dtype or float
    )
    # TODO: this is going to be strange
    tmp._chunks = tuple(
        (output_size,) * len(c) if i in axis else c for i, c in enumerate(tmp.chunks)
    )

    if meta is None and hasattr(x, "_meta"):
        try:
            reduced_meta = compute_meta(
                chunk, x.dtype, x._meta, axis=axis, keepdims=True, computing_meta=True
            )
        except TypeError:
            reduced_meta = compute_meta(
                chunk, x.dtype, x._meta, axis=axis, keepdims=True
            )
        except ValueError:
            pass
    else:
        reduced_meta = None

    result = _tree_reduce(
        tmp,
        aggregate,
        axis,
        keepdims,
        dtype,
        split_every,
        combine,
        name=name,
        concatenate=concatenate,
        reduced_meta=reduced_meta,
    )
    # TODO: forced chunks
    if keepdims and output_size != 1:
        result._chunks = tuple(
            (output_size,) if i in axis else c for i, c in enumerate(tmp.chunks)
        )
    # TODO: forced meta
    if meta is not None:
        result._meta = meta
    return result
    # return handle_out(out, result)


def _tree_reduce(
    x,
    aggregate,
    axis,
    keepdims,
    dtype,
    split_every=None,
    combine=None,
    name=None,
    concatenate=True,
    reduced_meta=None,
):
    """Perform the tree reduction step of a reduction.

    Lower level, users should use ``reduction`` or ``arg_reduction`` directly.
    """
    # Normalize split_every
    split_every = split_every or config.get("split_every", 16)
    if isinstance(split_every, dict):
        split_every = {k: split_every.get(k, 2) for k in axis}
    elif isinstance(split_every, Integral):
        n = builtins.max(int(split_every ** (1 / (len(axis) or 1))), 2)
        split_every = dict.fromkeys(axis, n)
    else:
        raise ValueError("split_every must be a int or a dict")

    # Reduce across intermediates
    depth = 1
    for i, n in enumerate(x.numblocks):
        if i in split_every and split_every[i] != 1:
            depth = int(builtins.max(depth, math.ceil(math.log(n, split_every[i]))))
    func = partial(combine or aggregate, axis=axis, keepdims=True)
    if concatenate:
        func = compose(func, partial(_concatenate2, axes=sorted(axis)))
    for _ in range(depth - 1):
        x = PartialReduce(
            x,
            func,
            split_every,
            True,
            dtype=dtype,
            name=(name or funcname(combine or aggregate)) + "-partial",
            reduced_meta=reduced_meta,
        )
    func = partial(aggregate, axis=axis, keepdims=keepdims)
    if concatenate:
        func = compose(func, partial(_concatenate2, axes=sorted(axis)))
    return PartialReduce(
        x,
        func,
        split_every,
        keepdims=keepdims,
        dtype=dtype,
        name=(name or funcname(aggregate)) + "-aggregate",
        reduced_meta=reduced_meta,
    )


class PartialReduce(Array):
    _parameters = [
        "array",
        "func",
        "split_every",
        "keepdims",
        "dtype",
        "name",
        "reduced_meta",
    ]
    _defaults = {
        "keepdims": False,
        "dtype": None,
        "name": None,
        "reduced_meta": None,
    }

    @cached_property
    def _name(self):
        return (
            (self.operand("name") or funcname(self.func))
            + "-"
            + tokenize(
                self.func, self.array, self.split_every, self.keepdims, self.dtype
            )
        )

    @cached_property
    def chunks(self):
        chunks = [
            tuple(1 for p in partition_all(self.split_every[i], c))
            if i in self.split_every
            else c
            for (i, c) in enumerate(self.array.chunks)
        ]

        if not self.keepdims:
            out_axis = [i for i in range(self.array.ndim) if i not in self.split_every]
            getter = lambda k: get(out_axis, k)
            chunks = list(getter(chunks))

        return tuple(chunks)

    def _layer(self):
        x = self.array
        parts = [
            list(partition_all(self.split_every.get(i, 1), range(n)))
            for (i, n) in enumerate(x.numblocks)
        ]
        keys = product(*map(range, map(len, parts)))
        if not self.keepdims:
            out_axis = [i for i in range(x.ndim) if i not in self.split_every]
            getter = lambda k: get(out_axis, k)
            keys = map(getter, keys)
        dsk = {}
        for k, p in zip(keys, product(*parts)):
            free = {
                i: j[0]
                for (i, j) in enumerate(p)
                if len(j) == 1 and i not in self.split_every
            }
            dummy = dict(i for i in enumerate(p) if i[0] in self.split_every)
            g = lol_tuples((x.name,), range(x.ndim), free, dummy)
            dsk[(self._name,) + k] = (self.func, g)

        return dsk

    @property
    def _meta(self):
        meta = self.array._meta
        if self.reduced_meta is not None:
            try:
                meta = self.func(self.reduced_meta, computing_meta=True)
            # no meta keyword argument exists for func, and it isn't required
            except TypeError:
                try:
                    meta = self.func(self.reduced_meta)
                except ValueError as e:
                    # min/max functions have no identity, don't apply function to meta
                    if "zero-size array to reduction operation" in str(e):
                        meta = self.reduced_meta
            # when no work can be computed on the empty array (e.g., func is a ufunc)
            except ValueError:
                pass

        # some functions can't compute empty arrays (those for which reduced_meta
        # fall into the ValueError exception) and we have to rely on reshaping
        # the array according to len(out_chunks)
        if is_arraylike(meta) and meta.ndim != len(self.chunks):
            if len(self.chunks) == 0:
                meta = meta.sum()
            else:
                meta = meta.reshape((0,) * len(self.chunks))

        return meta


@derived_from(np)
def sum(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is None:
        dtype = getattr(np.zeros(1, dtype=a.dtype).sum(), "dtype", object)
    result = reduction(
        a,
        chunk.sum,
        chunk.sum,
        axis=axis,
        keepdims=keepdims,
        dtype=dtype,
        split_every=split_every,
        out=out,
    )
    return result


@derived_from(np)
def prod(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.ones((1,), dtype=a.dtype).prod(), "dtype", object)
    return reduction(
        a,
        chunk.prod,
        chunk.prod,
        axis=axis,
        keepdims=keepdims,
        dtype=dt,
        split_every=split_every,
        out=out,
    )


@implements(np.min, np.amin)
@derived_from(np)
def min(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(
        a,
        chunk_min,
        chunk.min,
        combine=chunk_min,
        axis=axis,
        keepdims=keepdims,
        dtype=a.dtype,
        split_every=split_every,
        out=out,
    )


def chunk_min(x, axis=None, keepdims=None):
    """Version of np.min which ignores size 0 arrays"""
    if x.size == 0:
        return array_safe([], x, ndmin=x.ndim, dtype=x.dtype)
    else:
        return np.min(x, axis=axis, keepdims=keepdims)


@implements(np.max, np.amax)
@derived_from(np)
def max(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(
        a,
        chunk_max,
        chunk.max,
        combine=chunk_max,
        axis=axis,
        keepdims=keepdims,
        dtype=a.dtype,
        split_every=split_every,
        out=out,
    )


def chunk_max(x, axis=None, keepdims=None):
    """Version of np.max which ignores size 0 arrays"""
    if x.size == 0:
        return array_safe([], x, ndmin=x.ndim, dtype=x.dtype)
    else:
        return np.max(x, axis=axis, keepdims=keepdims)


@derived_from(np)
def any(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(
        a,
        chunk.any,
        chunk.any,
        axis=axis,
        keepdims=keepdims,
        dtype="bool",
        split_every=split_every,
        out=out,
    )


@derived_from(np)
def all(a, axis=None, keepdims=False, split_every=None, out=None):
    return reduction(
        a,
        chunk.all,
        chunk.all,
        axis=axis,
        keepdims=keepdims,
        dtype="bool",
        split_every=split_every,
        out=out,
    )


@derived_from(np)
def nansum(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(chunk.nansum(np.ones((1,), dtype=a.dtype)), "dtype", object)
    return reduction(
        a,
        chunk.nansum,
        chunk.sum,
        axis=axis,
        keepdims=keepdims,
        dtype=dt,
        split_every=split_every,
        out=out,
    )


def divide(a, b, dtype=None):
    key = lambda x: getattr(x, "__array_priority__", float("-inf"))
    f = divide_lookup.dispatch(type(builtins.max(a, b, key=key)))
    return f(a, b, dtype=dtype)


def numel(x, **kwargs):
    return numel_lookup(x, **kwargs)


def nannumel(x, **kwargs):
    return nannumel_lookup(x, **kwargs)


def mean_chunk(
    x, sum=chunk.sum, numel=numel, dtype="f8", computing_meta=False, **kwargs
):
    if computing_meta:
        return x
    n = numel(x, dtype=dtype, **kwargs)

    total = sum(x, dtype=dtype, **kwargs)

    return {"n": n, "total": total}


def mean_combine(
    pairs,
    sum=chunk.sum,
    numel=numel,
    dtype="f8",
    axis=None,
    computing_meta=False,
    **kwargs,
):
    if not isinstance(pairs, list):
        pairs = [pairs]

    ns = deepmap(lambda pair: pair["n"], pairs) if not computing_meta else pairs
    n = _concatenate2(ns, axes=axis).sum(axis=axis, **kwargs)

    if computing_meta:
        return n

    totals = deepmap(lambda pair: pair["total"], pairs)
    total = _concatenate2(totals, axes=axis).sum(axis=axis, **kwargs)

    return {"n": n, "total": total}


def mean_agg(pairs, dtype="f8", axis=None, computing_meta=False, **kwargs):
    ns = deepmap(lambda pair: pair["n"], pairs) if not computing_meta else pairs
    n = _concatenate2(ns, axes=axis)
    n = np.sum(n, axis=axis, dtype=dtype, **kwargs)

    if computing_meta:
        return n

    totals = deepmap(lambda pair: pair["total"], pairs)
    total = _concatenate2(totals, axes=axis).sum(axis=axis, dtype=dtype, **kwargs)

    with np.errstate(divide="ignore", invalid="ignore"):
        return divide(total, n, dtype=dtype)


@derived_from(np)
def mean(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    elif a.dtype == object:
        dt = object
    else:
        dt = getattr(np.mean(np.zeros(shape=(1,), dtype=a.dtype)), "dtype", object)
    return reduction(
        a,
        mean_chunk,
        mean_agg,
        axis=axis,
        keepdims=keepdims,
        dtype=dt,
        split_every=split_every,
        combine=mean_combine,
        out=out,
        concatenate=False,
    )


@derived_from(np)
def nanmean(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.mean(np.ones(shape=(1,), dtype=a.dtype)), "dtype", object)
    return reduction(
        a,
        partial(mean_chunk, sum=chunk.nansum, numel=nannumel),
        mean_agg,
        axis=axis,
        keepdims=keepdims,
        dtype=dt,
        split_every=split_every,
        out=out,
        concatenate=False,
        combine=partial(mean_combine, sum=chunk.nansum, numel=nannumel),
    )


def moment_chunk(
    A,
    order=2,
    sum=chunk.sum,
    numel=numel,
    dtype="f8",
    computing_meta=False,
    implicit_complex_dtype=False,
    **kwargs,
):
    if computing_meta:
        return A
    n = numel(A, **kwargs)

    n = n.astype(np.int64)
    if implicit_complex_dtype:
        total = sum(A, **kwargs)
    else:
        total = sum(A, dtype=dtype, **kwargs)

    with np.errstate(divide="ignore", invalid="ignore"):
        u = total / n
    d = A - u
    if np.issubdtype(A.dtype, np.complexfloating):
        d = np.abs(d)
    xs = [sum(d**i, dtype=dtype, **kwargs) for i in range(2, order + 1)]
    M = np.stack(xs, axis=-1)
    return {"total": total, "n": n, "M": M}


def _moment_helper(Ms, ns, inner_term, order, sum, axis, kwargs):
    M = Ms[..., order - 2].sum(axis=axis, **kwargs) + sum(
        ns * inner_term**order, axis=axis, **kwargs
    )
    for k in range(1, order - 1):
        coeff = math.factorial(order) / (math.factorial(k) * math.factorial(order - k))
        M += coeff * sum(Ms[..., order - k - 2] * inner_term**k, axis=axis, **kwargs)
    return M


def moment_combine(
    pairs,
    order=2,
    ddof=0,
    dtype="f8",
    sum=np.sum,
    axis=None,
    computing_meta=False,
    **kwargs,
):
    if not isinstance(pairs, list):
        pairs = [pairs]

    kwargs["dtype"] = None
    kwargs["keepdims"] = True

    ns = deepmap(lambda pair: pair["n"], pairs) if not computing_meta else pairs
    ns = _concatenate2(ns, axes=axis)
    n = ns.sum(axis=axis, **kwargs)

    if computing_meta:
        return n

    totals = _concatenate2(deepmap(lambda pair: pair["total"], pairs), axes=axis)
    Ms = _concatenate2(deepmap(lambda pair: pair["M"], pairs), axes=axis)

    total = totals.sum(axis=axis, **kwargs)

    with np.errstate(divide="ignore", invalid="ignore"):
        if np.issubdtype(total.dtype, np.complexfloating):
            mu = divide(total, n)
            inner_term = np.abs(divide(totals, ns) - mu)
        else:
            mu = divide(total, n, dtype=dtype)
            inner_term = divide(totals, ns, dtype=dtype) - mu

    xs = [
        _moment_helper(Ms, ns, inner_term, o, sum, axis, kwargs)
        for o in range(2, order + 1)
    ]
    M = np.stack(xs, axis=-1)
    return {"total": total, "n": n, "M": M}


def moment_agg(
    pairs,
    order=2,
    ddof=0,
    dtype="f8",
    sum=np.sum,
    axis=None,
    computing_meta=False,
    **kwargs,
):
    if not isinstance(pairs, list):
        pairs = [pairs]

    kwargs["dtype"] = dtype
    # To properly handle ndarrays, the original dimensions need to be kept for
    # part of the calculation.
    keepdim_kw = kwargs.copy()
    keepdim_kw["keepdims"] = True
    keepdim_kw["dtype"] = None

    ns = deepmap(lambda pair: pair["n"], pairs) if not computing_meta else pairs
    ns = _concatenate2(ns, axes=axis)
    n = ns.sum(axis=axis, **keepdim_kw)

    if computing_meta:
        return n

    totals = _concatenate2(deepmap(lambda pair: pair["total"], pairs), axes=axis)
    Ms = _concatenate2(deepmap(lambda pair: pair["M"], pairs), axes=axis)

    mu = divide(totals.sum(axis=axis, **keepdim_kw), n)

    with np.errstate(divide="ignore", invalid="ignore"):
        if np.issubdtype(totals.dtype, np.complexfloating):
            inner_term = np.abs(divide(totals, ns) - mu)
        else:
            inner_term = divide(totals, ns, dtype=dtype) - mu

    M = _moment_helper(Ms, ns, inner_term, order, sum, axis, kwargs)

    denominator = n.sum(axis=axis, **kwargs) - ddof

    # taking care of the edge case with empty or all-nans array with ddof > 0
    if isinstance(denominator, Number):
        if denominator < 0:
            denominator = np.nan
    elif denominator is not np.ma.masked:
        denominator[denominator < 0] = np.nan

    return divide(M, denominator, dtype=dtype)


def moment(
    a, order, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None, out=None
):
    """Calculate the nth centralized moment.

    Parameters
    ----------
    a : Array
        Data over which to compute moment
    order : int
        Order of the moment that is returned, must be >= 2.
    axis : int, optional
        Axis along which the central moment is computed. The default is to
        compute the moment of the flattened array.
    dtype : data-type, optional
        Type to use in computing the moment. For arrays of integer type the
        default is float64; for arrays of float types it is the same as the
        array type.
    keepdims : bool, optional
        If this is set to True, the axes which are reduced are left in the
        result as dimensions with size one. With this option, the result
        will broadcast correctly against the original array.
    ddof : int, optional
        "Delta Degrees of Freedom": the divisor used in the calculation is
        N - ddof, where N represents the number of elements. By default
        ddof is zero.

    Returns
    -------
    moment : Array

    References
    ----------
    .. [1] Pebay, Philippe (2008), "Formulas for Robust, One-Pass Parallel
        Computation of Covariances and Arbitrary-Order Statistical Moments",
        Technical Report SAND2008-6212, Sandia National Laboratories.

    """
    if not isinstance(order, Integral) or order < 0:
        raise ValueError("Order must be an integer >= 0")

    if order < 2:
        # reduced = a.sum(axis=axis)  # get reduced shape and chunks
        if order == 0:
            raise NotImplementedError("need to implement ones")  # TODO
            # When order equals 0, the result is 1, by definition.
            # return ones(
            #     reduced.shape, chunks=reduced.chunks, dtype="f8", meta=reduced._meta
            # )
        # By definition the first order about the mean is 0.
        raise NotImplementedError("need to implement zeros")  # TODO
        # return zeros(
        #     reduced.shape, chunks=reduced.chunks, dtype="f8", meta=reduced._meta
        # )

    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.var(np.ones(shape=(1,), dtype=a.dtype)), "dtype", object)

    implicit_complex_dtype = dtype is None and np.iscomplexobj(a)

    return reduction(
        a,
        partial(
            moment_chunk, order=order, implicit_complex_dtype=implicit_complex_dtype
        ),
        partial(moment_agg, order=order, ddof=ddof),
        axis=axis,
        keepdims=keepdims,
        dtype=dt,
        split_every=split_every,
        out=out,
        concatenate=False,
        combine=partial(moment_combine, order=order),
    )


@derived_from(np)
def var(a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None, out=None):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.var(np.ones(shape=(1,), dtype=a.dtype)), "dtype", object)

    implicit_complex_dtype = dtype is None and np.iscomplexobj(a._meta)

    return reduction(
        a,
        partial(moment_chunk, implicit_complex_dtype=implicit_complex_dtype),
        partial(moment_agg, ddof=ddof),
        axis=axis,
        keepdims=keepdims,
        dtype=dt,
        split_every=split_every,
        combine=moment_combine,
        name="var",
        out=out,
        concatenate=False,
    )


@derived_from(np)
def nanvar(
    a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None, out=None
):
    if dtype is not None:
        dt = dtype
    else:
        dt = getattr(np.var(np.ones(shape=(1,), dtype=a.dtype)), "dtype", object)

    implicit_complex_dtype = dtype is None and np.iscomplexobj(a)

    return reduction(
        a,
        partial(
            moment_chunk,
            sum=chunk.nansum,
            numel=nannumel,
            implicit_complex_dtype=implicit_complex_dtype,
        ),
        partial(moment_agg, sum=np.nansum, ddof=ddof),
        axis=axis,
        keepdims=keepdims,
        dtype=dt,
        split_every=split_every,
        combine=partial(moment_combine, sum=np.nansum),
        out=out,
        concatenate=False,
    )


def _sqrt(a):
    o = np.sqrt(a)
    if isinstance(o, np.ma.masked_array) and not o.shape and o.mask.all():
        return np.ma.masked
    return o


def safe_sqrt(a):
    """A version of sqrt that properly handles scalar masked arrays.

    To mimic ``np.ma`` reductions, we need to convert scalar masked arrays that
    have an active mask to the ``np.ma.masked`` singleton. This is properly
    handled automatically for reduction code, but not for ufuncs. We implement
    a simple version here, since calling `np.ma.sqrt` everywhere is
    significantly more expensive.
    """
    if hasattr(a, "_elemwise"):
        return a._elemwise(_sqrt, a)
    return _sqrt(a)


@derived_from(np)
def std(a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None, out=None):
    result = safe_sqrt(
        var(
            a,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            ddof=ddof,
            split_every=split_every,
            out=out,
        )
    )
    if dtype and dtype != result.dtype:
        result = result.astype(dtype)
    return result


@derived_from(np)
def nanstd(
    a, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None, out=None
):
    result = safe_sqrt(
        nanvar(
            a,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            ddof=ddof,
            split_every=split_every,
            out=out,
        )
    )
    if dtype and dtype != result.dtype:
        result = result.astype(dtype)
    return result


from dask_expr.array.blockwise import blockwise

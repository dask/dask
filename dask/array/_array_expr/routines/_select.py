"""Selection functions for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asarray, elemwise
from dask.utils import derived_from


@derived_from(np)
def digitize(a, bins, right=False):
    """Return the indices of the bins to which each value in input array belongs.

    Parameters
    ----------
    a : dask array
        Input array to be binned.
    bins : array_like
        Array of bins. Must be 1-dimensional and monotonic.
    right : bool, optional
        Indicating whether the intervals include the right or left bin edge.

    Returns
    -------
    indices : dask array of ints
        Output array of indices.
    """
    bins = np.asarray(bins)
    if bins.ndim != 1:
        raise ValueError("bins must be 1-dimensional")

    dtype = np.digitize(np.asarray([0], like=bins), bins, right=right).dtype
    return elemwise(np.digitize, a, dtype=dtype, bins=bins, right=right)


def _variadic_choose(a, *choices):
    return np.choose(a, choices)


@derived_from(np)
def choose(a, choices):
    a = asarray(a)
    choices = [asarray(c) for c in choices]
    return elemwise(_variadic_choose, a, *choices)


@derived_from(np)
def extract(condition, arr):
    from dask.array._array_expr.routines._misc import compress

    condition = asarray(condition).astype(bool)
    arr = asarray(arr)
    return compress(condition.ravel(), arr.ravel())


def _int_piecewise(x, *condlist, funclist=None, func_args=(), func_kw=None):
    return np.piecewise(x, list(condlist), funclist, *func_args, **(func_kw or {}))


@derived_from(np)
def piecewise(x, condlist, funclist, *args, **kw):
    x = asarray(x)
    return elemwise(
        _int_piecewise,
        x,
        *condlist,
        dtype=x.dtype,
        name="piecewise",
        funclist=funclist,
        func_args=args,
        func_kw=kw,
    )


def _select(*args, **kwargs):
    split_at = len(args) // 2
    condlist = args[:split_at]
    choicelist = args[split_at:]
    return np.select(condlist, choicelist, **kwargs)


@derived_from(np)
def select(condlist, choicelist, default=0):
    from dask.array._array_expr._collection import blockwise
    from dask.array._array_expr.routines._misc import result_type

    if len(condlist) != len(choicelist):
        raise ValueError("list of cases must be same length as list of conditions")

    if len(condlist) == 0:
        raise ValueError("select with an empty condition list is not possible")

    choicelist = [asarray(choice) for choice in choicelist]

    try:
        intermediate_dtype = result_type(*choicelist)
    except TypeError as e:
        msg = "Choicelist elements do not have a common dtype."
        raise TypeError(msg) from e

    blockwise_shape = tuple(range(choicelist[0].ndim))
    condargs = [arg for elem in condlist for arg in (elem, blockwise_shape)]
    choiceargs = [arg for elem in choicelist for arg in (elem, blockwise_shape)]

    return blockwise(
        _select,
        blockwise_shape,
        *condargs,
        *choiceargs,
        dtype=intermediate_dtype,
        default=default,
    )

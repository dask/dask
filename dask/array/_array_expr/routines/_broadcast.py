"""Broadcasting utilities for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asanyarray, asarray
from dask.utils import derived_from


def unify_chunks(*args, **kwargs):
    """
    Unify chunks across a sequence of arrays

    This utility function is used within other common operations like
    :func:`dask.array.core.map_blocks` and :func:`dask.array.core.blockwise`.
    It is not commonly used by end-users directly.

    Parameters
    ----------
    *args: sequence of Array, index pairs
        Sequence like (x, 'ij', y, 'jk', z, 'i')

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones(10, chunks=((5, 2, 3),))
    >>> y = da.ones(10, chunks=((2, 3, 5),))
    >>> chunkss, arrays = unify_chunks(x, 'i', y, 'i')
    >>> chunkss
    {'i': (2, 3, 2, 3)}

    Returns
    -------
    chunkss : dict
        Map like {index: chunks}.
    arrays : list
        List of rechunked arrays.
    """
    from toolz import partition

    from dask._collections import new_collection
    from dask.array._array_expr._expr import unify_chunks_expr

    if not args:
        return {}, []

    arginds = [
        (asanyarray(a) if ind is not None else a, ind) for a, ind in partition(2, args)
    ]

    arrays, inds = zip(*arginds)
    if all(ind is None for ind in inds):
        return {}, list(arrays)

    # Convert to expression-level args
    expr_args = []
    for a, ind in arginds:
        if ind is not None:
            expr_args.extend([a.expr, ind])
        else:
            expr_args.extend([a, ind])

    warn = kwargs.pop("warn", True)
    if kwargs:
        raise TypeError(f"Unexpected keyword arguments: {kwargs}")
    chunkss, expr_arrays, _ = unify_chunks_expr(*expr_args, warn=warn)

    # Convert back to collections
    result_arrays = []
    for a, orig_a_ind in zip(expr_arrays, arginds):
        orig_a, ind = orig_a_ind
        if ind is None:
            result_arrays.append(orig_a)
        else:
            result_arrays.append(new_collection(a))

    return chunkss, result_arrays


@derived_from(np)
def broadcast_arrays(*args, subok=False):
    """Broadcast any number of arrays against each other."""
    from toolz import concat

    from dask.array._array_expr._collection import broadcast_to
    from dask.array.core import broadcast_chunks, broadcast_shapes
    from dask.array.numpy_compat import NUMPY_GE_200

    subok = bool(subok)

    to_array = asanyarray if subok else asarray
    args = tuple(to_array(e) for e in args)

    if not args:
        if NUMPY_GE_200:
            return ()
        return []

    # Unify uneven chunking
    inds = [list(reversed(range(x.ndim))) for x in args]
    uc_args = list(concat(zip(args, inds)))
    _, args = unify_chunks(*uc_args, warn=False)

    shape = broadcast_shapes(*(e.shape for e in args))
    chunks = broadcast_chunks(*(e.chunks for e in args))

    if NUMPY_GE_200:
        result = tuple(broadcast_to(e, shape=shape, chunks=chunks) for e in args)
    else:
        result = [broadcast_to(e, shape=shape, chunks=chunks) for e in args]

    return result

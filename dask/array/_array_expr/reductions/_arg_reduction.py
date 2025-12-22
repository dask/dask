from __future__ import annotations

import operator
from itertools import product, repeat
from numbers import Integral

import numpy as np
from tlz import accumulate, pluck

from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import is_arraylike, validate_axis
from dask.tokenize import _tokenize_deterministic
from dask.utils import cached_property


class ArgChunk(ArrayExpr):
    """Expression for the initial chunk step of arg reductions (argmin/argmax).

    Maps the chunk function across all blocks, tracking offsets to compute
    global indices.
    """

    _parameters = ["array", "chunk_func", "axis", "ravel"]

    @cached_property
    def _name(self):
        return "arg-chunk-" + _tokenize_deterministic(
            self.array, self.chunk_func, self.axis, self.ravel
        )

    @cached_property
    def _meta(self):
        # The chunk function returns a structured array or dict with 'vals' and 'arg'
        # fields. The dtype comes from argmin on the meta.
        from dask.array.utils import asarray_safe, meta_from_array

        dtype = np.argmin(asarray_safe([1], like=meta_from_array(self.array)))
        if is_arraylike(dtype):
            return dtype
        # Return a small array with the correct dtype
        return np.array([], dtype=np.intp)

    @cached_property
    def chunks(self):
        # After the chunk step, each block is reduced to size 1 along the axis
        return tuple(
            (1,) * len(c) if i in self.axis else c
            for (i, c) in enumerate(self.array.chunks)
        )

    def _layer(self):
        x = self.array
        axis = self.axis
        ravel = self.ravel

        keys = list(product(*map(range, x.numblocks)))
        offsets = list(
            product(*(accumulate(operator.add, bd[:-1], 0) for bd in x.chunks))
        )
        if ravel:
            offset_info = list(zip(offsets, repeat(x.shape)))
        else:
            offset_info = list(pluck(axis[0], offsets))

        dsk = {}
        for k, off in zip(keys, offset_info):
            dsk[(self._name,) + tuple(k)] = (
                self.chunk_func,
                (x.name,) + tuple(k),
                axis,
                off,
            )
        return dsk


def arg_reduction(
    x, chunk, combine, agg, axis=None, keepdims=False, split_every=None, out=None
):
    """Generic function for arg reductions in array-expr.

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
    from dask.array._array_expr.core._blockwise_funcs import _handle_out
    from dask.array.utils import asarray_safe, meta_from_array

    if axis is None:
        axis = tuple(range(x.ndim))
        ravel = True
    elif isinstance(axis, Integral):
        axis = validate_axis(axis, x.ndim)
        axis = (axis,)
        ravel = x.ndim == 1
    else:
        raise TypeError(f"axis must be either `None` or int, got '{axis}'")

    for ax in axis:
        chunks = x.chunks[ax]
        if len(chunks) > 1 and np.isnan(chunks).any():
            raise ValueError(
                "Arg-reductions do not work with arrays that have "
                "unknown chunksizes. At some point in your computation "
                "this array lost chunking information.\n\n"
                "A possible solution is with \n"
                "  x.compute_chunk_sizes()"
            )

    # Create the ArgChunk expression for the initial chunk step
    tmp = ArgChunk(x.expr, chunk, axis, ravel)

    # Determine dtype
    dtype = np.argmin(asarray_safe([1], like=meta_from_array(x)))
    if hasattr(dtype, "dtype"):
        dtype = dtype.dtype
    else:
        dtype = np.dtype(type(dtype))

    # Import _tree_reduce from the same package
    from dask.array._array_expr.reductions._reduction import _tree_reduce

    result = _tree_reduce(
        tmp,
        agg,
        axis,
        keepdims=keepdims,
        dtype=dtype,
        split_every=split_every,
        combine=combine,
    )
    return _handle_out(out, result)

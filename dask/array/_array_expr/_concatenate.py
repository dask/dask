"""Concatenate operation - expression and collection function."""

from __future__ import annotations

import functools
from bisect import bisect
from itertools import product
from operator import add

import numpy as np
from tlz import accumulate
from toolz import concat

from dask._collections import new_collection
from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr, unify_chunks_expr
from dask.array.core import concatenate3
from dask.array.dispatch import concatenate_lookup
from dask.array.utils import meta_from_array


class Concatenate(ArrayExpr):
    _parameters = ["array", "axis", "meta"]

    @functools.cached_property
    def args(self):
        return [self.array] + self.operands[len(self._parameters) :]

    @functools.cached_property
    def _meta(self):
        return self.operand("meta")

    @functools.cached_property
    def chunks(self):
        bds = [a.chunks for a in self.args]
        chunks = (
            bds[0][: self.axis]
            + (sum((bd[self.axis] for bd in bds), ()),)
            + bds[0][self.axis + 1 :]
        )
        return chunks

    @functools.cached_property
    def _name(self):
        return "stack-" + self.deterministic_token

    @functools.cached_property
    def _layer(self) -> dict:
        axis = self.axis
        cum_dims = [0] + list(accumulate(add, [len(a.chunks[axis]) for a in self.args]))
        keys = list(product([self._name], *[range(len(bd)) for bd in self.chunks]))
        names = [a.name for a in self.args]

        values = [
            (names[bisect(cum_dims, key[axis + 1]) - 1],)
            + key[1 : axis + 1]
            + (key[axis + 1] - cum_dims[bisect(cum_dims, key[axis + 1]) - 1],)
            + key[axis + 2 :]
            for key in keys
        ]

        return dict(zip(keys, values))


class ConcatenateFinalize(ArrayExpr):
    """Finalize array computation by concatenating all blocks.

    This is used for arrays with unknown chunk sizes where rechunking
    is not possible.
    """

    _parameters = ["arr"]

    @functools.cached_property
    def _name(self):
        return f"concatenate-finalize-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return self.arr._meta

    @functools.cached_property
    def chunks(self):
        # Output is a single chunk with unknown size
        return tuple((np.nan,) for _ in range(self.arr.ndim))

    @functools.cached_property
    def numblocks(self):
        return tuple(1 for _ in range(self.arr.ndim))

    @functools.cached_property
    def _cached_keys(self):
        return List(TaskRef((self._name,) + (0,) * self.arr.ndim))

    def _layer(self) -> dict:
        # Get all keys from the input array in nested list structure
        arr_keys = self.arr.__dask_keys__()

        # Convert nested key structure to TaskRefs
        def convert_keys(keys):
            if isinstance(keys, list):
                return List(*[convert_keys(k) for k in keys])
            return TaskRef(keys)

        keys_list = convert_keys(arr_keys)

        out_key = (self._name,) + (0,) * self.arr.ndim
        return {
            out_key: Task(out_key, concatenate3, keys_list)
        }


def concatenate(seq, axis=0, allow_unknown_chunksizes=False):
    """
    Concatenate arrays along an existing axis

    Given a sequence of dask Arrays form a new dask Array by stacking them
    along an existing dimension (axis=0 by default)

    Parameters
    ----------
    seq: list of dask.arrays
    axis: int
        Dimension along which to align all of the arrays. If axis is None,
        arrays are flattened before use.
    allow_unknown_chunksizes: bool
        Allow unknown chunksizes, such as come from converting from dask
        dataframes.  Dask.array is unable to verify that chunks line up.  If
        data comes from differently aligned sources then this can cause
        unexpected results.

    Examples
    --------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [da.from_array(np.ones((4, 4)), chunks=(2, 2))
    ...          for i in range(3)]

    >>> x = da.concatenate(data, axis=0)
    >>> x.shape
    (12, 4)

    >>> da.concatenate(data, axis=1).shape
    (4, 12)

    Result is a new dask Array

    See Also
    --------
    stack
    """
    from dask.array import wrap

    # Lazy import to avoid circular dependency
    from dask.array._array_expr.core import asarray

    seq = [asarray(a, allow_unknown_chunksizes=allow_unknown_chunksizes) for a in seq]

    if not seq:
        raise ValueError("Need array(s) to concatenate")

    if axis is None:
        seq = [a.flatten() for a in seq]
        axis = 0

    seq_metas = [meta_from_array(s) for s in seq]
    _concatenate = concatenate_lookup.dispatch(
        type(max(seq_metas, key=lambda x: getattr(x, "__array_priority__", 0)))
    )
    meta = _concatenate(seq_metas, axis=axis)

    # Promote types to match meta
    seq = [a.astype(meta.dtype) for a in seq]

    # Find output array shape
    ndim = len(seq[0].shape)
    shape = tuple(
        sum(a.shape[i] for a in seq) if i == axis else seq[0].shape[i]
        for i in range(ndim)
    )

    # Drop empty arrays
    seq2 = [a for a in seq if a.size]
    if not seq2:
        seq2 = seq

    if axis < 0:
        axis = ndim + axis
    if axis >= ndim:
        msg = (
            "Axis must be less than than number of dimensions"
            "\nData has %d dimensions, but got axis=%d"
        )
        raise ValueError(msg % (ndim, axis))

    n = len(seq2)
    if n == 0:
        try:
            return wrap.empty_like(meta, shape=shape, chunks=shape, dtype=meta.dtype)
        except TypeError:
            return wrap.empty(shape, chunks=shape, dtype=meta.dtype)
    elif n == 1:
        return seq2[0]

    if not allow_unknown_chunksizes and not all(
        i == axis or all(x.shape[i] == seq2[0].shape[i] for x in seq2)
        for i in range(ndim)
    ):
        if any(map(np.isnan, seq2[0].shape)):
            raise ValueError(
                "Tried to concatenate arrays with unknown"
                f" shape {seq2[0].shape}.\n\nTwo solutions:\n"
                "  1. Force concatenation pass"
                " allow_unknown_chunksizes=True.\n"
                "  2. Compute shapes with "
                "[x.compute_chunk_sizes() for x in seq]"
            )
        raise ValueError("Shapes do not align: %s", [x.shape for x in seq2])

    inds = [list(range(ndim)) for i in range(n)]
    for i, ind in enumerate(inds):
        ind[axis] = -(i + 1)

    seq_tmp = [s.expr for s in seq2]
    uc_args = list(concat((s, i) for s, i in zip(seq_tmp, inds)))
    _, seq2, _ = unify_chunks_expr(*uc_args)
    return new_collection(Concatenate(seq2[0], axis, meta, *seq2[1:]))

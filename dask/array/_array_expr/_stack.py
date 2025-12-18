"""Stack operation - expression and collection function."""

from __future__ import annotations

import functools
from itertools import product

import numpy as np
from toolz import concat, first

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr, unify_chunks_expr
from dask.array.chunk import getitem
from dask.array.utils import meta_from_array


class Stack(ArrayExpr):
    _parameters = ["array", "axis", "meta"]

    @functools.cached_property
    def args(self):
        return [self.array] + self.operands[len(self._parameters) :]

    @functools.cached_property
    def _meta(self):
        return self.operand("meta")

    @functools.cached_property
    def chunks(self):
        n = len(self.args)
        return (
            self.array.chunks[: self.axis]
            + ((1,) * n,)
            + self.array.chunks[self.axis :]
        )

    @functools.cached_property
    def _name(self):
        return "stack-" + self.deterministic_token

    def _layer(self) -> dict:
        keys = list(product([self._name], *[range(len(bd)) for bd in self.chunks]))
        names = [a.name for a in self.args]
        axis = self.axis
        ndim = self._meta.ndim - 1

        inputs = [
            (names[key[axis + 1]],) + key[1 : axis + 1] + key[axis + 2 :]
            for key in keys
        ]
        values = [
            Task(
                key,
                getitem,
                TaskRef(inp),
                (slice(None, None, None),) * axis
                + (None,)
                + (slice(None, None, None),) * (ndim - axis),
            )
            for key, inp in zip(keys, inputs)
        ]
        return dict(zip(keys, values))


def stack(seq, axis=0, allow_unknown_chunksizes=False):
    """
    Stack arrays along a new axis

    Given a sequence of dask arrays, form a new dask array by stacking them
    along a new dimension (axis=0 by default)

    Parameters
    ----------
    seq: list of dask.arrays
    axis: int
        Dimension along which to align all of the arrays
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
    ...         for i in range(3)]

    >>> x = da.stack(data, axis=0)
    >>> x.shape
    (3, 4, 4)

    >>> da.stack(data, axis=1).shape
    (4, 3, 4)

    >>> da.stack(data, axis=-1).shape
    (4, 4, 3)

    Result is a new dask Array

    See Also
    --------
    concatenate
    """
    from dask.array import wrap

    # Lazy import to avoid circular dependency
    from dask.array._array_expr.core import asarray

    seq = [asarray(a, allow_unknown_chunksizes=allow_unknown_chunksizes) for a in seq]

    if not seq:
        raise ValueError("Need array(s) to stack")
    if not allow_unknown_chunksizes and not all(x.shape == seq[0].shape for x in seq):
        idx = first(i for i in enumerate(seq) if i[1].shape != seq[0].shape)
        raise ValueError(
            "Stacked arrays must have the same shape. The first array had shape "
            f"{seq[0].shape}, while array {idx[0] + 1} has shape {idx[1].shape}."
        )

    meta = np.stack([meta_from_array(a) for a in seq], axis=axis)
    seq = [x.astype(meta.dtype) for x in seq]

    ndim = meta.ndim - 1
    if axis < 0:
        axis = ndim + axis + 1
    shape = tuple(
        (
            len(seq)
            if i == axis
            else (seq[0].shape[i] if i < axis else seq[0].shape[i - 1])
        )
        for i in range(meta.ndim)
    )

    seq2 = [a for a in seq if a.size]
    if not seq2:
        seq2 = seq

    n = len(seq2)
    if n == 0:
        try:
            return wrap.empty_like(meta, shape=shape, chunks=shape, dtype=meta.dtype)
        except TypeError:
            return wrap.empty(shape, chunks=shape, dtype=meta.dtype)

    ind = list(range(ndim))
    uc_args = list(concat((x.expr, ind) for x in seq2))
    _, seq2, _ = unify_chunks_expr(*uc_args)

    assert len({a.chunks for a in seq2}) == 1  # same chunks

    return new_collection(Stack(seq2[0], axis, meta, *seq2[1:]))

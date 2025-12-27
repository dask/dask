from __future__ import annotations

import functools
import warnings
from operator import getitem

import numpy as np

from dask._task_spec import Alias
from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import meta_from_array


def getitem_variadic(x, *index):
    """Helper function for boolean indexing."""
    return x[index]


def slice_with_bool_dask_array(x, index):
    """Slice x with one or more dask arrays of bools.

    This is a helper function of :meth:`Array.__getitem__`.

    Parameters
    ----------
    x: Array
    index: tuple with as many elements as x.ndim, among which there are
           one or more Array's with dtype=bool

    Returns
    -------
    tuple of (sliced x, new index)

    where the new index is the same as the input, but with slice(None)
    replaced to the original slicer when a filter has been applied.

    Note: The sliced x will have nan chunks on the sliced axes.
    """
    from dask.array._array_expr._collection import (
        Array,
        blockwise,
        elemwise,
        new_collection,
    )
    from dask.array._array_expr._expr import ChunksOverride

    out_index = [
        slice(None) if isinstance(ind, Array) and ind.dtype == bool else ind
        for ind in index
    ]

    # Case 1: Full-dimensional boolean mask
    if len(index) == 1 and index[0].ndim == x.ndim:
        if not np.isnan(x.shape).any() and not np.isnan(index[0].shape).any():
            x = x.ravel()
            index = tuple(i.ravel() for i in index)
        elif x.ndim > 1:
            warnings.warn(
                "When slicing a Dask array of unknown chunks with a boolean mask "
                "Dask array, the output array may have a different ordering "
                "compared to the equivalent NumPy operation. This will raise an "
                "error in a future release of Dask.",
                stacklevel=3,
            )
        # Use elemwise to apply getitem across blocks
        y = elemwise(getitem, x, index[0], dtype=x.dtype)
        # Trigger eager chunk validation to match legacy behavior
        # This will raise if x and index have incompatible chunks
        _ = y.chunks
        result = BooleanIndexFlattened(y.expr)
        return new_collection(result), out_index

    # Case 2: 1D boolean arrays on specific dimensions
    if any(
        isinstance(ind, Array) and ind.dtype == bool and ind.ndim != 1 for ind in index
    ):
        raise NotImplementedError(
            "Slicing with dask.array of bools only permitted when "
            "the indexer has only one dimension or when "
            "it has the same dimension as the sliced "
            "array"
        )

    indexes = [
        ind if isinstance(ind, Array) and ind.dtype == bool else slice(None)
        for ind in index
    ]

    # Track which dimension indices have boolean arrays
    dsk_ind = []

    from toolz import concat

    arginds = []
    i = 0
    for dim, ind in enumerate(indexes):
        if isinstance(ind, Array) and ind.dtype == bool:
            dsk_ind.append(dim)
            new = (ind, tuple(range(i, i + ind.ndim)))
            i += x.ndim
        else:
            new = (slice(None), None)
            i += 1
        arginds.append(new)

    arginds = list(concat(arginds))

    out = blockwise(
        getitem_variadic,
        tuple(range(x.ndim)),
        x,
        tuple(range(x.ndim)),
        *arginds,
        dtype=x.dtype,
    )

    # For boolean indexing, override chunks on boolean-indexed dimensions
    # with nan values since the output size is unknown
    new_chunks = tuple(
        tuple(np.nan for _ in range(len(c))) if dim in dsk_ind else c
        for dim, c in enumerate(out.chunks)
    )
    result = ChunksOverride(out.expr, new_chunks)
    return new_collection(result), tuple(out_index)


class BooleanIndexFlattened(ArrayExpr):
    """Flattens the output of a full-dimensional boolean index operation."""

    _parameters = ["array"]

    @functools.cached_property
    def _name(self):
        return f"getitem-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array._meta, ndim=1)

    @functools.cached_property
    def chunks(self):
        # Total number of blocks = product of numblocks
        from functools import reduce
        from operator import mul

        nblocks = reduce(mul, self.array.numblocks, 1)
        return ((np.nan,) * nblocks,)

    def _layer(self) -> dict:
        from dask.base import flatten

        # Flatten the keys from the elemwise result
        keys = list(flatten(self.array.__dask_keys__()))
        return {(self._name, i): Alias((self._name, i), k) for i, k in enumerate(keys)}

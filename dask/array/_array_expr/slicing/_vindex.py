from __future__ import annotations

import functools
import math
from collections import defaultdict
from itertools import product
from numbers import Number
from operator import mul

import numpy as np

from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import (
    _get_axis,
    _vindex_merge,
    _vindex_slice_and_transpose,
    interleave_none,
    keyname,
)
from dask.array.slicing import replace_ellipsis
from dask.array.utils import meta_from_array
from dask.utils import cached_cumsum, cached_max


def _numpy_vindex(indexer, arr):
    """Helper for vindex with single-block arrays indexed by dask arrays."""
    return arr[indexer]


def _vindex(x, *indexes):
    """Point wise indexing with broadcasting.

    >>> x = np.arange(56).reshape((7, 8))
    >>> x
    array([[ 0,  1,  2,  3,  4,  5,  6,  7],
           [ 8,  9, 10, 11, 12, 13, 14, 15],
           [16, 17, 18, 19, 20, 21, 22, 23],
           [24, 25, 26, 27, 28, 29, 30, 31],
           [32, 33, 34, 35, 36, 37, 38, 39],
           [40, 41, 42, 43, 44, 45, 46, 47],
           [48, 49, 50, 51, 52, 53, 54, 55]])

    >>> from dask.array._array_expr._collection import from_array
    >>> d = from_array(x, chunks=(3, 4))
    >>> result = _vindex(d, [0, 1, 6, 0], [0, 1, 0, 7])
    >>> result.compute()
    array([ 0,  9, 48,  7])
    """

    indexes = replace_ellipsis(x.ndim, indexes)

    nonfancy_indexes = []
    reduced_indexes = []
    for ind in indexes:
        if isinstance(ind, Number):
            nonfancy_indexes.append(ind)
        elif isinstance(ind, slice):
            nonfancy_indexes.append(ind)
            reduced_indexes.append(slice(None))
        else:
            nonfancy_indexes.append(slice(None))
            reduced_indexes.append(ind)

    nonfancy_indexes = tuple(nonfancy_indexes)
    reduced_indexes = tuple(reduced_indexes)

    x = x[nonfancy_indexes]

    array_indexes = {}
    for i, (ind, size) in enumerate(zip(reduced_indexes, x.shape)):
        if not isinstance(ind, slice):
            ind = np.array(ind, copy=True)
            if ind.dtype.kind == "b":
                raise IndexError("vindex does not support indexing with boolean arrays")
            if ((ind >= size) | (ind < -size)).any():
                raise IndexError(
                    "vindex key has entries out of bounds for "
                    f"indexing along axis {i} of size {size}: {ind!r}"
                )
            ind %= size
            array_indexes[i] = ind

    if array_indexes:
        x = _vindex_array(x, array_indexes)

    return x


def _vindex_array(x, dict_indexes):
    """Point wise indexing with only NumPy Arrays."""
    from dask.array._array_expr._collection import new_collection
    from dask.array.wrap import empty

    try:
        broadcast_shape = np.broadcast_shapes(
            *(arr.shape for arr in dict_indexes.values())
        )
    except ValueError as e:
        shapes_str = " ".join(str(a.shape) for a in dict_indexes.values())
        raise IndexError(
            "shape mismatch: indexing arrays could not be "
            f"broadcast together with shapes {shapes_str}"
        ) from e
    npoints = math.prod(broadcast_shape)

    if npoints > 0:
        result_1d = new_collection(
            VIndexArray(x.expr, dict_indexes, broadcast_shape, npoints)
        )
        return result_1d.reshape(broadcast_shape + result_1d.shape[1:])

    # output has zero dimension - just create a new zero-shape array
    axes = [i for i in range(x.ndim) if i in dict_indexes]
    chunks = [c for i, c in enumerate(x.chunks) if i not in axes]
    chunks.insert(0, (0,))
    chunks = tuple(chunks)

    result_1d = empty(tuple(map(sum, chunks)), chunks=chunks, dtype=x.dtype)
    return result_1d.reshape(broadcast_shape + result_1d.shape[1:])


class VIndexArray(ArrayExpr):
    """Point-wise vectorized indexing with broadcasting."""

    _parameters = ["array", "dict_indexes", "broadcast_shape", "npoints"]

    @functools.cached_property
    def _name(self):
        return f"vindex-merge-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array._meta, ndim=len(self.chunks))

    @functools.cached_property
    def _axes(self):
        """Axes that have array indexing."""
        return [i for i in range(self.array.ndim) if i in self.dict_indexes]

    def _subset_to_indexed_axes(self, iterable):
        for i, elem in enumerate(iterable):
            if i in self._axes:
                yield elem

    @functools.cached_property
    def _max_chunk_point_dimensions(self):
        return functools.reduce(
            mul, map(cached_max, self._subset_to_indexed_axes(self.array.chunks))
        )

    @functools.cached_property
    def chunks(self):
        axes = self._axes
        npoints = self.npoints
        max_chunk_point_dimensions = self._max_chunk_point_dimensions

        chunks = [c for i, c in enumerate(self.array.chunks) if i not in axes]

        n_chunks, remainder = divmod(npoints, max_chunk_point_dimensions)
        chunks.insert(
            0,
            (
                (max_chunk_point_dimensions,) * n_chunks
                + ((remainder,) if remainder > 0 else ())
                if npoints > 0
                else (0,)
            ),
        )
        return tuple(chunks)

    def _layer(self) -> dict:
        dict_indexes = self.dict_indexes
        broadcast_shape = self.broadcast_shape
        npoints = self.npoints
        axes = self._axes
        max_chunk_point_dimensions = self._max_chunk_point_dimensions

        bounds2 = tuple(
            np.array(cached_cumsum(c, initial_zero=True))
            for c in self._subset_to_indexed_axes(self.array.chunks)
        )
        axis = _get_axis(
            tuple(i if i in axes else None for i in range(self.array.ndim))
        )

        # Now compute indices of each output element within each input block
        block_idxs = tuple(
            np.searchsorted(b, ind, side="right") - 1
            for b, ind in zip(bounds2, dict_indexes.values())
        )
        starts = (b[i] for i, b in zip(block_idxs, bounds2))
        inblock_idxs = []
        for idx, start in zip(dict_indexes.values(), starts):
            a = idx - start
            if len(a) > 0:
                dtype = np.min_scalar_type(np.max(a, axis=None))
                inblock_idxs.append(a.astype(dtype, copy=False))
            else:
                inblock_idxs.append(a)

        inblock_idxs = np.broadcast_arrays(*inblock_idxs)  # type: ignore[assignment]

        n_chunks, remainder = divmod(npoints, max_chunk_point_dimensions)

        other_blocks = product(
            *[
                range(len(c)) if i not in axes else [None]
                for i, c in enumerate(self.array.chunks)
            ]
        )

        full_slices = [
            slice(None, None) if i not in axes else None for i in range(self.array.ndim)
        ]

        # The output is constructed as a new dimension and then reshaped
        outinds = np.arange(npoints).reshape(broadcast_shape)
        outblocks, outblock_idx = np.divmod(outinds, max_chunk_point_dimensions)

        ravel_shape = (
            n_chunks + 1,
            *self._subset_to_indexed_axes(self.array.numblocks),
        )
        keys = np.ravel_multi_index([outblocks, *block_idxs], ravel_shape)
        sortidx = np.argsort(keys, axis=None)
        sorted_keys = keys.flat[sortidx]
        sorted_inblock_idxs = [_.flat[sortidx] for _ in inblock_idxs]
        sorted_outblock_idx = outblock_idx.flat[sortidx]
        dtype = np.min_scalar_type(max_chunk_point_dimensions)
        sorted_outblock_idx = sorted_outblock_idx.astype(dtype, copy=False)
        flag = np.concatenate([[True], sorted_keys[1:] != sorted_keys[:-1], [True]])
        (key_bounds,) = flag.nonzero()

        slice_name = f"vindex-slice-{self.deterministic_token}"
        dsk = {}

        for okey in other_blocks:
            merge_inputs = defaultdict(list)
            merge_indexer = defaultdict(list)
            for i, (start, stop) in enumerate(
                zip(key_bounds[:-1], key_bounds[1:], strict=True)
            ):
                slicer = slice(start, stop)
                key = sorted_keys[start]
                outblock, *input_blocks = np.unravel_index(key, ravel_shape)
                inblock = [_[slicer] for _ in sorted_inblock_idxs]
                k = keyname(slice_name, i, okey)
                dsk[k] = Task(
                    k,
                    _vindex_slice_and_transpose,
                    TaskRef((self.array._name,) + interleave_none(okey, input_blocks)),
                    interleave_none(full_slices, inblock),
                    axis,
                )
                merge_inputs[outblock].append(TaskRef(k))
                merge_indexer[outblock].append(sorted_outblock_idx[slicer])

            for i in merge_inputs.keys():
                k = keyname(self._name, i, okey)
                dsk[k] = Task(
                    k,
                    _vindex_merge,
                    merge_indexer[i],
                    List(*merge_inputs[i]),
                )

        return dsk

    def __dask_keys__(self):
        # Override to return 1D keys since we reshape after
        return [
            (self._name,) + idx
            for idx in np.ndindex(tuple(len(c) for c in self.chunks))
        ]

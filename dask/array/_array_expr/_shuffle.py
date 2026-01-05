from __future__ import annotations

import copy
import functools
from itertools import count, product
from typing import Literal

import numpy as np

from dask._task_spec import DataNode, List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.chunk import getitem
from dask.array.dispatch import concatenate_lookup, take_lookup
from dask.base import tokenize


def _validate_indexer(chunks, indexer, axis):
    if not isinstance(indexer, list) or not all(isinstance(i, list) for i in indexer):
        raise ValueError("indexer must be a list of lists of positional indices")

    if not axis <= len(chunks):
        raise ValueError(
            f"Axis {axis} is out of bounds for array with {len(chunks)} axes"
        )

    if max(map(max, indexer)) >= sum(chunks[axis]):
        raise IndexError(
            f"Indexer contains out of bounds index. Dimension only has {sum(chunks[axis])} elements."
        )


def shuffle(x, indexer: list[list[int]], axis: int, chunks: Literal["auto"] = "auto"):
    """
    Reorders one dimensions of a Dask Array based on an indexer.

    The indexer defines a list of positional groups that will end up in the same chunk
    together. A single group is in at most one chunk on this dimension, but a chunk
    might contain multiple groups to avoid fragmentation of the array.

    The algorithm tries to balance the chunksizes as much as possible to ideally keep the
    number of chunks consistent or at least manageable.

    Parameters
    ----------
    x: dask array
        Array to be shuffled.
    indexer:  list[list[int]]
        The indexer that determines which elements along the dimension will end up in the
        same chunk. Multiple groups can be in the same chunk to avoid fragmentation, but
        each group will end up in exactly one chunk.
    axis: int
        The axis to shuffle along.
    chunks: "auto"
        Hint on how to rechunk if single groups are becoming too large. The default is
        to split chunks along the other dimensions evenly to keep the chunksize
        consistent. The rechunking is done in a way that ensures that non all-to-all
        network communication is necessary, chunks are only split and not combined with
        other chunks.

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> arr = np.array([[1, 2, 3, 4, 5, 6, 7, 8], [9, 10, 11, 12, 13, 14, 15, 16]])
    >>> x = da.from_array(arr, chunks=(2, 4))

    Separate the elements in different groups.

    >>> y = x.shuffle([[6, 5, 2], [4, 1], [3, 0, 7]], axis=1)

    The shuffle algorihthm will combine the first 2 groups into a single chunk to keep
    the number of chunks small.

    The tolerance of increasing the chunk size is controlled by the configuration
    "array.chunk-size-tolerance". The default value is 1.25.

    >>> y.chunks
    ((2,), (5, 3))

    The array was reordered along axis 1 according to the positional indexer that was given.

    >>> y.compute()
    array([[ 7,  6,  3,  5,  2,  4,  1,  8],
           [15, 14, 11, 13, 10, 12,  9, 16]])
    """
    from dask._collections import new_collection
    from dask.array._shuffle import _rechunk_other_dimensions

    if np.isnan(x.shape).any():
        from dask.array.core import unknown_chunk_message

        raise ValueError(
            f"Shuffling only allowed with known chunk sizes. {unknown_chunk_message}"
        )
    assert isinstance(axis, int), "axis must be an integer"
    _validate_indexer(x.chunks, indexer, axis)

    x = _rechunk_other_dimensions(x, max(map(len, indexer)), axis, chunks)

    name = "shuffle"

    result = _shuffle(x.expr, indexer, axis, name)
    return new_collection(result)


def _shuffle(x, indexer, axis, name):
    if len(indexer) == len(x.chunks[axis]):
        # check if the array is already shuffled the way we want
        ctr = 0
        for idx, c in zip(indexer, x.chunks[axis]):
            if idx != list(range(ctr, ctr + c)):
                break
            ctr += c
        else:
            return x
    return Shuffle(x, indexer, axis, name)


class Shuffle(ArrayExpr):
    _parameters = ["array", "indexer", "axis", "name"]

    @functools.cached_property
    def _meta(self):
        return self.array._meta

    @functools.cached_property
    def _name(self):
        return f"{self.operand('name')}-{self.deterministic_token}"

    @functools.cached_property
    def chunks(self):
        output_chunks = []
        for i, c in enumerate(self.array.chunks):
            if i == self.axis:
                output_chunks.append(tuple(map(len, self._new_chunks)))
            else:
                output_chunks.append(c)
        return tuple(output_chunks)

    @functools.cached_property
    def _chunk_size_limit(self):
        """Max input chunk size on the shuffle axis."""
        return max(self.array.chunks[self.axis])

    @functools.cached_property
    def _new_chunks(self):
        current_chunk, new_chunks = [], []
        limit = self._chunk_size_limit
        for idx in copy.deepcopy(self.indexer):
            # Split oversized groups into limit-sized pieces
            if len(idx) > limit:
                # Flush current chunk first
                if current_chunk:
                    new_chunks.append(current_chunk)
                    current_chunk = []
                # Split large group into limit-sized pieces
                for i in range(0, len(idx), limit):
                    new_chunks.append(idx[i : i + limit])
            elif len(current_chunk) + len(idx) > limit and len(current_chunk) > 0:
                new_chunks.append(current_chunk)
                current_chunk = idx.copy()
            else:
                current_chunk.extend(idx)
                if len(current_chunk) > limit:
                    new_chunks.append(current_chunk)
                    current_chunk = []
        if len(current_chunk) > 0:
            new_chunks.append(current_chunk)
        return new_chunks

    def _simplify_down(self):
        """Push shuffle through various operations using _accept_shuffle pattern."""
        # Check if child can accept this shuffle
        if hasattr(self.array, "_accept_shuffle"):
            return self.array._accept_shuffle(self)

    def _simplify_up(self, parent, dependents):
        """Allow slice operations to push through Shuffle."""
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        return None

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through Shuffle.

        Shuffle reorganizes data along a single axis. We can push slices through:
        1. Non-shuffle axes: directly push through
        2. Shuffle axis (step=1): if input indices are contiguous, slice input
           and adjust the indexer

        Example (non-shuffle axis):
            Slice(Shuffle(x, axis=0), [:, 10:20])
            -> Shuffle(Slice(x, [:, 10:20]), axis=0)

        Example (shuffle axis, contiguous):
            Slice(Shuffle(x, axis=0), [100:200, :])
            -> Shuffle(Slice(x, [input_start:input_stop, :]), adjusted_indexer, axis=0)
        """
        from dask._collections import new_collection

        axis = self.axis
        index = slice_expr.index
        indexer = self.indexer

        # Pad index to full length
        full_index = list(index) + [slice(None)] * (len(self.shape) - len(index))

        # Check if we're slicing on the shuffle axis
        axis_slice = full_index[axis]

        if axis_slice == slice(None):
            # Not slicing shuffle axis - push through directly
            sliced_input = new_collection(self.array)[tuple(full_index)]
            return Shuffle(
                sliced_input.expr,
                indexer,
                self.axis,
                self.operand("name"),
            )

        # Slicing on shuffle axis - check if we can handle it
        if not isinstance(axis_slice, slice):
            return None  # Integer indexing removes the dimension

        # Only handle step=1 slices
        if axis_slice.step is not None and axis_slice.step != 1:
            return None

        # Normalize slice bounds
        axis_size = self.shape[axis]
        start, stop, _ = axis_slice.indices(axis_size)
        if start >= stop:
            return None  # Empty slice

        # Slice the indexer
        new_indexer = indexer[start:stop]

        # Find all input indices needed
        input_indices = set()
        for chunk in new_indexer:
            input_indices.update(chunk)

        if not input_indices:
            return None  # No indices

        input_min = min(input_indices)
        input_max = max(input_indices)

        # Check if input indices are contiguous
        if len(input_indices) != input_max - input_min + 1:
            return None  # Non-contiguous, can't use simple slice

        # Adjust indexer: subtract input_min from each index
        adjusted_indexer = [[idx - input_min for idx in chunk] for chunk in new_indexer]

        # Build slice for input array
        input_slice = list(full_index)
        input_slice[axis] = slice(input_min, input_max + 1)

        sliced_input = new_collection(self.array)[tuple(input_slice)]

        return Shuffle(
            sliced_input.expr,
            adjusted_indexer,
            self.axis,
            self.operand("name"),
        )

    def _layer(self) -> dict:
        chunks = self.array.chunks
        axis = self.axis

        chunk_boundaries = np.cumsum(chunks[axis])

        # Get existing chunk tuple locations
        chunk_tuples = list(
            product(*(range(len(c)) for i, c in enumerate(chunks) if i != axis))
        )

        intermediates: dict = dict()
        merges: dict = dict()
        dtype = np.min_scalar_type(max(*chunks[axis], self._chunk_size_limit))
        split_name = f"shuffle-split-{self.deterministic_token}"
        slices = [slice(None)] * len(chunks)
        split_name_suffixes = count()
        sorter_name = "shuffle-sorter-"
        taker_name = "shuffle-taker-"

        old_blocks = {
            old_index: (self.array._name,) + old_index
            for old_index in np.ndindex(tuple([len(c) for c in chunks]))
        }

        for new_chunk_idx, new_chunk_taker in enumerate(self._new_chunks):
            new_chunk_taker = np.array(new_chunk_taker)
            sorter = np.argsort(new_chunk_taker).astype(dtype)
            sorter_key = sorter_name + tokenize(sorter)
            # low level fusion can't deal with arrays on first position
            merges[sorter_key] = DataNode(sorter_key, (1, sorter))

            sorted_array = new_chunk_taker[sorter]
            source_chunk_nr, taker_boundary_ = np.unique(
                np.searchsorted(chunk_boundaries, sorted_array, side="right"),
                return_index=True,
            )
            taker_boundary: list[int] = taker_boundary_.tolist()
            taker_boundary.append(len(new_chunk_taker))

            taker_cache: dict = {}
            for chunk_tuple in chunk_tuples:
                merge_keys = []

                for c, b_start, b_end in zip(
                    source_chunk_nr, taker_boundary[:-1], taker_boundary[1:]
                ):
                    # insert our axis chunk id into the chunk_tuple
                    chunk_key = convert_key(chunk_tuple, c, axis)
                    name = (split_name, next(split_name_suffixes))
                    this_slice = slices.copy()

                    # Cache the takers to allow de-duplication when serializing
                    # Ugly!
                    if c in taker_cache:
                        taker_key = taker_cache[c]
                    else:
                        this_slice[axis] = (
                            sorted_array[b_start:b_end]
                            - (chunk_boundaries[c - 1] if c > 0 else 0)
                        ).astype(dtype)
                        if len(source_chunk_nr) == 1:
                            this_slice[axis] = this_slice[axis][np.argsort(sorter)]

                        taker_key = taker_name + tokenize(this_slice)
                        # low level fusion can't deal with arrays on first position
                        intermediates[taker_key] = DataNode(
                            taker_key, (1, tuple(this_slice))
                        )
                        taker_cache[c] = taker_key

                    intermediates[name] = Task(
                        name,
                        _getitem,
                        TaskRef(old_blocks[chunk_key]),
                        TaskRef(taker_key),
                    )
                    merge_keys.append(name)

                merge_suffix = convert_key(chunk_tuple, new_chunk_idx, axis)
                out_name_merge = (self._name,) + merge_suffix
                if len(merge_keys) > 1:
                    merges[out_name_merge] = Task(
                        out_name_merge,
                        concatenate_arrays,
                        List(*(TaskRef(m) for m in merge_keys)),
                        TaskRef(sorter_key),
                        axis,
                    )
                elif len(merge_keys) == 1:
                    t = intermediates.pop(merge_keys[0])
                    t.key = out_name_merge
                    merges[out_name_merge] = t
                else:
                    raise NotImplementedError

        return {**merges, **intermediates}


def _getitem(obj, index):
    return getitem(obj, index[1])


def concatenate_arrays(arrs, sorter, axis):
    return take_lookup(
        concatenate_lookup.dispatch(type(arrs[0]))(arrs, axis=axis),
        np.argsort(sorter[1]),
        axis=axis,
    )


def convert_key(key, chunk, axis):
    key = list(key)
    key.insert(axis, int(chunk))  # Normalize np.int64 to Python int
    return tuple(key)

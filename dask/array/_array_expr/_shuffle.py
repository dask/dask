from __future__ import annotations

import copy
import functools
from itertools import count, product

import numpy as np

from dask import config
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


def _shuffle(x, indexer, axis, name):
    _validate_indexer(x.chunks, indexer, axis)

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
    def _chunksize_tolerance(self):
        return config.get("array.chunk-size-tolerance")

    @functools.cached_property
    def _chunk_size_limit(self):
        return int(
            sum(self.array.chunks[self.axis])
            / len(self.array.chunks[self.axis])
            * self._chunksize_tolerance
        )

    @functools.cached_property
    def _new_chunks(self):
        current_chunk, new_chunks = [], []
        for idx in copy.deepcopy(self.indexer):
            if (
                len(current_chunk) + len(idx) > self._chunk_size_limit
                and len(current_chunk) > 0
            ):
                new_chunks.append(current_chunk)
                current_chunk = idx.copy()
            else:
                current_chunk.extend(idx)
                if (
                    len(current_chunk)
                    > self._chunk_size_limit / self._chunksize_tolerance
                ):
                    new_chunks.append(current_chunk)
                    current_chunk = []
        if len(current_chunk) > 0:
            new_chunks.append(current_chunk)
        return new_chunks

    def _simplify_down(self):
        """Push shuffle through various operations."""
        from dask.array._array_expr._blockwise import Blockwise, Elemwise
        from dask.array._array_expr.manipulation._transpose import Transpose

        # Shuffle(Transpose) -> Transpose(Shuffle) with remapped axis
        if isinstance(self.array, Transpose):
            return self._pushdown_through_transpose()

        # Shuffle(Concatenate) -> Concatenate(Shuffle, Shuffle, ...) on non-concat axis
        from dask.array._array_expr._concatenate import Concatenate
        from dask.array._array_expr._stack import Stack

        if isinstance(self.array, (Concatenate, Stack)):
            return self._pushdown_through_concatenate()

        # Shuffle(Elemwise) -> Elemwise(Shuffle, Shuffle, ...)
        if isinstance(self.array, Elemwise):
            return self._pushdown_through_elemwise()

        # Shuffle(Blockwise) -> Blockwise(Shuffle, ...) when axis not affected
        if isinstance(self.array, Blockwise):
            return self._pushdown_through_blockwise()

    def _pushdown_through_transpose(self):
        """Push shuffle through transpose by remapping axis."""
        from dask.array._array_expr.manipulation._transpose import Transpose

        transpose = self.array
        axes = transpose.axes

        # Map shuffle axis through transpose: axes[i] tells us which input axis
        # becomes output axis i. So to shuffle output axis `self.axis`, we need
        # to shuffle input axis `axes[self.axis]`.
        input_axis = axes[self.axis]

        shuffled_input = Shuffle(
            transpose.array, self.indexer, input_axis, self.operand("name")
        )
        return Transpose(shuffled_input, axes)

    def _pushdown_through_concatenate(self):
        """Push shuffle through concatenate/stack on non-concat axis."""
        from dask._collections import new_collection
        from dask.array._array_expr._stack import Stack

        concat = self.array
        concat_axis = concat.axis
        shuffle_axis = self.axis

        # For Stack, the stacked axis is new - adjust shuffle axis accordingly
        if isinstance(concat, Stack):
            if shuffle_axis == concat_axis:
                # Shuffling on the stacked axis itself - can't push through
                return None
            # Adjust axis for inputs (they have one fewer dimension)
            if shuffle_axis > concat_axis:
                input_shuffle_axis = shuffle_axis - 1
            else:
                input_shuffle_axis = shuffle_axis
        else:
            # For Concatenate, can't shuffle on concat axis (would split indices across arrays)
            if shuffle_axis == concat_axis:
                return None
            input_shuffle_axis = shuffle_axis

        # Shuffle each input
        arrays = concat.args
        shuffled_arrays = [
            new_collection(
                Shuffle(a, self.indexer, input_shuffle_axis, self.operand("name"))
            )
            for a in arrays
        ]

        return type(concat)(
            shuffled_arrays[0].expr,
            concat_axis,
            concat._meta,
            *[a.expr for a in shuffled_arrays[1:]],
        )

    def _pushdown_through_elemwise(self):
        """Push shuffle through elemwise by shuffling each input."""
        from dask.array._array_expr._blockwise import Elemwise
        from dask.array.core import is_scalar_for_elemwise

        elemwise = self.array
        axis = self.axis
        indexer = self.indexer
        name = self.operand("name")

        # Only push through if all array inputs have enough dimensions
        for arg in elemwise.elemwise_args:
            if is_scalar_for_elemwise(arg):
                continue
            if arg.ndim <= axis:
                return None

        # Check where/out as well
        if hasattr(elemwise.where, "ndim") and elemwise.where.ndim <= axis:
            return None
        if hasattr(elemwise.out, "ndim") and elemwise.out.ndim <= axis:
            return None

        # Shuffle each array input
        new_args = [
            arg if is_scalar_for_elemwise(arg) else Shuffle(arg, indexer, axis, name)
            for arg in elemwise.elemwise_args
        ]

        # Shuffle where/out if they are arrays
        new_where = elemwise.where
        if hasattr(new_where, "ndim"):
            new_where = Shuffle(new_where, indexer, axis, name)

        new_out = elemwise.out
        if hasattr(new_out, "ndim"):
            new_out = Shuffle(new_out, indexer, axis, name)

        return Elemwise(
            elemwise.op,
            elemwise.operand("dtype"),
            elemwise.operand("name"),
            new_where,
            new_out,
            elemwise.operand("_user_kwargs"),
            *new_args,
        )

    def _pushdown_through_blockwise(self):
        """Push shuffle through blockwise when shuffle axis is not modified."""
        import toolz

        from dask.array._array_expr._blockwise import Blockwise

        blockwise = self.array
        axis = self.axis
        out_ind = blockwise.out_ind

        # Get the index label for the shuffle axis
        shuffle_ind = out_ind[axis]

        # Can't push through if shuffle axis is a new axis or has adjusted chunks
        if blockwise.new_axes and shuffle_ind in blockwise.new_axes:
            return None
        if blockwise.adjust_chunks and shuffle_ind in blockwise.adjust_chunks:
            return None

        # Shuffle each array input on the corresponding axis
        new_args = []
        for arr, ind in toolz.partition(2, blockwise.args):
            if ind is None:
                # Literal argument
                new_args.extend([arr, ind])
            elif shuffle_ind in ind:
                # Find the axis in this input that corresponds to shuffle_ind
                input_axis = ind.index(shuffle_ind)
                shuffled = Shuffle(arr, self.indexer, input_axis, self.operand("name"))
                new_args.extend([shuffled, ind])
            else:
                # This input doesn't have the shuffle dimension
                new_args.extend([arr, ind])

        return Blockwise(
            blockwise.func,
            blockwise.out_ind,
            blockwise.operand("name"),
            blockwise.token,
            blockwise.operand("dtype"),
            blockwise.adjust_chunks,
            blockwise.new_axes,
            blockwise.align_arrays,
            blockwise.concatenate,
            blockwise._meta_provided,
            blockwise.kwargs,
            *new_args,
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
    key.insert(axis, chunk)
    return tuple(key)

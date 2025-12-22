from __future__ import annotations

import functools
import math
from itertools import product

import numpy as np

from dask._task_spec import Alias, List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import concatenate_shaped
from dask.array.slicing import parse_assignment_indices, setitem
from dask.array.utils import meta_from_array
from dask.base import is_dask_collection
from dask.core import flatten
from dask.utils import cached_cumsum


def setitem_array_expr(out_name, array, indices, value):
    """Array-expr version of setitem_array that generates Task objects directly.

    This function creates a new dask graph that assigns values to each block
    that is touched by the indices, leaving other blocks unchanged.
    """
    array_shape = array.shape
    value_shape = value.shape
    value_ndim = len(value_shape)

    # Reformat input indices
    indices, implied_shape, reverse, implied_shape_positions = parse_assignment_indices(
        indices, array_shape
    )

    # Empty slices can only be assigned size 1 values
    if 0 in implied_shape and value_shape and max(value_shape) > 1:
        raise ValueError(
            f"shape mismatch: value array of shape {value_shape} "
            "could not be broadcast to indexing result "
            f"of shape {tuple(implied_shape)}"
        )

    # Set variables needed when creating the part of the assignment value
    offset = len(implied_shape) - value_ndim
    if offset >= 0:
        array_common_shape = implied_shape[offset:]
        value_common_shape = value_shape
        value_offset = 0
        reverse = [i - offset for i in reverse if i >= offset]
    else:
        value_offset = -offset
        array_common_shape = implied_shape
        value_common_shape = value_shape[value_offset:]
        offset = 0
        if value_shape[:value_offset] != (1,) * value_offset:
            raise ValueError(
                "could not broadcast input array from shape"
                f"{value_shape} into shape {tuple(implied_shape)}"
            )

    base_value_indices = []
    non_broadcast_dimensions = []

    for i, (a, b, j) in enumerate(
        zip(array_common_shape, value_common_shape, implied_shape_positions)
    ):
        index = indices[j]
        if is_dask_collection(index) and index.dtype == bool:
            if math.isnan(b) or b <= index.size:
                base_value_indices.append(None)
                non_broadcast_dimensions.append(i)
            else:
                raise ValueError(
                    f"shape mismatch: value array dimension size of {b} is "
                    "greater then corresponding boolean index size of "
                    f"{index.size}"
                )
            continue

        if b == 1:
            base_value_indices.append(slice(None))
        elif a == b:
            base_value_indices.append(None)
            non_broadcast_dimensions.append(i)
        elif math.isnan(a):
            base_value_indices.append(None)
            non_broadcast_dimensions.append(i)
        else:
            raise ValueError(
                f"shape mismatch: value array of shape {value_shape} "
                "could not be broadcast to indexing result of shape "
                f"{tuple(implied_shape)}"
            )

    # Translate chunks tuple to array locations
    chunks = array.chunks
    cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
    array_locations = [
        [(s, s + dim) for s, dim in zip(starts, shapes)]
        for starts, shapes in zip(cumdims, chunks)
    ]
    array_locations = product(*array_locations)

    in_keys = list(flatten(array.__dask_keys__()))

    # Build graph with Task objects
    dsk = {}
    out_name_tuple = (out_name,)

    # Helper closures for index handling (simplified from legacy)
    def block_index_from_1d_index(index, loc0, loc1, is_bool):
        if is_bool:
            return index[loc0:loc1]
        elif is_dask_collection(index):
            i = np.where((loc0 <= index) & (index < loc1), index, loc1)
            return i - loc0
        else:
            i = np.where((loc0 <= index) & (index < loc1))[0]
            return index[i] - loc0

    def block_index_shape_from_1d_bool_index(index, loc0, loc1):
        return np.sum(index[loc0:loc1])

    def n_preceding_from_1d_bool_index(index, loc0):
        return np.sum(index[:loc0])

    def value_indices_from_1d_int_index(index, vsize, loc0, loc1):
        if is_dask_collection(index):
            if np.isnan(index.size):
                i = np.where((loc0 <= index) & (index < loc1), True, False)
                i = concatenate_array_chunks_expr(i)
                i._chunks = ((vsize,),)
            else:
                i = np.where((loc0 <= index) & (index < loc1))[0]
                i = concatenate_array_chunks_expr(i)
        else:
            i = np.where((loc0 <= index) & (index < loc1))[0]
        return i

    for in_key, locations in zip(in_keys, array_locations):
        block_indices = []
        block_indices_shape = []
        block_preceding_sizes = []
        overlaps = True
        dim_1d_int_index = None

        for dim, (index, (loc0, loc1)) in enumerate(zip(indices, locations)):
            integer_index = isinstance(index, int)
            if isinstance(index, slice):
                stop = loc1 - loc0
                if index.stop < loc1:
                    stop -= loc1 - index.stop
                start = index.start - loc0
                if start < 0:
                    start %= index.step
                if start >= stop:
                    overlaps = False
                    break
                step = index.step
                block_index = slice(start, stop, step)
                block_index_size, rem = divmod(stop - start, step)
                if rem:
                    block_index_size += 1
                pre = index.indices(loc0)
                n_preceding, rem = divmod(pre[1] - pre[0], step)
                if rem:
                    n_preceding += 1
            elif integer_index:
                if not loc0 <= index < loc1:
                    overlaps = False
                    break
                block_index = index - loc0
            else:
                is_bool = index.dtype == bool
                block_index = block_index_from_1d_index(index, loc0, loc1, is_bool)
                if is_bool:
                    block_index_size = block_index_shape_from_1d_bool_index(
                        index, loc0, loc1
                    )
                    n_preceding = n_preceding_from_1d_bool_index(index, loc0)
                else:
                    block_index_size = None
                    n_preceding = None
                    dim_1d_int_index = dim
                    loc0_loc1 = loc0, loc1

                if not is_dask_collection(index) and not block_index.size:
                    overlaps = False
                    break

            block_indices.append(block_index)
            if not integer_index:
                block_indices_shape.append(block_index_size)
                block_preceding_sizes.append(n_preceding)

        out_key = out_name_tuple + in_key[1:]

        if not overlaps:
            dsk[out_key] = Alias(out_key, in_key)
            continue

        # Build value indices for this block
        value_indices = base_value_indices[:]
        for i in non_broadcast_dimensions:
            j = i + offset
            if j == dim_1d_int_index:
                value_indices[i] = value_indices_from_1d_int_index(
                    indices[j], value_shape[i + value_offset], *loc0_loc1
                )
            else:
                start = block_preceding_sizes[j]
                value_indices[i] = slice(start, start + block_indices_shape[j])

        for i in reverse:
            size = value_common_shape[i]
            start, stop, step = value_indices[i].indices(size)
            size -= 1
            start = size - start
            stop = size - stop
            if stop < 0:
                stop = None
            value_indices[i] = slice(start, stop, -1)

        if value_ndim > len(indices):
            value_indices.insert(0, Ellipsis)

        # Get the value slice and concatenate to single chunk
        v = value[tuple(value_indices)]
        v = concatenate_array_chunks_expr(v)
        v_key = next(flatten(v.__dask_keys__()))

        # Merge value's graph into dsk
        dsk.update(dict(v.__dask_graph__()))

        # Convert block_indices to use TaskRef for any dask keys
        task_block_indices = []
        for idx in block_indices:
            if is_dask_collection(idx):
                idx = concatenate_array_chunks_expr(idx)
                idx_key = next(flatten(idx.__dask_keys__()))
                dsk.update(dict(idx.__dask_graph__()))
                task_block_indices.append(TaskRef(idx_key))
            else:
                task_block_indices.append(idx)

        # Create Task with proper TaskRef wrappers
        dsk[out_key] = Task(
            out_key,
            setitem,
            TaskRef(in_key),
            TaskRef(v_key),
            List(*task_block_indices),
        )

    return dsk


class SetItem(ArrayExpr):
    """Expression for array assignment (setitem)."""

    _parameters = ["array", "index", "value"]

    @functools.cached_property
    def _name(self):
        return f"setitem-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        meta = meta_from_array(self.array._meta, ndim=self.array.ndim)
        if np.isscalar(meta):
            meta = np.array(meta)
        return meta

    @property
    def chunks(self):
        return self.array.chunks

    def _layer(self) -> dict:
        from dask.array._array_expr._collection import Array

        # Wrap expressions as Array for setitem_array_expr
        array = Array(self.array)
        value = Array(self.value) if hasattr(self.value, "_meta") else self.value

        return setitem_array_expr(self._name, array, self.index, value)


class ConcatenateArrayChunks(ArrayExpr):
    """Concatenate all chunks of an array into a single chunk.

    This is an array-expr version of dask.array.slicing.concatenate_array_chunks.
    """

    _parameters = ["array"]

    @functools.cached_property
    def _name(self):
        return f"concatenate-shaped-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array._meta, ndim=self.array.ndim)

    @functools.cached_property
    def chunks(self):
        # Single chunk containing all the data
        shape = self.array.shape
        if not shape:
            return ((1,),)
        return tuple((s,) for s in shape)

    def _layer(self) -> dict:
        from dask.base import flatten

        # Get all keys from the input array as TaskRefs
        keys = [TaskRef(k) for k in flatten(self.array.__dask_keys__())]
        # Output key has ndim indices, all 0 since we have a single chunk
        out_key = (self._name,) + (0,) * self.array.ndim

        return {
            out_key: Task(
                out_key,
                concatenate_shaped,
                List(*keys),
                self.array.numblocks,
            )
        }


def concatenate_array_chunks_expr(x):
    """Concatenate all chunks of an array into a single chunk.

    Array-expr version of dask.array.slicing.concatenate_array_chunks.
    """
    from dask.array._array_expr._collection import new_collection

    if x.npartitions == 1:
        return x

    return new_collection(ConcatenateArrayChunks(x.expr))

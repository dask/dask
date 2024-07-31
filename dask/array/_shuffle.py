from __future__ import annotations

from itertools import count, product

import numpy as np
import toolz

from dask.array.chunk import getitem
from dask.array.core import Array, unknown_chunk_message
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph


def shuffle(x, indexer: list[list[int]], axis):
    if not isinstance(indexer, list) or not all(isinstance(i, list) for i in indexer):
        raise ValueError("indexer must be a list of lists of positional indices")

    if np.isnan(x.shape).any():
        raise ValueError(
            f"Shuffling only allowed with known chunk sizes. {unknown_chunk_message}"
        )

    if not axis <= len(x.chunks):
        raise ValueError(
            f"Axis {axis} is out of bounds for array with {len(x.chunks)} axes"
        )

    if max(map(max, indexer)) >= sum(x.chunks[axis]):  # type: ignore[arg-type]
        raise IndexError(
            f"Indexer contains out of bounds index. Dimension only has {sum(x.chunks[axis])} elements."
        )

    average_chunk_size = int(sum(x.chunks[axis]) / len(x.chunks[axis]) * 1.25)

    # Figure out how many groups we can put into one chunk
    current_chunk, new_chunks = [], []  # type: ignore[var-annotated]
    for idx in indexer:
        if (
            len(current_chunk) + len(idx) > average_chunk_size
            and len(current_chunk) > 0
        ):
            new_chunks.append(current_chunk)
            current_chunk = idx.copy()
        else:
            current_chunk.extend(idx)
            if len(current_chunk) > average_chunk_size / 1.25:
                new_chunks.append(current_chunk)
                current_chunk = []
    if len(current_chunk) > 0:
        new_chunks.append(current_chunk)

    chunk_boundaries = np.cumsum(x.chunks[axis])

    # Get existing chunk tuple locations
    chunk_tuples = list(
        product(*(range(len(c)) for i, c in enumerate(x.chunks) if i != axis))
    )

    intermediates = dict()
    merges = dict()
    token = tokenize(x, indexer, axis)
    split_name = f"shuffle-split-{token}"
    merge_name = f"shuffle-merge-{token}"
    slices = [slice(None)] * len(x.chunks)
    split_name_suffixes = count()

    old_blocks = np.empty([len(c) for c in x.chunks], dtype="O")
    for old_index in np.ndindex(old_blocks.shape):
        old_blocks[old_index] = (x.name,) + old_index

    for new_chunk_idx, new_chunk_taker in enumerate(new_chunks):
        new_chunk_taker = np.array(new_chunk_taker)  # type: ignore[assignment]
        sorter = np.argsort(new_chunk_taker)
        sorted_array = new_chunk_taker[sorter]
        source_chunk_nr, taker_boundary = np.unique(
            np.searchsorted(chunk_boundaries, sorted_array, side="right"),
            return_index=True,
        )
        taker_boundary = taker_boundary.tolist()
        taker_boundary.append(len(new_chunk_taker))

        for chunk_tuple in chunk_tuples:
            merge_keys = []

            for c, b_start, b_end in zip(
                source_chunk_nr, taker_boundary[:-1], taker_boundary[1:]
            ):
                # insert our axis chunk id into the chunk_tuple
                chunk_key = convert_key(chunk_tuple, c, axis)
                name = (split_name, next(split_name_suffixes))
                this_slice = slices.copy()
                this_slice[axis] = sorted_array[b_start:b_end] - (
                    chunk_boundaries[c - 1] if c > 0 else 0
                )
                intermediates[name] = getitem, old_blocks[chunk_key], tuple(this_slice)
                merge_keys.append(name)

            merge_suffix = convert_key(chunk_tuple, new_chunk_idx, axis)
            if len(merge_keys) >= 1:
                merges[(merge_name,) + merge_suffix] = (
                    concatenate_arrays,
                    merge_keys,
                    sorter,
                    axis,
                )
            else:
                raise NotImplementedError

    layer = toolz.merge(merges, intermediates)
    graph = HighLevelGraph.from_collections(merge_name, layer, dependencies=[x])

    chunks = []
    for i, c in enumerate(x.chunks):
        if i == axis:
            chunks.append(tuple(map(len, new_chunks)))
        else:
            chunks.append(c)

    return Array(graph, merge_name, chunks, meta=x)


def concatenate_arrays(arrs, sorter, axis):
    return np.take(np.concatenate(arrs, axis=axis), np.argsort(sorter), axis=axis)


def convert_key(key, chunk, axis):
    key = list(key)
    key.insert(axis, chunk)
    return tuple(key)

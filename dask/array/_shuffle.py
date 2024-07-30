from __future__ import annotations

from itertools import count, product

import numpy as np
import toolz

from dask.array.chunk import getitem
from dask.array.core import Array, concatenate3
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph


def shuffle(
    x,
    indexer: list[list[int]],
    axis,
):
    average_chunk_size = int(sum(x.chunks[axis]) / len(x.chunks[axis]) * 1.25)

    # Figure out how many groups we can put into one chunk
    current_bucket, buckets = [], []
    for index in indexer:
        if (
            len(current_bucket) + len(index) > average_chunk_size
            and len(current_bucket) > 0
        ):
            buckets.append(current_bucket)
            current_bucket = index.copy()
        else:
            current_bucket.extend(index)
            if len(current_bucket) > average_chunk_size / 1.25:
                buckets.append(current_bucket)
                current_bucket = []
    if len(current_bucket) > 0:
        buckets.append(current_bucket)

    chunk_borders = np.cumsum(x.chunks[axis])
    new_index = list(
        product(*(range(len(c)) for i, c in enumerate(x.chunks) if i != axis))
    )

    intermediates = dict()
    merges = dict()
    token = tokenize(x, indexer, axis)
    split_name = f"shuffle-split-{token}"
    merge_name = f"shuffle-merge-{token}"
    slices = (slice(None),) * (len(x.chunks) - 1)
    split_name_suffixes = count()

    old_blocks = np.empty([len(c) for c in x.chunks], dtype="O")
    for index in np.ndindex(old_blocks.shape):
        old_blocks[index] = (x.name,) + index

    for final_chunk, bucket in enumerate(buckets):
        arr = np.array(bucket)
        sorter = np.argsort(arr)
        sorted_array = arr[sorter]
        chunk_nrs, borders = np.unique(
            np.searchsorted(chunk_borders, sorted_array, side="right"),
            return_index=True,
        )
        borders = borders.tolist()
        borders.append(len(bucket))

        for nidx in new_index:
            keys = []

            for i, (c, b_start, b_end) in enumerate(
                zip(chunk_nrs, borders[:-1], borders[1:])
            ):
                key = convert_key(nidx, c, axis)
                name = (split_name, next(split_name_suffixes))
                intermediates[name] = (
                    getitem,
                    old_blocks[key],
                    convert_key(
                        slices,
                        sorted_array[b_start:b_end]
                        - (chunk_borders[c - 1] if c > 0 else 0),
                        axis,
                    ),
                )
                keys.append(name)

            final_suffix = convert_key(nidx, final_chunk, axis)
            if len(keys) > 1:
                merges[(merge_name,) + final_suffix] = (
                    concatenate_arrays,
                    keys,
                    sorter,
                    axis,
                )
            elif len(keys) == 1:
                merges[(merge_name,) + final_suffix] = keys[0]
            else:
                raise NotImplementedError

    layer = toolz.merge(merges, intermediates)
    graph = HighLevelGraph.from_collections(merge_name, layer, dependencies=[x])

    chunks = []
    for i, c in enumerate(x.chunks):
        if i == axis:
            chunks.append(tuple(map(len, buckets)))
        else:
            chunks.append(c)

    return Array(graph, merge_name, chunks, meta=x)


def concatenate_arrays(arrs, sorter, axis):
    return np.take(np.concatenate(arrs, axis=axis), np.argsort(sorter), axis=axis)


def convert_key(key, chunk, axis):
    key = list(key)
    key.insert(axis, chunk)
    return tuple(key)

from __future__ import annotations

import itertools
import operator

import numpy as np
import toolz

from dask._task_spec import Alias, List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import concatenate3
from dask.array.rechunk import (
    _balance_chunksizes,
    _choose_rechunk_method,
    _validate_rechunk,
    intersect_chunks,
    normalize_chunks,
    plan_rechunk,
    tokenize,
    validate_axis,
)
from dask.utils import cached_property


class Rechunk(ArrayExpr):
    _parameters = [
        "array",
        "_chunks",
        "threshold",
        "block_size_limit",
        "balance",
        "method",
    ]

    _defaults = {
        "_chunks": "auto",
        "threshold": None,
        "block_size_limit": None,
        "balance": None,
        "method": None,
    }

    @property
    def _meta(self):
        return self.array._meta

    @property
    def _name(self):
        return "rechunk-merge-" + tokenize(*self.operands)

    @cached_property
    def chunks(self):
        x = self.array
        chunks = self.operand("_chunks")

        # don't rechunk if array is empty
        if x.ndim > 0 and all(s == 0 for s in x.shape):
            return x.chunks

        if isinstance(chunks, dict):
            chunks = {validate_axis(c, x.ndim): v for c, v in chunks.items()}
            for i in range(x.ndim):
                if i not in chunks:
                    chunks[i] = x.chunks[i]
                elif chunks[i] is None:
                    chunks[i] = x.chunks[i]
        if isinstance(chunks, (tuple, list)):
            chunks = tuple(
                lc if lc is not None else rc for lc, rc in zip(chunks, x.chunks)
            )
        chunks = normalize_chunks(
            chunks,
            x.shape,
            limit=self.block_size_limit,
            dtype=x.dtype,
            previous_chunks=x.chunks,
        )

        if not len(chunks) == x.ndim:
            raise ValueError("Provided chunks are not consistent with shape")

        if self.balance:
            chunks = tuple(_balance_chunksizes(chunk) for chunk in chunks)

        _validate_rechunk(x.chunks, chunks)

        return chunks

    def _simplify_down(self):
        # Rechunk(Rechunk(x)) -> single Rechunk to final chunks
        # Only match Rechunk, not TasksRechunk (which is already lowered)
        if type(self.array) is Rechunk:
            return Rechunk(
                self.array.array,
                self._chunks,
                self.threshold,
                self.block_size_limit,
                self.balance or self.array.balance,
                self.method,
            )

    def _lower(self):

        if not self.balance and (self.chunks == self.array.chunks):
            return self.array

        method = self.method or _choose_rechunk_method(
            self.array.chunks, self.chunks, threshold=self.threshold
        )
        if method == "p2p":
            raise NotImplementedError
        else:
            return TasksRechunk(
                self.array, self.chunks, self.threshold, self.block_size_limit
            )


class TasksRechunk(Rechunk):
    _parameters = ["array", "_chunks", "threshold", "block_size_limit"]

    @cached_property
    def chunks(self):
        return self.operand("_chunks")

    def _lower(self):
        return

    def _layer(self):
        steps = plan_rechunk(
            self.array.chunks,
            self.chunks,
            self.array.dtype.itemsize,
            self.threshold,
            self.block_size_limit,
        )
        name = self.array.name
        old_chunks = self.array.chunks
        layers = []
        for i, c in enumerate(steps):
            level = len(steps) - i - 1
            name, old_chunks, layer = _compute_rechunk(
                name, old_chunks, c, level, self.name
            )
            layers.append(layer)

        return toolz.merge(*layers)


def _convert_to_task_refs(obj):
    """Recursively convert nested lists of keys to TaskRefs."""
    if isinstance(obj, list):
        return List(*[_convert_to_task_refs(item) for item in obj])
    elif isinstance(obj, tuple):
        # Keys are tuples like (name, i, j, ...)
        return TaskRef(obj)
    else:
        return obj


def _compute_rechunk(old_name, old_chunks, chunks, level, name):
    """Compute the rechunk of *x* to the given *chunks*."""
    ndim = len(old_chunks)
    crossed = intersect_chunks(old_chunks, chunks)
    x2 = {}
    intermediates = {}

    if level != 0:
        merge_name = name.replace("rechunk-merge-", f"rechunk-merge-{level}-")
        split_name = name.replace("rechunk-merge-", f"rechunk-split-{level}-")
    else:
        merge_name = name.replace("rechunk-merge-", "rechunk-merge-")
        split_name = name.replace("rechunk-merge-", "rechunk-split-")
    split_name_suffixes = itertools.count()

    # Pre-allocate old block references
    old_blocks = np.empty([len(c) for c in old_chunks], dtype="O")
    for index in np.ndindex(old_blocks.shape):
        old_blocks[index] = (old_name,) + index

    # Iterate over all new blocks
    new_index = itertools.product(*(range(len(c)) for c in chunks))

    for new_idx, cross1 in zip(new_index, crossed):
        key = (merge_name,) + new_idx
        old_block_indices = [[cr[i][0] for cr in cross1] for i in range(ndim)]
        subdims1 = [len(set(old_block_indices[i])) for i in range(ndim)]

        rec_cat_arg = np.empty(subdims1, dtype="O")
        rec_cat_arg_flat = rec_cat_arg.flat

        # Iterate over the old blocks required to build the new block
        for rec_cat_index, ind_slices in enumerate(cross1):
            old_block_index, slices = zip(*ind_slices)
            intermediate_name = (split_name, next(split_name_suffixes))
            old_index = old_blocks[old_block_index][1:]
            if all(
                slc.start == 0 and slc.stop == old_chunks[i][ind]
                for i, (slc, ind) in enumerate(zip(slices, old_index))
            ):
                # No slicing needed - use old block directly
                rec_cat_arg_flat[rec_cat_index] = old_blocks[old_block_index]
            else:
                # Need to slice the old block
                intermediates[intermediate_name] = Task(
                    intermediate_name,
                    operator.getitem,
                    TaskRef(old_blocks[old_block_index]),
                    slices,
                )
                rec_cat_arg_flat[rec_cat_index] = intermediate_name

        assert rec_cat_index == rec_cat_arg.size - 1

        # New block is formed by concatenation of sliced old blocks
        if all(d == 1 for d in rec_cat_arg.shape):
            # Single source block - alias to it
            source_key = rec_cat_arg.flat[0]
            x2[key] = Alias(key, source_key)
        else:
            # Multiple source blocks - concatenate
            x2[key] = Task(key, concatenate3, _convert_to_task_refs(rec_cat_arg.tolist()))

    del old_blocks, new_index

    return name, chunks, {**x2, **intermediates}


def rechunk(
    x,
    chunks="auto",
    threshold=None,
    block_size_limit=None,
    balance=False,
    method=None,
):
    """
    Convert blocks in dask array x for new chunks.

    Parameters
    ----------
    x: dask array
        Array to be rechunked.
    chunks:  int, tuple, dict or str, optional
        The new block dimensions to create. -1 indicates the full size of the
        corresponding dimension. Default is "auto" which automatically
        determines chunk sizes.
    threshold: int, optional
        The graph growth factor under which we don't bother introducing an
        intermediate step.
    block_size_limit: int, optional
        The maximum block size (in bytes) we want to produce
        Defaults to the configuration value ``array.chunk-size``
    balance : bool, default False
        If True, try to make each chunk to be the same size.

        This means ``balance=True`` will remove any small leftover chunks, so
        using ``x.rechunk(chunks=len(x) // N, balance=True)``
        will almost certainly result in ``N`` chunks.
    method: {'tasks', 'p2p'}, optional.
        Rechunking method to use.


    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones((1000, 1000), chunks=(100, 100))

    Specify uniform chunk sizes with a tuple

    >>> y = x.rechunk((1000, 10))

    Or chunk only specific dimensions with a dictionary

    >>> y = x.rechunk({0: 1000})

    Use the value ``-1`` to specify that you want a single chunk along a
    dimension or the value ``"auto"`` to specify that dask can freely rechunk a
    dimension to attain blocks of a uniform block size

    >>> y = x.rechunk({0: -1, 1: 'auto'}, block_size_limit=1e8)

    If a chunk size does not divide the dimension then rechunk will leave any
    unevenness to the last chunk.

    >>> x.rechunk(chunks=(400, -1)).chunks
    ((400, 400, 200), (1000,))

    However if you want more balanced chunks, and don't mind Dask choosing a
    different chunksize for you then you can use the ``balance=True`` option.

    >>> x.rechunk(chunks=(400, -1), balance=True).chunks
    ((500, 500), (1000,))
    """
    from dask._collections import new_collection

    return new_collection(
        x.expr.rechunk(chunks, threshold, block_size_limit, balance, method)
    )

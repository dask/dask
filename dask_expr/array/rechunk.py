import itertools
import numbers
import operator

import dask
import numpy as np
import toolz
from dask.array.core import concatenate3
from dask.array.rechunk import (
    _balance_chunksizes,
    _validate_rechunk,
    intersect_chunks,
    normalize_chunks,
    plan_rechunk,
    tokenize,
    validate_axis,
)
from dask.utils import cached_property

from dask_expr.array import Array
from dask_expr.array.core import IO


class Rechunk(Array):
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

    def _layer(self):
        method = self.method or dask.config.get("array.rechunk.method")
        if method == "tasks":
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

        if method == "p2p":
            raise NotImplementedError(
                "This shouldn't be hard, but I haven't done it yet, things are in motion over there"
            )

    def _simplify_down(self):
        if isinstance(self.array, Rechunk):
            # TODO: should maybe or the two balance values
            return Rechunk(self.array.array, *self.operands[1:])
        if isinstance(self.array, Elemwise):
            if isinstance(self._chunks, (str, numbers.Number)):
                return self.array.substitute(
                    self.array,
                    self.array.rechunk(self._chunks),
                )
            # TODO: handle subclasses
            # TODO: this probably doesn't support contractions or expansions
            #       We should probably just abort in those cases for now (or do
            #       chunksize math)
            if type(self.array) == Elemwise and isinstance(self._chunks, (dict, tuple)):
                args = []
                for arg, inds in toolz.partition_all(2, self.array.args):
                    if inds is None:
                        args.append(arg)
                    else:
                        assert isinstance(arg, Array)
                        if isinstance(self._chunks, tuple):
                            idx = tuple(self.array.out_ind.index(i) for i in inds)
                            chunks = tuple([self._chunks[i] for i in idx])
                        elif isinstance(self._chunks, dict):
                            chunks = {
                                i: self._chunks[j]
                                for i, j in zip(self.array.out_ind, inds)
                                if j in self._chunks
                            }
                        arg = arg.rechunk(chunks)
                        args.append(arg)

                return Elemwise(*self.array.operands[: -len(args)], *args)

        if isinstance(self.array, Transpose):
            if isinstance(self._chunks, tuple):
                new = tuple(self._chunks[i] for i in self.array.axes)
            elif isinstance(self._chunks, dict):
                new = {self.array.axes.index[k]: v for k, v in self._chunks.items()}
            else:
                return None
            return self.array.substitute(
                self.array.array, self.array.array.rechunk(new)
            )

        if isinstance(self.array, IO) and "chunks" in self.array._parameters:
            chunks = self._chunks
            if isinstance(chunks, tuple):
                chunks = tuple(
                    c if n != 1 else 1 if isinstance(c, numbers.Number) else (1,)
                    for n, c in zip(self.array.shape, self._chunks)
                )
            return self.array.substitute_parameters({"chunks": chunks})


def _compute_rechunk(old_name, old_chunks, chunks, level, name):
    """Compute the rechunk of *x* to the given *chunks*."""
    # TODO: redo this logic
    # if x.size == 0:
    #     # Special case for empty array, as the algorithm below does not behave correctly
    #     return empty(x.shape, chunks=chunks, dtype=x.dtype)

    ndim = len(old_chunks)
    crossed = intersect_chunks(old_chunks, chunks)
    x2 = dict()
    intermediates = dict()
    # token = tokenize(old_name, chunks)
    if level != 0:
        merge_name = name.replace("rechunk-merge-", f"rechunk-merge-{level}-")
        split_name = name.replace("rechunk-merge-", f"rechunk-split-{level}-")
    else:
        merge_name = name.replace("rechunk-merge-", "rechunk-merge-")
        split_name = name.replace("rechunk-merge-", "rechunk-split-")
    split_name_suffixes = itertools.count()

    # Pre-allocate old block references, to allow re-use and reduce the
    # graph's memory footprint a bit.
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
            name = (split_name, next(split_name_suffixes))
            old_index = old_blocks[old_block_index][1:]
            if all(
                slc.start == 0 and slc.stop == old_chunks[i][ind]
                for i, (slc, ind) in enumerate(zip(slices, old_index))
            ):
                rec_cat_arg_flat[rec_cat_index] = old_blocks[old_block_index]
            else:
                intermediates[name] = (
                    operator.getitem,
                    old_blocks[old_block_index],
                    slices,
                )
                rec_cat_arg_flat[rec_cat_index] = name

        assert rec_cat_index == rec_cat_arg.size - 1

        # New block is formed by concatenation of sliced old blocks
        if all(d == 1 for d in rec_cat_arg.shape):
            x2[key] = rec_cat_arg.flat[0]
        else:
            x2[key] = (concatenate3, rec_cat_arg.tolist())

    del old_blocks, new_index

    return name, chunks, {**x2, **intermediates}


from dask_expr.array.blockwise import Elemwise, Transpose

from __future__ import annotations

import functools
import re
from functools import cached_property, reduce
from itertools import product
from operator import mul

import numpy as np
import toolz

from dask._expr import FinalizeCompute, SingletonExpr
from dask._task_spec import List, Task, TaskRef
from dask.array.core import T_IntOrNaN, common_blockdim, unknown_chunk_message
from dask.blockwise import broadcast_dimensions
from dask.layers import ArrayBlockwiseDep
from dask.utils import cached_cumsum, funcname

_OBJECT_AT_PATTERN = re.compile(r"<.+? at 0x[0-9a-fA-F]+>")


def _simplify_repr(op):
    """Simplify operand representation for tree_repr display."""
    if isinstance(op, np.ndarray):
        return "<array>"
    if isinstance(op, np.dtype):
        return str(op)
    if callable(op):
        return funcname(op)
    # Simplify objects that show "object at 0x..." in repr
    r = repr(op)
    if " object at 0x" in r:
        return f"<{type(op).__name__}>"
    return op


def _clean_header(header):
    """Clean up any remaining verbose patterns in the header string."""
    # Replace "<function foo at 0x...>" or "<X object at 0x...>" with "..."
    return _OBJECT_AT_PATTERN.sub("...", header)


class ArrayExpr(SingletonExpr):

    def _operands_for_repr(self):
        return []

    def _tree_repr_lines(self, indent=0, recursive=True):
        header = funcname(type(self)) + ":"
        lines = []
        for i, op in enumerate(self.operands):
            if isinstance(op, ArrayExpr):
                if recursive:
                    lines.extend(op._tree_repr_lines(2))
            else:
                op = _simplify_repr(op)
                header = self._tree_repr_argument_construction(i, op, header)

        header = _clean_header(header)
        lines = [header] + lines
        lines = [" " * indent + line for line in lines]
        return lines

    @cached_property
    def shape(self) -> tuple[T_IntOrNaN, ...]:
        return tuple(cached_cumsum(c, initial_zero=True)[-1] for c in self.chunks)

    @cached_property
    def ndim(self):
        return len(self.shape)

    @cached_property
    def chunksize(self) -> tuple[T_IntOrNaN, ...]:
        return tuple(max(c) for c in self.chunks)

    @cached_property
    def dtype(self):
        if isinstance(self._meta, tuple):
            dtype = self._meta[0].dtype
        else:
            dtype = self._meta.dtype
        return dtype

    @cached_property
    def chunks(self):
        if "chunks" in self._parameters:
            return self.operand("chunks")
        raise NotImplementedError("Subclass must implement 'chunks'")

    @cached_property
    def numblocks(self):
        return tuple(map(len, self.chunks))

    @cached_property
    def size(self) -> T_IntOrNaN:
        """Number of elements in array"""
        return reduce(mul, self.shape, 1)

    @property
    def name(self):
        return self._name

    def __len__(self):
        if not self.chunks:
            raise TypeError("len() of unsized object")
        if np.isnan(self.chunks[0]).any():
            msg = (
                "Cannot call len() on object with unknown chunk size."
                f"{unknown_chunk_message}"
            )
            raise ValueError(msg)
        return int(sum(self.chunks[0]))

    @functools.cached_property
    def _cached_keys(self):
        out = self.lower_completely()

        name, chunks, numblocks = out.name, out.chunks, out.numblocks

        def keys(*args):
            if not chunks:
                return List(TaskRef((name,)))
            ind = len(args)
            if ind + 1 == len(numblocks):
                result = List(
                    *(TaskRef((name,) + args + (i,)) for i in range(numblocks[ind]))
                )
            else:
                result = List(*(keys(*(args + (i,))) for i in range(numblocks[ind])))
            return result

        return keys()

    def __dask_keys__(self):
        key_refs = self._cached_keys

        def unwrap(task):
            if isinstance(task, List):
                return [unwrap(t) for t in task.args]
            return task.key

        return unwrap(key_refs)

    def __hash__(self):
        return hash(self._name)

    def optimize(self):
        return self.simplify().lower_completely()

    def rechunk(
        self,
        chunks="auto",
        threshold=None,
        block_size_limit=None,
        balance=False,
        method=None,
    ):
        if self.ndim > 0 and all(s == 0 for s in self.shape):
            return self

        from dask.array._array_expr._rechunk import Rechunk

        result = Rechunk(self, chunks, threshold, block_size_limit, balance, method)
        # Ensure that chunks are compatible
        result.chunks
        return result

    def finalize_compute(self):
        return FinalizeComputeArray(self)


def unify_chunks_expr(*args):
    # TODO(expr): This should probably be a dedicated expression
    # This is the implementation that expects the inputs to be expressions, the public facing
    # variant needs to sanitize the inputs
    if not args:
        return {}, [], False
    arginds = list(toolz.partition(2, args))
    arrays, inds = zip(*arginds)
    if all(ind is None for ind in inds):
        return {}, list(arrays), False
    if all(ind == inds[0] for ind in inds) and all(
        a.chunks == arrays[0].chunks for a in arrays
    ):
        return dict(zip(inds[0], arrays[0].chunks)), arrays, False

    nameinds = []
    blockdim_dict = dict()
    for a, ind in arginds:
        if ind is not None and not isinstance(a, ArrayBlockwiseDep):
            nameinds.append((a.name, ind))
            blockdim_dict[a.name] = a.chunks
        else:
            nameinds.append((a, ind))

    chunkss = broadcast_dimensions(nameinds, blockdim_dict, consolidate=common_blockdim)

    arrays = []
    changed = False
    for a, i in arginds:
        if i is None or isinstance(a, ArrayBlockwiseDep):
            pass
        else:
            chunks = tuple(
                (
                    chunkss[j]
                    if a.shape[n] > 1
                    else (a.shape[n],) if not np.isnan(sum(chunkss[j])) else None
                )
                for n, j in enumerate(i)
            )
            if chunks != a.chunks and all(a.chunks):
                a = a.rechunk(chunks)
                changed = True
            else:
                pass
        arrays.append(a)
    return chunkss, arrays, changed


# Import Stack, Concatenate, and ConcatenateFinalize from their modules
from dask.array._array_expr._stack import Stack
from dask.array._array_expr._concatenate import Concatenate, ConcatenateFinalize


class FinalizeComputeArray(FinalizeCompute, ArrayExpr):
    _parameters = ["arr"]

    def chunks(self):
        return (self.arr.shape,)

    def _simplify_down(self):
        if self.arr.numblocks in ((), (1,)):
            return self.arr
        else:
            # For arrays with unknown chunk sizes, use ConcatenateFinalize
            # instead of rechunking (which requires known shapes)
            if any(np.isnan(s) for s in self.arr.shape):
                return ConcatenateFinalize(self.arr)
            from dask.array._array_expr._rechunk import Rechunk

            return Rechunk(
                self.arr,
                tuple(-1 for _ in range(self.arr.ndim)),
                method="tasks",
            )


class ChunksOverride(ArrayExpr):
    """Override chunks metadata for an array expression.

    This creates an alias layer while preserving the underlying computation.
    Useful when the actual output chunk sizes differ from what the expression
    system infers (e.g., boolean indexing produces unknown chunk sizes).
    """

    _parameters = ["array", "_chunks"]

    @functools.cached_property
    def _name(self):
        return f"chunks-override-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return self.array._meta

    @functools.cached_property
    def chunks(self):
        return self._chunks

    def _layer(self) -> dict:
        from dask._task_spec import Alias
        from itertools import product

        dsk = {}
        chunk_ranges = [range(len(c)) for c in self._chunks]
        for idx in product(*chunk_ranges):
            out_key = (self._name,) + idx
            in_key = (self.array._name,) + idx
            dsk[out_key] = Alias(out_key, in_key)
        return dsk

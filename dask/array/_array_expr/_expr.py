from __future__ import annotations

import functools
from bisect import bisect
from functools import cached_property, reduce
from itertools import product
from operator import add, mul

import numpy as np
import toolz
from tlz import accumulate

from dask._expr import Expr
from dask.array.chunk import getitem
from dask.array.core import (
    T_IntOrNaN,
    common_blockdim,
    normalize_chunks,
    unknown_chunk_message,
)
from dask.blockwise import broadcast_dimensions
from dask.core import quote
from dask.layers import ArrayBlockwiseDep
from dask.tokenize import _tokenize_deterministic
from dask.utils import cached_cumsum


class ArrayExpr(Expr):
    _cached_keys = None

    def _operands_for_repr(self):
        return []

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

    def __dask_keys__(self):
        out = self.lower_completely()
        if self._cached_keys is not None:
            return self._cached_keys

        name, chunks, numblocks = out.name, out.chunks, out.numblocks

        def keys(*args):
            if not chunks:
                return [(name,)]
            ind = len(args)
            if ind + 1 == len(numblocks):
                result = [(name,) + args + (i,) for i in range(numblocks[ind])]
            else:
                result = [keys(*(args + (i,))) for i in range(numblocks[ind])]
            return result

        self._cached_keys = result = keys()
        return result

    def __hash__(self):
        return hash(self._name)

    def optimize(self):
        return self.simplify().lower_completely()

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError as err:
            if key.startswith("_meta"):
                # Avoid a recursive loop if/when `self._meta*`
                # produces an `AttributeError`
                raise RuntimeError(
                    f"Failed to generate metadata for {self}. "
                    "This operation may not be supported by the current backend."
                )

            # Allow operands to be accessed as attributes
            # as long as the keys are not already reserved
            # by existing methods/properties
            _parameters = type(self)._parameters
            if key in _parameters:
                idx = _parameters.index(key)
                return self.operands[idx]

            raise AttributeError(
                f"{err}\n\n"
                "This often means that you are attempting to use an unsupported "
                f"API function.."
            )

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
        result.chunks
        return result


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


class Stack(ArrayExpr):
    _parameters = ["array", "axis", "meta"]

    @functools.cached_property
    def args(self):
        return [self.array] + self.operands[len(self._parameters) :]

    @functools.cached_property
    def _meta(self):
        return self.operand("meta")

    @functools.cached_property
    def chunks(self):
        n = len(self.args)
        return (
            self.array.chunks[: self.axis]
            + ((1,) * n,)
            + self.array.chunks[self.axis :]
        )

    @functools.cached_property
    def _name(self):
        return "stack-" + _tokenize_deterministic(*self.operands)

    def _layer(self) -> dict:
        keys = list(product([self._name], *[range(len(bd)) for bd in self.chunks]))
        names = [a.name for a in self.args]
        axis = self.axis
        ndim = self._meta.ndim - 1

        inputs = [
            (names[key[axis + 1]],) + key[1 : axis + 1] + key[axis + 2 :]
            for key in keys
        ]
        values = [
            (
                getitem,
                inp,
                (slice(None, None, None),) * axis
                + (None,)
                + (slice(None, None, None),) * (ndim - axis),
            )
            for inp in inputs
        ]
        return dict(zip(keys, values))


class Concatenate(ArrayExpr):
    _parameters = ["array", "axis", "meta"]

    @functools.cached_property
    def args(self):
        return [self.array] + self.operands[len(self._parameters) :]

    @functools.cached_property
    def _meta(self):
        return self.operand("meta")

    @functools.cached_property
    def chunks(self):
        bds = [a.chunks for a in self.args]
        chunks = (
            bds[0][: self.axis]
            + (sum((bd[self.axis] for bd in bds), ()),)
            + bds[0][self.axis + 1 :]
        )
        return chunks

    @functools.cached_property
    def _name(self):
        return "stack-" + _tokenize_deterministic(*self.operands)

    def _layer(self) -> dict:
        axis = self.axis
        cum_dims = [0] + list(accumulate(add, [len(a.chunks[axis]) for a in self.args]))
        keys = list(product([self._name], *[range(len(bd)) for bd in self.chunks]))
        names = [a.name for a in self.args]

        values = [
            (names[bisect(cum_dims, key[axis + 1]) - 1],)
            + key[1 : axis + 1]
            + (key[axis + 1] - cum_dims[bisect(cum_dims, key[axis + 1]) - 1],)
            + key[axis + 2 :]
            for key in keys
        ]

        return dict(zip(keys, values))


class BroadcastTo(ArrayExpr):
    _parameters = ["array", "shape", "chunks"]

    @functools.cached_property
    def _meta(self):
        return self.array._meta

    @functools.cached_property
    def _name(self):
        return "broadcast_to-" + _tokenize_deterministic(*self.operands)

    @functools.cached_property
    def chunks(self):
        chunks = self.operand("chunks")
        shape = self.operand("shape")
        ndim_new = len(shape) - self.array.ndim
        x = self.array
        if chunks is None:
            chunks = tuple((s,) for s in shape[:ndim_new]) + tuple(
                bd if old > 1 else (new,)
                for bd, old, new in zip(x.chunks, x.shape, shape[ndim_new:])
            )
        else:
            chunks = normalize_chunks(
                chunks, shape, dtype=x.dtype, previous_chunks=x.chunks
            )
            for old_bd, new_bd in zip(x.chunks, chunks[ndim_new:]):
                if old_bd != new_bd and old_bd != (1,):
                    raise ValueError(
                        "cannot broadcast chunks %s to chunks %s: "
                        "new chunks must either be along a new "
                        "dimension or a dimension of size 1" % (x.chunks, chunks)
                    )
        return chunks

    def _layer(self) -> dict:
        dsk = {}
        ndim_new = len(self.operand("shape")) - self.array.ndim
        enumerated_chunks = product(*(enumerate(bds) for bds in self.chunks))
        for new_index, chunk_shape in (zip(*ec) for ec in enumerated_chunks):
            old_index = tuple(
                0 if bd == (1,) else i
                for bd, i in zip(self.array.chunks, new_index[ndim_new:])
            )
            old_key = (self.array._name,) + old_index
            new_key = (self._name,) + new_index
            dsk[new_key] = (np.broadcast_to, old_key, quote(chunk_shape))
        return dsk

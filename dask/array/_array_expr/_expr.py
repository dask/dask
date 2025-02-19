from __future__ import annotations

from functools import cached_property, reduce
from operator import mul

import numpy as np
import toolz

from dask._expr import Expr
from dask.array.core import T_IntOrNaN, common_blockdim
from dask.blockwise import broadcast_dimensions
from dask.utils import cached_cumsum


class ArrayExpr(Expr):
    _cached_keys = None

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

        return Rechunk(self, chunks, threshold, block_size_limit, balance, method)


def unify_chunks_expr(*args):
    # TODO(expr): This should probably be a dedicated expression
    # This is the implementation that expects the inputs to be expressions, the public facing
    # variant needs to sanitize the inputs
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
        if ind is not None:
            nameinds.append((a.name, ind))
            blockdim_dict[a.name] = a.chunks
        else:
            nameinds.append((a, ind))

    chunkss = broadcast_dimensions(nameinds, blockdim_dict, consolidate=common_blockdim)

    arrays = []
    changed = False
    for a, i in arginds:
        if i is None:
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
        arrays.extend([a, i])
    return chunkss, arrays, changed

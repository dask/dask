"""Block indexing expression for x.blocks[...] access."""

from __future__ import annotations

import functools
import math
from itertools import product
from numbers import Number

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Alias
from dask.array._array_expr._expr import ArrayExpr
from dask.array.slicing import normalize_index


class BlockView:
    """An array-like interface to the blocks of an array.

    BlockView provides an array-like interface to the blocks of a dask array.
    Numpy-style indexing of a BlockView returns a selection of blocks as a
    new dask array.

    You can index BlockView like a numpy array of shape equal to the number
    of blocks in each dimension (available as array.blocks.size). The
    dimensionality of the output array matches the dimension of this array,
    even if integer indices are passed. Slicing with np.newaxis or multiple
    lists is not supported.
    """

    __slots__ = ("_array",)

    def __init__(self, array):
        self._array = array

    def __getitem__(self, index):
        return new_collection(blocks_getitem(self._array.expr, index))

    def __eq__(self, other):
        # Check if other is any BlockView type (including legacy)
        if hasattr(other, "_array") and type(other).__name__ == "BlockView":
            return self._array is other._array
        return NotImplemented

    @property
    def size(self):
        """The total number of blocks in the array."""
        return math.prod(self.shape)

    @property
    def shape(self):
        """The number of blocks per axis. Alias of dask.array.numblocks."""
        return self._array.numblocks

    def ravel(self):
        """Return a flattened list of all the blocks in the array in C order."""
        return [self[idx] for idx in np.ndindex(self.shape)]


class Blocks(ArrayExpr):
    """Expression for block-based indexing (x.blocks[...]).

    This expression allows accessing array blocks by block index rather than
    element index. The index is normalized to always use slices (never integers)
    to preserve dimensionality.

    Parameters
    ----------
    array : ArrayExpr
        The source array expression
    index : tuple
        Normalized block indices (after converting integers to length-1 slices)
    """

    _parameters = ["array", "index"]

    @functools.cached_property
    def _name(self):
        return f"blocks-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return self.array._meta

    @functools.cached_property
    def chunks(self):
        """Compute chunks by selecting from the source array's chunks."""
        return tuple(
            tuple(np.array(c)[idx].tolist())
            for c, idx in zip(self.array.chunks, self.index)
        )

    def _layer(self) -> dict:
        """Generate the task graph layer.

        Each output block is an alias to the corresponding input block.
        """
        # Pre-compute index mappings for each dimension
        index_maps = [
            np.arange(n)[idx] for n, idx in zip(self.array.numblocks, self.index)
        ]

        dsk = {}
        for out_key in product(*(range(len(c)) for c in self.chunks)):
            in_key = tuple(int(m[i]) for m, i in zip(index_maps, out_key))
            out_name = (self._name,) + out_key
            in_name = (self.array._name,) + in_key
            dsk[out_name] = Alias(out_name, in_name)

        return dsk


def blocks_getitem(array, index):
    """Create a Blocks expression for block indexing.

    Parameters
    ----------
    array : ArrayExpr
        The source array expression
    index : tuple
        The block index (may contain integers, slices, or lists)

    Returns
    -------
    Blocks
        The blocks expression
    """
    if not isinstance(index, tuple):
        index = (index,)

    if sum(isinstance(ind, (np.ndarray, list)) for ind in index) > 1:
        raise ValueError("Can only slice with a single list")
    if any(ind is None for ind in index):
        raise ValueError("Slicing with np.newaxis or None is not supported")

    # Normalize index to array's numblocks
    index = normalize_index(index, array.numblocks)

    # Convert integers to length-1 slices to preserve dimensionality
    index = tuple(slice(k, k + 1) if isinstance(k, Number) else k for k in index)

    return Blocks(array, index)

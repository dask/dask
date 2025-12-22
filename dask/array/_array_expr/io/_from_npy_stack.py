from __future__ import annotations

import functools
import os
import pickle
from itertools import product

import numpy as np

from dask.array._array_expr.io._base import IO


class FromNpyStack(IO):
    """Expression for loading an array from a stack of .npy files."""

    _parameters = ["dirname", "mmap_mode"]
    _defaults = {"mmap_mode": "r"}

    @functools.cached_property
    def _info(self):
        """Load and cache the info file."""
        dirname = self.operand("dirname")
        with open(os.path.join(dirname, "info"), "rb") as f:
            return pickle.load(f)

    @functools.cached_property
    def _meta(self):
        info = self._info
        return np.empty((0,) * len(info["chunks"]), dtype=info["dtype"])

    @functools.cached_property
    def chunks(self):
        return self._info["chunks"]

    @functools.cached_property
    def _name(self):
        return "from-npy-stack-" + self.deterministic_token

    def _layer(self):
        dirname = self.operand("dirname")
        mmap_mode = self.operand("mmap_mode")
        info = self._info
        chunks = info["chunks"]
        axis = info["axis"]

        keys = list(product([self._name], *[range(len(c)) for c in chunks]))
        values = [
            (np.load, os.path.join(dirname, f"{i}.npy"), mmap_mode)
            for i in range(len(chunks[axis]))
        ]
        return dict(zip(keys, values))


def from_npy_stack(dirname, mmap_mode="r"):
    """Load dask array from stack of npy files

    Parameters
    ----------
    dirname: string
        Directory of .npy files
    mmap_mode: (None or 'r')
        Read data in memory map mode

    See Also
    --------
    to_npy_stack
    """
    from dask.array._array_expr._collection import new_collection

    return new_collection(FromNpyStack(dirname=dirname, mmap_mode=mmap_mode))

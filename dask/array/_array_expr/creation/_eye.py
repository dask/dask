from __future__ import annotations

import functools

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import normalize_chunks


class Eye(ArrayExpr):
    _parameters = ["N", "M", "k", "dtype", "chunks"]
    _defaults = {"M": None, "k": 0, "dtype": float, "chunks": "auto"}

    @functools.cached_property
    def _M(self):
        return self.M if self.M is not None else self.N

    @functools.cached_property
    def dtype(self):
        return np.dtype(self.operand("dtype") or float)

    @functools.cached_property
    def _meta(self):
        return np.empty((0, 0), dtype=self.dtype)

    @functools.cached_property
    def chunks(self):
        vchunks, hchunks = normalize_chunks(
            self.operand("chunks"), shape=(self.N, self._M), dtype=self.dtype
        )
        return (vchunks, hchunks)

    @functools.cached_property
    def _chunk_size(self):
        # Use the first vertical chunk size for diagonal positioning logic
        return self.chunks[0][0]

    def _layer(self) -> dict:
        dsk = {}
        vchunks, hchunks = self.chunks
        chunk_size = self._chunk_size
        k = self.k
        dtype = self.dtype

        for i, vchunk in enumerate(vchunks):
            for j, hchunk in enumerate(hchunks):
                key = (self._name, i, j)
                # Check if this block contains part of the k-diagonal
                if (j - i - 1) * chunk_size <= k <= (j - i + 1) * chunk_size:
                    local_k = k - (j - i) * chunk_size
                    task = Task(
                        key,
                        np.eye,
                        vchunk,
                        hchunk,
                        local_k,
                        dtype,
                    )
                else:
                    task = Task(key, np.zeros, (vchunk, hchunk), dtype)
                dsk[key] = task
        return dsk


def eye(N, chunks="auto", M=None, k=0, dtype=float):
    """
    Return a 2-D Array with ones on the diagonal and zeros elsewhere.

    Parameters
    ----------
    N : int
      Number of rows in the output.
    chunks : int, str
        How to chunk the array. Must be one of the following forms:

        -   A blocksize like 1000.
        -   A size in bytes, like "100 MiB" which will choose a uniform
            block-like shape
        -   The word "auto" which acts like the above, but uses a configuration
            value ``array.chunk-size`` for the chunk size
    M : int, optional
      Number of columns in the output. If None, defaults to `N`.
    k : int, optional
      Index of the diagonal: 0 (the default) refers to the main diagonal,
      a positive value refers to an upper diagonal, and a negative value
      to a lower diagonal.
    dtype : data-type, optional
      Data-type of the returned array.

    Returns
    -------
    I : Array of shape (N,M)
      An array where all elements are equal to zero, except for the `k`-th
      diagonal, whose values are equal to one.
    """
    if dtype is None:
        dtype = float

    if not isinstance(chunks, (int, str)):
        raise ValueError("chunks must be an int or string")

    return new_collection(Eye(N, M, k, dtype, chunks))

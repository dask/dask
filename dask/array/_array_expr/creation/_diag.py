from __future__ import annotations

import functools

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._collection import asarray
from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import meta_from_array
from dask.utils import derived_from


class Diag1D(ArrayExpr):
    """Create a diagonal matrix from a 1D array (k=0 case only)."""

    _parameters = ["x"]

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.x, ndim=2)

    @functools.cached_property
    def dtype(self):
        return self.x.dtype

    @functools.cached_property
    def chunks(self):
        chunks_1d = self.x.chunks[0]
        return (chunks_1d, chunks_1d)

    def _layer(self) -> dict:
        dsk = {}
        x = self.x
        chunks_1d = x.chunks[0]
        meta = self._meta

        for i, m in enumerate(chunks_1d):
            for j, n in enumerate(chunks_1d):
                key = (self._name, i, j)
                if i == j:
                    dsk[key] = Task(key, np.diag, TaskRef((x._name, i)))
                else:
                    dsk[key] = Task(key, np.zeros_like, meta, shape=(m, n))
        return dsk


class Diag2DSimple(ArrayExpr):
    """Extract diagonal from a 2D array with square chunks (k=0 case only)."""

    _parameters = ["x"]

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.x, ndim=1)

    @functools.cached_property
    def dtype(self):
        return self.x.dtype

    @functools.cached_property
    def chunks(self):
        return (self.x.chunks[0],)

    def _layer(self) -> dict:
        dsk = {}
        x = self.x
        x_keys = x.__dask_keys__()

        for i, row in enumerate(x_keys):
            key = (self._name, i)
            dsk[key] = Task(key, np.diag, TaskRef(row[i]))
        return dsk


@derived_from(np)
def diag(v, k=0):
    from dask.array._array_expr._collection import Array

    from ._diagonal import diagonal
    from ._pad import pad

    if not isinstance(v, np.ndarray) and not isinstance(v, Array):
        raise TypeError(f"v must be a dask array or numpy array, got {type(v)}")

    # Handle numpy arrays - wrap and return
    if isinstance(v, np.ndarray) or (
        hasattr(v, "__array_function__") and not isinstance(v, Array)
    ):
        if v.ndim == 1:
            abs(k)
            result = np.diag(v, k)
            return asarray(result)
        elif v.ndim == 2:
            result = np.diag(v, k)
            return asarray(result)
        else:
            raise ValueError("Array must be 1d or 2d only")

    v = asarray(v)

    if v.ndim != 1:
        if v.ndim != 2:
            raise ValueError("Array must be 1d or 2d only")
        # 2D case: extract diagonal
        if k == 0 and v.chunks[0] == v.chunks[1]:
            return new_collection(Diag2DSimple(v.expr))
        else:
            return diagonal(v, k)

    # 1D case: create diagonal matrix
    if k == 0:
        return new_collection(Diag1D(v.expr))
    elif k > 0:
        return pad(diag(v), [[0, k], [k, 0]], mode="constant")
    else:  # k < 0
        return pad(diag(v), [[-k, 0], [0, -k]], mode="constant")

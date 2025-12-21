"""Cholesky decomposition for array-expr."""

from __future__ import annotations

import functools
import operator

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Alias, List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr.linalg._utils import _solve_triangular_lower
from dask.array.utils import meta_from_array


def _cholesky_lower(a):
    """Compute Cholesky decomposition (lower triangular)."""
    return np.linalg.cholesky(a)


def _zeros_like_shape(arr_meta, shape):
    """Create zeros with specific shape, dtype from arr_meta."""
    return np.zeros(shape, dtype=arr_meta.dtype)


class Cholesky(ArrayExpr):
    """Block Cholesky decomposition.

    Computes both lower and upper triangular factors.
    """

    _parameters = ["array"]

    @functools.cached_property
    def _meta(self):
        from dask.array.utils import array_safe

        arr_meta = meta_from_array(self.array._meta)
        cho = np.linalg.cholesky(
            array_safe([[1, 2], [2, 5]], dtype=self.array.dtype, like=arr_meta)
        )
        return meta_from_array(self.array._meta, dtype=cho.dtype)

    @functools.cached_property
    def chunks(self):
        return self.array.chunks

    @functools.cached_property
    def _name(self):
        return f"cholesky-{self.deterministic_token}"

    def _layer(self):
        vdim = len(self.array.chunks[0])
        hdim = len(self.array.chunks[1])

        name = self._name
        name_upper = f"cholesky-upper-{self.deterministic_token}"
        name_lt_dot = f"cholesky-lt-dot-{self.deterministic_token}"

        dsk = {}

        for j in range(hdim):
            for i in range(vdim):
                if i < j:
                    chunk_shape = (
                        self.array.chunks[0][i],
                        self.array.chunks[1][j],
                    )
                    dsk[(name, i, j)] = Task(
                        (name, i, j),
                        _zeros_like_shape,
                        self.array._meta,
                        chunk_shape,
                    )
                    dsk[(name_upper, j, i)] = TaskRef((name, i, j))
                elif i == j:
                    target = TaskRef((self.array._name, i, j))
                    if i > 0:
                        prevs = []
                        for p in range(i):
                            prev = (name_lt_dot, i, p, i, p)
                            dsk[prev] = Task(
                                prev,
                                np.dot,
                                TaskRef((name, i, p)),
                                TaskRef((name_upper, p, i)),
                            )
                            prevs.append(TaskRef(prev))
                        target = Task(
                            None, operator.sub, target, Task(None, sum, List(*prevs))
                        )
                    dsk[(name, i, i)] = Task((name, i, i), _cholesky_lower, target)
                    dsk[(name_upper, i, i)] = Task(
                        (name_upper, i, i), np.transpose, TaskRef((name, i, i))
                    )
                else:
                    target = TaskRef((self.array._name, j, i))
                    if j > 0:
                        prevs = []
                        for p in range(j):
                            prev = (name_lt_dot, j, p, i, p)
                            dsk[prev] = Task(
                                prev,
                                np.dot,
                                TaskRef((name, j, p)),
                                TaskRef((name_upper, p, i)),
                            )
                            prevs.append(TaskRef(prev))
                        target = Task(
                            None, operator.sub, target, Task(None, sum, List(*prevs))
                        )
                    dsk[(name_upper, j, i)] = Task(
                        (name_upper, j, i),
                        _solve_triangular_lower,
                        TaskRef((name, j, j)),
                        target,
                    )
                    dsk[(name, i, j)] = Task(
                        (name, i, j), np.transpose, TaskRef((name_upper, j, i))
                    )

        return dsk


class CholeskyLower(ArrayExpr):
    """Extract lower triangular from Cholesky.

    This is a view into the lower triangular portion of the Cholesky
    factorization. It uses Alias tasks to reference the parent Cholesky's
    tasks.
    """

    _parameters = ["chol"]

    @functools.cached_property
    def _meta(self):
        return self.chol._meta

    @functools.cached_property
    def chunks(self):
        return self.chol.chunks

    @functools.cached_property
    def _name(self):
        return f"cholesky-lower-{self.chol.deterministic_token}"

    def _layer(self):
        vdim = len(self.chol.chunks[0])
        hdim = len(self.chol.chunks[1])
        parent_name = self.chol._name

        dsk = {}
        for i in range(vdim):
            for j in range(hdim):
                out_key = (self._name, i, j)
                in_key = (parent_name, i, j)
                dsk[out_key] = Alias(out_key, in_key)
        return dsk


class CholeskyUpper(ArrayExpr):
    """Extract upper triangular from Cholesky.

    This is a view into the upper triangular portion of the Cholesky
    factorization. It uses Alias tasks to reference the parent Cholesky's
    tasks.
    """

    _parameters = ["chol"]

    @functools.cached_property
    def _meta(self):
        return self.chol._meta

    @functools.cached_property
    def chunks(self):
        return self.chol.chunks

    @functools.cached_property
    def _name(self):
        return f"cholesky-upper-view-{self.chol.deterministic_token}"

    def _layer(self):
        vdim = len(self.chol.chunks[0])
        hdim = len(self.chol.chunks[1])
        parent_name = f"cholesky-upper-{self.chol.deterministic_token}"

        dsk = {}
        for i in range(vdim):
            for j in range(hdim):
                out_key = (self._name, i, j)
                in_key = (parent_name, i, j)
                dsk[out_key] = Alias(out_key, in_key)
        return dsk


def _cholesky(a):
    """Private function to compute both L and U Cholesky factors."""
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)

    if a.ndim != 2:
        raise ValueError("Dimension must be 2 to perform cholesky decomposition")

    xdim, ydim = a.shape
    if xdim != ydim:
        raise ValueError(
            "Input must be a square matrix to perform cholesky decomposition"
        )
    if len(set(a.chunks[0] + a.chunks[1])) != 1:
        msg = (
            "All chunks must be a square matrix to perform cholesky decomposition. "
            "Use .rechunk method to change the size of chunks."
        )
        raise ValueError(msg)

    chol_expr = Cholesky(a.expr)
    lower_expr = CholeskyLower(chol_expr)
    upper_expr = CholeskyUpper(chol_expr)

    return new_collection(lower_expr), new_collection(upper_expr)


def cholesky(a, lower=False):
    """Returns the Cholesky decomposition of a Hermitian positive-definite matrix.

    Parameters
    ----------
    a : (M, M) array_like
        Matrix to be decomposed
    lower : bool, optional
        Whether to compute the upper or lower triangular Cholesky
        factorization. Default is upper-triangular.

    Returns
    -------
    c : (M, M) Array
        Upper- or lower-triangular Cholesky factor of `a`.
    """
    l, u = _cholesky(a)
    if lower:
        return l
    else:
        return u

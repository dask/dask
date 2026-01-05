"""LU decomposition for array-expr."""

from __future__ import annotations

import functools
import operator

import numpy as np

from dask._collections import new_collection
from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr.linalg._utils import (
    _solve_triangular_lower,
    _transpose,
)
from dask.array.utils import meta_from_array


def _scipy_lu(a):
    """Compute LU decomposition using scipy."""
    import scipy.linalg

    return scipy.linalg.lu(a)


class LU(ArrayExpr):
    """Block LU decomposition returning (P, L, U) tuple.

    Uses scipy.linalg.lu for diagonal blocks and propagates through
    off-diagonal blocks with forward/backward substitution.
    """

    _parameters = ["array"]

    @functools.cached_property
    def _meta(self):
        import scipy.linalg

        pp, ll, uu = scipy.linalg.lu(np.ones(shape=(1, 1), dtype=self.array.dtype))
        arr_meta = self.array._meta
        return (
            meta_from_array(arr_meta, ndim=2, dtype=pp.dtype),
            meta_from_array(arr_meta, ndim=2, dtype=ll.dtype),
            meta_from_array(arr_meta, ndim=2, dtype=uu.dtype),
        )

    @functools.cached_property
    def chunks(self):
        return self.array.chunks

    @functools.cached_property
    def _name(self):
        return f"lu-{self.deterministic_token}"

    def _layer(self):
        vdim = len(self.array.chunks[0])
        hdim = len(self.array.chunks[1])

        name_lu = f"lu-lu-{self.deterministic_token}"
        name_p = f"lu-p-{self.deterministic_token}"
        name_l = f"lu-l-{self.deterministic_token}"
        name_u = f"lu-u-{self.deterministic_token}"
        name_transpose = f"lu-p-inv-{self.deterministic_token}"
        name_l_permuted = f"lu-l-permute-{self.deterministic_token}"
        name_transposed = f"lu-u-transpose-{self.deterministic_token}"
        name_plu_dot = f"lu-plu-dot-{self.deterministic_token}"
        name_lu_dot = f"lu-lu-dot-{self.deterministic_token}"

        dsk = {}

        for i in range(min(vdim, hdim)):
            target = TaskRef((self.array._name, i, i))
            if i > 0:
                prevs = []
                for p in range(i):
                    prev = (name_plu_dot, i, p, p, i)
                    dsk[prev] = Task(
                        prev,
                        np.dot,
                        TaskRef((name_l_permuted, i, p)),
                        TaskRef((name_u, p, i)),
                    )
                    prevs.append(TaskRef(prev))
                target = Task(None, operator.sub, target, Task(None, sum, List(*prevs)))
            dsk[(name_lu, i, i)] = Task((name_lu, i, i), _scipy_lu, target)

            for j in range(i + 1, hdim):
                target_h = Task(
                    None,
                    np.dot,
                    TaskRef((name_transpose, i, i)),
                    TaskRef((self.array._name, i, j)),
                )
                if i > 0:
                    prevs = []
                    for p in range(i):
                        prev = (name_lu_dot, i, p, p, j)
                        dsk[prev] = Task(
                            prev,
                            np.dot,
                            TaskRef((name_l, i, p)),
                            TaskRef((name_u, p, j)),
                        )
                        prevs.append(TaskRef(prev))
                    target_h = Task(
                        None, operator.sub, target_h, Task(None, sum, List(*prevs))
                    )
                dsk[(name_lu, i, j)] = Task(
                    (name_lu, i, j),
                    _solve_triangular_lower,
                    TaskRef((name_l, i, i)),
                    target_h,
                )

            for k in range(i + 1, vdim):
                target_v = TaskRef((self.array._name, k, i))
                if i > 0:
                    prevs = []
                    for p in range(i):
                        prev = (name_plu_dot, k, p, p, i)
                        dsk[prev] = Task(
                            prev,
                            np.dot,
                            TaskRef((name_l_permuted, k, p)),
                            TaskRef((name_u, p, i)),
                        )
                        prevs.append(TaskRef(prev))
                    target_v = Task(
                        None, operator.sub, target_v, Task(None, sum, List(*prevs))
                    )
                dsk[(name_lu, k, i)] = Task(
                    (name_lu, k, i),
                    np.transpose,
                    Task(
                        None,
                        _solve_triangular_lower,
                        TaskRef((name_transposed, i, i)),
                        Task(None, np.transpose, target_v),
                    ),
                )

        for i in range(min(vdim, hdim)):
            for j in range(min(vdim, hdim)):
                if i == j:
                    dsk[(name_p, i, j)] = Task(
                        (name_p, i, j),
                        operator.getitem,
                        TaskRef((name_lu, i, j)),
                        0,
                    )
                    dsk[(name_l, i, j)] = Task(
                        (name_l, i, j),
                        operator.getitem,
                        TaskRef((name_lu, i, j)),
                        1,
                    )
                    dsk[(name_u, i, j)] = Task(
                        (name_u, i, j),
                        operator.getitem,
                        TaskRef((name_lu, i, j)),
                        2,
                    )
                    dsk[(name_l_permuted, i, j)] = Task(
                        (name_l_permuted, i, j),
                        np.dot,
                        TaskRef((name_p, i, j)),
                        TaskRef((name_l, i, j)),
                    )
                    dsk[(name_transposed, i, j)] = Task(
                        (name_transposed, i, j),
                        _transpose,
                        TaskRef((name_u, i, j)),
                    )
                    dsk[(name_transpose, i, j)] = Task(
                        (name_transpose, i, j),
                        _transpose,
                        TaskRef((name_p, i, j)),
                    )
                elif i > j:
                    chunk_shape = (
                        self.array.chunks[0][i],
                        self.array.chunks[1][j],
                    )
                    dsk[(name_p, i, j)] = Task((name_p, i, j), np.zeros, chunk_shape)
                    dsk[(name_l, i, j)] = Task(
                        (name_l, i, j),
                        np.dot,
                        TaskRef((name_transpose, i, i)),
                        TaskRef((name_lu, i, j)),
                    )
                    dsk[(name_u, i, j)] = Task((name_u, i, j), np.zeros, chunk_shape)
                    dsk[(name_l_permuted, i, j)] = TaskRef((name_lu, i, j))
                else:
                    chunk_shape = (
                        self.array.chunks[0][i],
                        self.array.chunks[1][j],
                    )
                    dsk[(name_p, i, j)] = Task((name_p, i, j), np.zeros, chunk_shape)
                    dsk[(name_l, i, j)] = Task((name_l, i, j), np.zeros, chunk_shape)
                    dsk[(name_u, i, j)] = TaskRef((name_lu, i, j))

        return dsk


class LUGetP(ArrayExpr):
    """Extract P from LU decomposition."""

    _parameters = ["lu"]

    @functools.cached_property
    def _meta(self):
        return self.lu._meta[0]

    @functools.cached_property
    def chunks(self):
        return self.lu.chunks

    @functools.cached_property
    def _name(self):
        return f"lu-p-{self.lu.deterministic_token}"

    def _layer(self):
        return {}


class LUGetL(ArrayExpr):
    """Extract L from LU decomposition."""

    _parameters = ["lu"]

    @functools.cached_property
    def _meta(self):
        return self.lu._meta[1]

    @functools.cached_property
    def chunks(self):
        return self.lu.chunks

    @functools.cached_property
    def _name(self):
        return f"lu-l-{self.lu.deterministic_token}"

    def _layer(self):
        return {}


class LUGetU(ArrayExpr):
    """Extract U from LU decomposition."""

    _parameters = ["lu"]

    @functools.cached_property
    def _meta(self):
        return self.lu._meta[2]

    @functools.cached_property
    def chunks(self):
        return self.lu.chunks

    @functools.cached_property
    def _name(self):
        return f"lu-u-{self.lu.deterministic_token}"

    def _layer(self):
        return {}


def lu(a):
    """Compute the LU decomposition of a matrix.

    Examples
    --------
    >>> p, l, u = da.linalg.lu(x)  # doctest: +SKIP

    Returns
    -------
    p : Array, permutation matrix
    l : Array, lower triangular matrix with unit diagonal.
    u : Array, upper triangular matrix
    """
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)

    if a.ndim != 2:
        raise ValueError("Dimension must be 2 to perform lu decomposition")

    xdim, ydim = a.shape
    if xdim != ydim:
        raise ValueError("Input must be a square matrix to perform lu decomposition")
    if len(set(a.chunks[0] + a.chunks[1])) != 1:
        msg = (
            "All chunks must be a square matrix to perform lu decomposition. "
            "Use .rechunk method to change the size of chunks."
        )
        raise ValueError(msg)

    lu_expr = LU(a.expr)
    p_expr = LUGetP(lu_expr)
    l_expr = LUGetL(lu_expr)
    u_expr = LUGetU(lu_expr)

    return new_collection(p_expr), new_collection(l_expr), new_collection(u_expr)

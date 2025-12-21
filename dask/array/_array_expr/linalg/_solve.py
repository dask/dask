"""Solve linear systems for array-expr."""

from __future__ import annotations

import functools
import operator
import warnings

import numpy as np

from dask._collections import new_collection
from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr.linalg._utils import (
    _solve_triangular_lower,
    _solve_triangular_upper,
)


class SolveTriangular(ArrayExpr):
    """Blocked triangular solve: solve a @ x = b for x.

    Uses forward or backward substitution depending on `lower`.
    """

    _parameters = ["a", "b", "lower"]
    _defaults = {"lower": False}

    @functools.cached_property
    def _meta(self):
        from dask.array.utils import array_safe, meta_from_array

        a_meta = meta_from_array(self.a._meta)
        b_meta = meta_from_array(self.b._meta)
        res = _solve_triangular_lower(
            array_safe([[1, 0], [1, 2]], dtype=self.a.dtype, like=a_meta),
            array_safe([0, 1], dtype=self.b.dtype, like=b_meta),
        )
        return meta_from_array(self.a._meta, self.b.ndim, dtype=res.dtype)

    @functools.cached_property
    def chunks(self):
        return self.b.chunks

    @functools.cached_property
    def _name(self):
        return f"solve-triangular-{self.deterministic_token}"

    def _layer(self):
        vchunks = len(self.a.chunks[1])
        hchunks = 1 if self.b.ndim == 1 else len(self.b.chunks[1])
        name_mdot = f"solve-tri-dot-{self.deterministic_token}"

        def _b_init(i, j):
            if self.b.ndim == 1:
                return (self.b._name, i)
            else:
                return (self.b._name, i, j)

        def _key(i, j):
            if self.b.ndim == 1:
                return (self._name, i)
            else:
                return (self._name, i, j)

        dsk = {}
        if self.lower:
            for i in range(vchunks):
                for j in range(hchunks):
                    target = TaskRef(_b_init(i, j))
                    if i > 0:
                        prevs = []
                        for k in range(i):
                            prev_key = (name_mdot, i, k, k, j)
                            dsk[prev_key] = Task(
                                prev_key,
                                np.dot,
                                TaskRef((self.a._name, i, k)),
                                TaskRef(_key(k, j)),
                            )
                            prevs.append(TaskRef(prev_key))
                        target = Task(
                            None,
                            operator.sub,
                            target,
                            Task(None, sum, List(*prevs)),
                        )
                    dsk[_key(i, j)] = Task(
                        _key(i, j),
                        _solve_triangular_lower,
                        TaskRef((self.a._name, i, i)),
                        target,
                    )
        else:
            for i in range(vchunks - 1, -1, -1):
                for j in range(hchunks):
                    target = TaskRef(_b_init(i, j))
                    if i < vchunks - 1:
                        prevs = []
                        for k in range(i + 1, vchunks):
                            prev_key = (name_mdot, i, k, k, j)
                            dsk[prev_key] = Task(
                                prev_key,
                                np.dot,
                                TaskRef((self.a._name, i, k)),
                                TaskRef(_key(k, j)),
                            )
                            prevs.append(TaskRef(prev_key))
                        target = Task(
                            None,
                            operator.sub,
                            target,
                            Task(None, sum, List(*prevs)),
                        )
                    dsk[_key(i, j)] = Task(
                        _key(i, j),
                        _solve_triangular_upper,
                        TaskRef((self.a._name, i, i)),
                        target,
                    )

        return dsk


def solve_triangular(a, b, lower=False):
    """Solve the equation `a x = b` for `x`, assuming a is a triangular matrix.

    Parameters
    ----------
    a : (M, M) array_like
        A triangular matrix
    b : (M,) or (M, N) array_like
        Right-hand side matrix in `a x = b`
    lower : bool, optional
        Use only data contained in the lower triangle of `a`.
        Default is to use upper triangle.

    Returns
    -------
    x : (M,) or (M, N) array
        Solution to the system `a x = b`. Shape of return matches `b`.
    """
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)
    b = asanyarray(b)

    if a.ndim != 2:
        raise ValueError("a must be 2 dimensional")
    if b.ndim <= 2:
        if a.shape[1] != b.shape[0]:
            raise ValueError("a.shape[1] and b.shape[0] must be equal")
        if a.chunks[1] != b.chunks[0]:
            msg = (
                "a.chunks[1] and b.chunks[0] must be equal. "
                "Use .rechunk method to change the size of chunks."
            )
            raise ValueError(msg)
    else:
        raise ValueError("b must be 1 or 2 dimensional")

    expr = SolveTriangular(a.expr, b.expr, lower)
    return new_collection(expr)


def solve(a, b, sym_pos=None, assume_a="gen"):
    """Solve the equation ``a x = b`` for ``x``.

    By default, use LU decomposition and forward / backward substitutions.
    When ``assume_a = "pos"`` use Cholesky decomposition.

    Parameters
    ----------
    a : (M, M) array_like
        A square matrix.
    b : (M,) or (M, N) array_like
        Right-hand side matrix in ``a x = b``.
    sym_pos : bool, optional
        Assume a is symmetric and positive definite. If ``True``, use Cholesky
        decomposition.

        .. note::
            ``sym_pos`` is deprecated and will be removed in a future version.
            Use ``assume_a = 'pos'`` instead.

    assume_a : {'gen', 'pos'}, optional
        Type of data matrix. It is used to choose the dedicated solver.
        Note that Dask does not support 'her' and 'sym' types.

    Returns
    -------
    x : (M,) or (M, N) Array
        Solution to the system ``a x = b``. Shape of the return matches the
        shape of `b`.

    See Also
    --------
    scipy.linalg.solve
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array._array_expr.linalg._cholesky import _cholesky
    from dask.array._array_expr.linalg._lu import lu

    if sym_pos is not None:
        warnings.warn(
            "The sym_pos keyword is deprecated and should be replaced by using "
            "``assume_a = 'pos'``. ``sym_pos`` will be removed in a future version.",
            category=FutureWarning,
        )
        if sym_pos:
            assume_a = "pos"

    if assume_a == "pos":
        l, u = _cholesky(a)
    elif assume_a == "gen":
        p, l, u = lu(a)
        b = asanyarray(b)
        b = p.T.dot(b)
    else:
        raise ValueError(
            f"{assume_a = } is not a recognized matrix structure, "
            "valid structures in Dask are 'pos' and 'gen'."
        )

    uy = solve_triangular(l, b, lower=True)
    return solve_triangular(u, uy)


def inv(a):
    """Compute the inverse of a matrix with LU decomposition.

    Parameters
    ----------
    a : array_like
        Square matrix to be inverted.

    Returns
    -------
    ainv : Array
        Inverse of the matrix `a`.
    """
    from dask.array._array_expr._creation import eye
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)
    return solve(a, eye(a.shape[0], chunks=a.chunks[0][0]))


def _lstsq_singular(rt, r):
    """Compute singular values from R'R eigenvalues."""
    return np.sqrt(np.linalg.eigvalsh(np.dot(rt, r)))[::-1]


class LstsqRank(ArrayExpr):
    """Compute matrix rank from R factor."""

    _parameters = ["r"]

    @functools.cached_property
    def _meta(self):
        return np.array(0, dtype=int)

    @functools.cached_property
    def chunks(self):
        return ()

    @functools.cached_property
    def _name(self):
        return f"lstsq-rank-{self.deterministic_token}"

    def _layer(self):
        r_key = (self.r._name, 0, 0)
        out_key = (self._name,)
        return {out_key: Task(out_key, np.linalg.matrix_rank, TaskRef(r_key))}


class LstsqSingular(ArrayExpr):
    """Compute singular values from R factor."""

    _parameters = ["r"]

    @functools.cached_property
    def _meta(self):
        input_dtype = self.r.dtype
        if np.issubdtype(input_dtype, np.complexfloating):
            dtype = np.finfo(input_dtype).dtype
        else:
            dtype = input_dtype
        return np.empty((0,), dtype=dtype)

    @functools.cached_property
    def chunks(self):
        return ((self.r.shape[0],),)

    @functools.cached_property
    def _name(self):
        return f"lstsq-singular-{self.deterministic_token}"

    def _layer(self):
        r_key = (self.r._name, 0, 0)
        out_key = (self._name, 0)
        rt_key = (f"lstsq-rt-{self.deterministic_token}", 0, 0)
        return {
            rt_key: Task(rt_key, lambda x: x.T.conj(), TaskRef(r_key)),
            out_key: Task(out_key, _lstsq_singular, TaskRef(rt_key), TaskRef(r_key)),
        }


def lstsq(a, b):
    """Return the least-squares solution to a linear matrix equation using QR.

    Solves the equation `a x = b` by computing a vector `x` that
    minimizes the Euclidean 2-norm `|| b - a x ||^2`. The equation may
    be under-, well-, or over- determined (i.e., the number of
    linearly independent rows of `a` can be less than, equal to, or
    greater than its number of linearly independent columns). If `a`
    is square and of full rank, then `x` (but for round-off error) is
    the "exact" solution of the equation.

    Parameters
    ----------
    a : (M, N) array_like
        "Coefficient" matrix.
    b : {(M,), (M, K)} array_like
        Ordinate or "dependent variable" values.

    Returns
    -------
    x : {(N,), (N, K)} Array
        Least-squares solution.
    residuals : {(1,), (K,)} Array
        Sums of residuals; squared Euclidean 2-norm for each column in
        ``b - a*x``.
    rank : Array
        Rank of matrix `a`.
    s : (min(M, N),) Array
        Singular values of `a`.
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array._array_expr.linalg._qr import qr

    a = asanyarray(a)
    b = asanyarray(b)

    q, r = qr(a)
    x = solve_triangular(r, q.T.conj().dot(b))
    residuals = b - a.dot(x)
    residuals = abs(residuals**2).sum(axis=0, keepdims=b.ndim == 1)

    rank_expr = LstsqRank(r.expr)
    s_expr = LstsqSingular(r.expr)

    return x, residuals, new_collection(rank_expr), new_collection(s_expr)

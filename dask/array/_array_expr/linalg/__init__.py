"""Linear algebra submodule for array-expr.

This module provides native expression-based implementations of linear algebra
operations for the array-expr system.
"""

from dask.array._array_expr.linalg._cholesky import cholesky
from dask.array._array_expr.linalg._lu import lu
from dask.array._array_expr.linalg._norm import norm
from dask.array._array_expr.linalg._qr import qr, sfqr, tsqr
from dask.array._array_expr.linalg._solve import inv, lstsq, solve, solve_triangular
from dask.array._array_expr.linalg._svd import (
    compression_level,
    compression_matrix,
    svd,
    svd_compressed,
)

__all__ = [
    "cholesky",
    "compression_level",
    "compression_matrix",
    "inv",
    "lstsq",
    "lu",
    "norm",
    "qr",
    "sfqr",
    "solve",
    "solve_triangular",
    "svd",
    "svd_compressed",
    "tsqr",
]

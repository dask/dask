"""Array routines for array-expr."""

# Direct imports from submodules
# Re-exports from other modules
from dask.array._array_expr._blockwise import outer  # noqa: F401
from dask.array._array_expr._ufunc import (  # noqa: F401
    allclose,
    around,
    isclose,
    isnull,
    notnull,
    round,
)
from dask.array._array_expr.routines._apply import apply_along_axis, apply_over_axes
from dask.array._array_expr.routines._bincount import bincount
from dask.array._array_expr.routines._broadcast import broadcast_arrays, unify_chunks
from dask.array._array_expr.routines._coarsen import aligned_coarsen_chunks, coarsen
from dask.array._array_expr.routines._diff import diff
from dask.array._array_expr.routines._gradient import gradient
from dask.array._array_expr.routines._indexing import ravel_multi_index, unravel_index
from dask.array._array_expr.routines._insert_delete import (
    append,
    delete,
    ediff1d,
    insert,
)
from dask.array._array_expr.routines._misc import (
    compress,
    ndim,
    result_type,
    shape,
    take,
)
from dask.array._array_expr.routines._nonzero import (
    argwhere,
    count_nonzero,
    flatnonzero,
    isnonzero,
    nonzero,
)
from dask.array._array_expr.routines._search import isin, searchsorted
from dask.array._array_expr.routines._select import (
    choose,
    digitize,
    extract,
    piecewise,
    select,
)
from dask.array._array_expr.routines._statistics import average, corrcoef, cov
from dask.array._array_expr.routines._topk import argtopk, topk
from dask.array._array_expr.routines._triangular import (
    tril,
    tril_indices,
    tril_indices_from,
    triu,
    triu_indices,
    triu_indices_from,
)
from dask.array._array_expr.routines._unique import union1d, unique
from dask.array._array_expr.routines._where import where

__all__ = [
    "aligned_coarsen_chunks",
    "allclose",
    "append",
    "apply_along_axis",
    "apply_over_axes",
    "argwhere",
    "argtopk",
    "around",
    "average",
    "bincount",
    "broadcast_arrays",
    "choose",
    "coarsen",
    "compress",
    "corrcoef",
    "count_nonzero",
    "cov",
    "delete",
    "diff",
    "digitize",
    "ediff1d",
    "extract",
    "flatnonzero",
    "gradient",
    "insert",
    "isclose",
    "isin",
    "isnonzero",
    "isnull",
    "ndim",
    "nonzero",
    "notnull",
    "outer",
    "piecewise",
    "ravel_multi_index",
    "result_type",
    "round",
    "searchsorted",
    "select",
    "shape",
    "take",
    "topk",
    "tril",
    "tril_indices",
    "tril_indices_from",
    "triu",
    "triu_indices",
    "triu_indices_from",
    "unify_chunks",
    "union1d",
    "unique",
    "unravel_index",
    "where",
]

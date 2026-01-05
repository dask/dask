"""Re-exports from routines submodules and other locations.

This module maintains backward compatibility by re-exporting all routines
from their new locations.
"""

from __future__ import annotations

# Re-exports from _blockwise
from dask.array._array_expr._blockwise import outer  # noqa: F401

# Re-exports from _ufunc
from dask.array._array_expr._ufunc import (  # noqa: F401
    allclose,
    around,
    isclose,
    isnull,
    notnull,
    round,
)

# Re-exports from routines submodules
from dask.array._array_expr.routines._apply import (  # noqa: F401
    apply_along_axis,
    apply_over_axes,
)
from dask.array._array_expr.routines._bincount import bincount  # noqa: F401
from dask.array._array_expr.routines._broadcast import (  # noqa: F401
    broadcast_arrays,
    unify_chunks,
)
from dask.array._array_expr.routines._coarsen import (  # noqa: F401
    Coarsen,
    aligned_coarsen_chunks,
    coarsen,
)
from dask.array._array_expr.routines._gradient import gradient  # noqa: F401
from dask.array._array_expr.routines._indexing import (  # noqa: F401
    ravel_multi_index,
    unravel_index,
)
from dask.array._array_expr.routines._insert_delete import (  # noqa: F401
    append,
    delete,
    ediff1d,
    insert,
)
from dask.array._array_expr.routines._misc import (  # noqa: F401
    compress,
    ndim,
    result_type,
    shape,
    take,
)
from dask.array._array_expr.routines._nonzero import (  # noqa: F401
    argwhere,
    count_nonzero,
    flatnonzero,
    isnonzero,
    nonzero,
)
from dask.array._array_expr.routines._search import (  # noqa: F401
    isin,
    searchsorted,
)
from dask.array._array_expr.routines._select import (  # noqa: F401
    choose,
    digitize,
    extract,
    piecewise,
    select,
)
from dask.array._array_expr.routines._statistics import (  # noqa: F401
    average,
    corrcoef,
    cov,
)
from dask.array._array_expr.routines._topk import argtopk, topk  # noqa: F401
from dask.array._array_expr.routines._triangular import (  # noqa: F401
    tril,
    tril_indices,
    tril_indices_from,
    triu,
    triu_indices,
    triu_indices_from,
)
from dask.array._array_expr.routines._unique import union1d, unique  # noqa: F401

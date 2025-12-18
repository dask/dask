"""Array routines."""

# Import from submodules (these don't trigger circular imports)
from dask.array._array_expr.routines._diff import diff
from dask.array._array_expr.routines._where import where


def __getattr__(name):
    """Lazy import of routines from _routines to avoid circular imports."""
    _routines_names = {
        "round",
        "around",
        "isclose",
        "allclose",
        "isnull",
        "notnull",
        "append",
        "count_nonzero",
        "gradient",
        "searchsorted",
        "compress",
        "broadcast_arrays",
        "unify_chunks",
        "shape",
        "ndim",
        "result_type",
        "take",
        "outer",
        "tril",
        "triu",
    }
    if name in _routines_names:
        from dask.array._array_expr import _routines

        return getattr(_routines, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "round",
    "around",
    "isclose",
    "allclose",
    "isnull",
    "notnull",
    "append",
    "count_nonzero",
    "diff",
    "gradient",
    "searchsorted",
    "compress",
    "broadcast_arrays",
    "unify_chunks",
    "shape",
    "ndim",
    "result_type",
    "take",
    "outer",
    "tril",
    "triu",
    "where",
]

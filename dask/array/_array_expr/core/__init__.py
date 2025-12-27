"""Core array types and wrapping functions.

This module re-exports the core Array class and conversion functions.
"""

from dask.array._array_expr.core._blockwise_funcs import blockwise, elemwise
from dask.array._array_expr.core._conversion import (
    array,
    asanyarray,
    asarray,
    from_array,
)
from dask.array._array_expr.core._from_graph import from_graph


def __getattr__(name):
    """Lazy import of Array to avoid circular imports."""
    if name == "Array":
        from dask.array._array_expr._collection import Array

        return Array
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Array",
    "from_array",
    "from_graph",
    "asarray",
    "asanyarray",
    "array",
    "blockwise",
    "elemwise",
]

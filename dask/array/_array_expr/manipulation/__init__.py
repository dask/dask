"""Array manipulation functions: flip, transpose, reshape, expand_dims, etc."""

# Import from module files
from dask.array._array_expr.manipulation._flip import flip, flipud, fliplr, rot90
from dask.array._array_expr.manipulation._transpose import (
    swapaxes,
    moveaxis,
    rollaxis,
    transpose,
)
from dask.array._array_expr.manipulation._expand import (
    expand_dims,
    atleast_1d,
    atleast_2d,
    atleast_3d,
)
from dask.array._array_expr.manipulation._roll import roll


def __getattr__(name):
    """Lazy import of reshape and ravel to avoid circular imports."""
    if name in ("reshape", "ravel"):
        from dask.array._array_expr import _collection
        return getattr(_collection, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "flip",
    "flipud",
    "fliplr",
    "rot90",
    "swapaxes",
    "moveaxis",
    "rollaxis",
    "transpose",
    "expand_dims",
    "atleast_1d",
    "atleast_2d",
    "atleast_3d",
    "roll",
    "reshape",
    "ravel",
]

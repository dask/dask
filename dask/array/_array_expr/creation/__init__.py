"""Array creation functions for array-expr."""

from __future__ import annotations

from ._arange import Arange, arange
from ._diag import Diag1D, Diag2DSimple, diag
from ._diagonal import Diagonal, diagonal
from ._eye import Eye, eye
from ._linspace import Linspace, linspace
from ._mesh import fromfunction, indices, meshgrid
from ._ones_zeros import (
    BroadcastTrick,
    Empty,
    Full,
    Ones,
    Zeros,
    empty,
    empty_like,
    full,
    full_like,
    ones,
    ones_like,
    wrap,
    wrap_func_shape_as_first_arg,
    zeros,
    zeros_like,
)
from ._pad import pad
from ._repeat import repeat
from ._tile import tile
from ._tri import tri

__all__ = [
    # Classes
    "Arange",
    "BroadcastTrick",
    "Diag1D",
    "Diag2DSimple",
    "Diagonal",
    "Empty",
    "Eye",
    "Full",
    "Linspace",
    "Ones",
    "Zeros",
    # Functions
    "arange",
    "diag",
    "diagonal",
    "empty",
    "empty_like",
    "eye",
    "fromfunction",
    "full",
    "full_like",
    "indices",
    "linspace",
    "meshgrid",
    "ones",
    "ones_like",
    "pad",
    "repeat",
    "tile",
    "tri",
    "wrap",
    "wrap_func_shape_as_first_arg",
    "zeros",
    "zeros_like",
]

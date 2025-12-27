from __future__ import annotations

from dask.array._array_expr._expr import ArrayExpr


class IO(ArrayExpr):
    # Whether rechunk can be pushed into this IO expression by modifying its chunks.
    # False by default since many IO expressions have chunks that affect computation
    # (e.g., Random generates different values with different chunks).
    _can_rechunk_pushdown = False

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asarray
from dask.utils import derived_from


@derived_from(np)
def tile(A, reps):
    from dask.array._array_expr._collection import block

    from ._ones_zeros import empty

    try:
        tup = tuple(reps)
    except TypeError:
        tup = (reps,)
    if any(i < 0 for i in tup):
        raise ValueError("Negative `reps` are not allowed.")
    c = asarray(A)

    if all(tup):
        for nrep in tup[::-1]:
            c = nrep * [c]
        return block(c)

    d = len(tup)
    if d < c.ndim:
        tup = (1,) * (c.ndim - d) + tup
    if c.ndim < d:
        shape = (1,) * (d - c.ndim) + c.shape
    else:
        shape = c.shape
    shape_out = tuple(s * t for s, t in zip(shape, tup))
    return empty(shape=shape_out, dtype=c.dtype)

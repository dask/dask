from __future__ import annotations

import numpy as np

from dask.utils import derived_from


@derived_from(np)
def tri(N, M=None, k=0, dtype=float, chunks="auto", *, like=None):
    from dask.array._array_expr._ufunc import greater_equal
    from dask.array.core import normalize_chunks

    from ._arange import arange

    if M is None:
        M = N

    chunks = normalize_chunks(chunks, shape=(N, M), dtype=dtype)

    m = greater_equal(
        arange(N, chunks=chunks[0][0], like=like).reshape(1, N).T,
        arange(-k, M - k, chunks=chunks[1][0], like=like),
    )

    # Avoid making a copy if the requested type is already bool
    m = m.astype(dtype, copy=False)

    return m

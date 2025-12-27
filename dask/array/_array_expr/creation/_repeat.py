from __future__ import annotations

from numbers import Integral

import numpy as np
from tlz import sliding_window

from dask.array._array_expr._collection import concatenate
from dask.utils import cached_cumsum, derived_from


@derived_from(np)
def repeat(a, repeats, axis=None):
    if axis is None:
        if a.ndim == 1:
            axis = 0
        else:
            raise NotImplementedError("Must supply an integer axis value")

    if not isinstance(repeats, Integral):
        raise NotImplementedError("Only integer valued repeats supported")

    if -a.ndim <= axis < 0:
        axis += a.ndim
    elif not 0 <= axis <= a.ndim - 1:
        raise ValueError(f"axis(={axis}) out of bounds")

    if repeats == 0:
        return a[tuple(slice(None) if d != axis else slice(0) for d in range(a.ndim))]
    elif repeats == 1:
        return a

    cchunks = cached_cumsum(a.chunks[axis], initial_zero=True)
    slices = []
    for c_start, c_stop in sliding_window(2, cchunks):
        ls = np.linspace(c_start, c_stop, repeats).round(0)
        for ls_start, ls_stop in sliding_window(2, ls):
            if ls_start != ls_stop:
                slices.append(slice(ls_start, ls_stop))

    all_slice = slice(None, None, None)
    slices = [
        (all_slice,) * axis + (s,) + (all_slice,) * (a.ndim - axis - 1) for s in slices
    ]

    slabs = [a[slc] for slc in slices]

    out = []
    for slab in slabs:
        chunks = list(slab.chunks)
        assert len(chunks[axis]) == 1
        chunks[axis] = (chunks[axis][0] * repeats,)
        chunks = tuple(chunks)
        result = slab.map_blocks(
            np.repeat, repeats, axis=axis, chunks=chunks, dtype=slab.dtype
        )
        out.append(result)

    return concatenate(out, axis=axis)

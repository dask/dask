from __future__ import annotations

import itertools

import numpy as np

from dask.array._array_expr._collection import asarray
from dask.utils import derived_from


@derived_from(np)
def meshgrid(*xi, sparse=False, indexing="xy", **kwargs):
    from dask.array._array_expr._routines import broadcast_arrays
    from dask.array.numpy_compat import NUMPY_GE_200

    sparse = bool(sparse)

    if "copy" in kwargs:
        raise NotImplementedError("`copy` not supported")

    if kwargs:
        raise TypeError("unsupported keyword argument(s) provided")

    if indexing not in ("ij", "xy"):
        raise ValueError("`indexing` must be `'ij'` or `'xy'`")

    xi = [asarray(e) for e in xi]
    xi = [e.flatten() for e in xi]

    if indexing == "xy" and len(xi) > 1:
        xi[0], xi[1] = xi[1], xi[0]

    grid = []
    for i in range(len(xi)):
        s = len(xi) * [None]
        s[i] = slice(None)
        s = tuple(s)

        r = xi[i][s]

        grid.append(r)

    if not sparse:
        grid = broadcast_arrays(*grid)

    if indexing == "xy" and len(xi) > 1:
        grid = (grid[1], grid[0], *grid[2:])

    out_type = tuple if NUMPY_GE_200 else list
    return out_type(grid)


def indices(dimensions, dtype=int, chunks="auto"):
    """
    Implements NumPy's ``indices`` for Dask Arrays.

    Generates a grid of indices covering the dimensions provided.

    The final array has the shape ``(len(dimensions), *dimensions)``. The
    chunks are used to specify the chunking for axis 1 up to
    ``len(dimensions)``. The 0th axis always has chunks of length 1.

    Parameters
    ----------
    dimensions : sequence of ints
        The shape of the index grid.
    dtype : dtype, optional
        Type to use for the array. Default is ``int``.
    chunks : sequence of ints, str
        The size of each block.  Must be one of the following forms:

        - A blocksize like (500, 1000)
        - A size in bytes, like "100 MiB" which will choose a uniform
          block-like shape
        - The word "auto" which acts like the above, but uses a configuration
          value ``array.chunk-size`` for the chunk size

        Note that the last block will have fewer samples if ``len(array) % chunks != 0``.

    Returns
    -------
    grid : dask array
    """
    import numpy as np

    from dask.array._array_expr._collection import stack
    from dask.array.core import normalize_chunks

    from ._arange import arange
    from ._ones_zeros import empty

    dimensions = tuple(dimensions)
    dtype = np.dtype(dtype)
    chunks = normalize_chunks(chunks, shape=dimensions, dtype=dtype)

    if len(dimensions) != len(chunks):
        raise ValueError("Need same number of chunks as dimensions.")

    xi = []
    for i in range(len(dimensions)):
        xi.append(arange(dimensions[i], dtype=dtype, chunks=(chunks[i],)))

    grid = []
    if all(dimensions):
        grid = meshgrid(*xi, indexing="ij")

    if grid:
        grid = stack(grid)
    else:
        grid = empty((len(dimensions),) + dimensions, dtype=dtype, chunks=(1,) + chunks)

    return grid


@derived_from(np)
def fromfunction(func, chunks="auto", shape=None, dtype=None, **kwargs):

    from dask.array._array_expr._collection import blockwise
    from dask.array.core import normalize_chunks

    from ._arange import arange

    dtype = dtype or float
    chunks = normalize_chunks(chunks, shape, dtype=dtype)

    inds = tuple(range(len(shape)))

    arrs = [arange(s, dtype=dtype, chunks=c) for s, c in zip(shape, chunks)]
    arrs = meshgrid(*arrs, indexing="ij")

    args = sum(zip(arrs, itertools.repeat(inds)), ())

    res = blockwise(func, inds, *args, token="fromfunction", **kwargs)

    return res

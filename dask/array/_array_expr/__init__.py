from __future__ import annotations

import dask.array._array_expr._backends
from dask.array._array_expr import fft, random
from dask.array._array_expr._collection import (
    Array,
    array,
    asanyarray,
    asarray,
    atleast_1d,
    atleast_2d,
    atleast_3d,
    block,
    blockwise,
    broadcast_to,
    concatenate,
    dstack,
    elemwise,
    expand_dims,
    flip,
    fliplr,
    flipud,
    from_array,
    hstack,
    moveaxis,
    ravel,
    rechunk,
    reshape,
    roll,
    rollaxis,
    rot90,
    squeeze,
    stack,
    swapaxes,
    transpose,
    vstack,
)
from dask.array._array_expr.routines._diff import diff
from dask.array._array_expr.routines._where import where
from dask.array._array_expr._creation import (
    arange,
    diag,
    diagonal,
    empty,
    empty_like,
    eye,
    fromfunction,
    full,
    full_like,
    indices,
    linspace,
    meshgrid,
    ones,
    ones_like,
    pad,
    repeat,
    tile,
    tri,
    zeros,
    zeros_like,
)
from dask.array._array_expr._gufunc import *
from dask.array._array_expr._io import from_delayed, from_npy_stack, store, to_npy_stack
from dask.array._array_expr._einsum import einsum
from dask.array._array_expr._linalg import dot, matmul, tensordot, vdot
from dask.array._array_expr._map_blocks import map_blocks
from dask.array._array_expr._overlap import map_overlap, overlap, trim_overlap
from dask.array._array_expr._reductions import (
    _tree_reduce,
    arg_reduction,
    reduction,
    trace,
)
from dask.array._array_expr._histogram import histogram, histogram2d, histogramdd
from dask.array._array_expr._routines import (
    allclose,
    append,
    argwhere,
    around,
    broadcast_arrays,
    choose,
    compress,
    count_nonzero,
    digitize,
    extract,
    flatnonzero,
    gradient,
    isclose,
    isin,
    isnull,
    ndim,
    nonzero,
    notnull,
    outer,
    piecewise,
    result_type,
    round,
    searchsorted,
    select,
    shape,
    take,
    tril,
    triu,
    unify_chunks,
)
from dask.array._array_expr._ufunc import *

from __future__ import annotations

import dask.array._array_expr._backends
from dask.array._array_expr import random
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
    diff,
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
    where,
)
from dask.array._array_expr._creation import (
    arange,
    empty,
    empty_like,
    full,
    full_like,
    linspace,
    ones,
    ones_like,
    repeat,
    zeros,
    zeros_like,
)
from dask.array._array_expr._gufunc import *
from dask.array._array_expr._io import from_delayed, from_npy_stack, store, to_npy_stack
from dask.array._array_expr._linalg import dot, matmul, tensordot, vdot
from dask.array._array_expr._map_blocks import map_blocks
from dask.array._array_expr._overlap import map_overlap, overlap, trim_overlap
from dask.array._array_expr._reductions import _tree_reduce, arg_reduction, reduction
from dask.array._array_expr._histogram import histogram
from dask.array._array_expr._routines import (
    allclose,
    append,
    around,
    broadcast_arrays,
    compress,
    count_nonzero,
    gradient,
    isclose,
    isnull,
    ndim,
    notnull,
    outer,
    result_type,
    round,
    searchsorted,
    shape,
    unify_chunks,
)
from dask.array._array_expr._ufunc import *

import math
from functools import cached_property

import numpy as np

import dask.array as da
from dask.array import chunk
from dask.array.dispatch import (
    array_io_dispatch,
    concatenate_lookup,
    divide_lookup,
    einsum_lookup,
    empty_lookup,
    nannumel_lookup,
    numel_lookup,
    percentile_lookup,
    tensordot_lookup,
)
from dask.array.numpy_compat import divide as np_divide
from dask.array.numpy_compat import ma_divide
from dask.array.percentile import _percentile
from dask.backends import DaskBackendIOEntrypoint

concatenate_lookup.register((object, np.ndarray), np.concatenate)
tensordot_lookup.register((object, np.ndarray), np.tensordot)
einsum_lookup.register((object, np.ndarray), np.einsum)
empty_lookup.register((object, np.ndarray), np.empty)
empty_lookup.register(np.ma.masked_array, np.ma.empty)
divide_lookup.register((object, np.ndarray), np_divide)
divide_lookup.register(np.ma.masked_array, ma_divide)


@percentile_lookup.register(np.ndarray)
def percentile(a, q, method="linear"):
    return _percentile(a, q, method)


@concatenate_lookup.register(np.ma.masked_array)
def _concatenate(arrays, axis=0):
    out = np.ma.concatenate(arrays, axis=axis)
    fill_values = [i.fill_value for i in arrays if hasattr(i, "fill_value")]
    if any(isinstance(f, np.ndarray) for f in fill_values):
        raise ValueError(
            "Dask doesn't support masked array's with non-scalar `fill_value`s"
        )
    if fill_values:
        # If all the fill_values are the same copy over the fill value
        fill_values = np.unique(fill_values)
        if len(fill_values) == 1:
            out.fill_value = fill_values[0]
    return out


@tensordot_lookup.register(np.ma.masked_array)
def _tensordot(a, b, axes=2):
    # Much of this is stolen from numpy/core/numeric.py::tensordot
    # Please see license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    try:
        iter(axes)
    except TypeError:
        axes_a = list(range(-axes, 0))
        axes_b = list(range(0, axes))
    else:
        axes_a, axes_b = axes
    try:
        na = len(axes_a)
        axes_a = list(axes_a)
    except TypeError:
        axes_a = [axes_a]
        na = 1
    try:
        nb = len(axes_b)
        axes_b = list(axes_b)
    except TypeError:
        axes_b = [axes_b]
        nb = 1

    # a, b = asarray(a), asarray(b)  # <--- modified
    as_ = a.shape
    nda = a.ndim
    bs = b.shape
    ndb = b.ndim
    equal = True
    if na != nb:
        equal = False
    else:
        for k in range(na):
            if as_[axes_a[k]] != bs[axes_b[k]]:
                equal = False
                break
            if axes_a[k] < 0:
                axes_a[k] += nda
            if axes_b[k] < 0:
                axes_b[k] += ndb
    if not equal:
        raise ValueError("shape-mismatch for sum")

    # Move the axes to sum over to the end of "a"
    # and to the front of "b"
    notin = [k for k in range(nda) if k not in axes_a]
    newaxes_a = notin + axes_a
    N2 = 1
    for axis in axes_a:
        N2 *= as_[axis]
    newshape_a = (-1, N2)
    olda = [as_[axis] for axis in notin]

    notin = [k for k in range(ndb) if k not in axes_b]
    newaxes_b = axes_b + notin
    N2 = 1
    for axis in axes_b:
        N2 *= bs[axis]
    newshape_b = (N2, -1)
    oldb = [bs[axis] for axis in notin]

    at = a.transpose(newaxes_a).reshape(newshape_a)
    bt = b.transpose(newaxes_b).reshape(newshape_b)
    res = np.ma.dot(at, bt)
    return res.reshape(olda + oldb)


@tensordot_lookup.register_lazy("cupy")
@concatenate_lookup.register_lazy("cupy")
@nannumel_lookup.register_lazy("cupy")
@numel_lookup.register_lazy("cupy")
def register_cupy():
    import cupy

    from dask.array.dispatch import percentile_lookup

    concatenate_lookup.register(cupy.ndarray, cupy.concatenate)
    tensordot_lookup.register(cupy.ndarray, cupy.tensordot)
    percentile_lookup.register(cupy.ndarray, percentile)
    numel_lookup.register(cupy.ndarray, _numel_arraylike)
    nannumel_lookup.register(cupy.ndarray, _nannumel)

    @einsum_lookup.register(cupy.ndarray)
    def _cupy_einsum(*args, **kwargs):
        # NB: cupy does not accept `order` or `casting` kwargs - ignore
        kwargs.pop("casting", None)
        kwargs.pop("order", None)
        return cupy.einsum(*args, **kwargs)


@tensordot_lookup.register_lazy("cupyx")
@concatenate_lookup.register_lazy("cupyx")
def register_cupyx():

    from cupyx.scipy.sparse import spmatrix

    try:
        from cupyx.scipy.sparse import hstack, vstack
    except ImportError as e:
        raise ImportError(
            "Stacking of sparse arrays requires at least CuPy version 8.0.0"
        ) from e

    def _concat_cupy_sparse(L, axis=0):
        if axis == 0:
            return vstack(L)
        elif axis == 1:
            return hstack(L)
        else:
            msg = (
                "Can only concatenate cupy sparse matrices for axis in "
                "{0, 1}.  Got %s" % axis
            )
            raise ValueError(msg)

    concatenate_lookup.register(spmatrix, _concat_cupy_sparse)
    tensordot_lookup.register(spmatrix, _tensordot_scipy_sparse)


@tensordot_lookup.register_lazy("sparse")
@concatenate_lookup.register_lazy("sparse")
@nannumel_lookup.register_lazy("sparse")
@numel_lookup.register_lazy("sparse")
def register_sparse():
    import sparse

    concatenate_lookup.register(sparse.COO, sparse.concatenate)
    tensordot_lookup.register(sparse.COO, sparse.tensordot)
    # Enforce dense ndarray for the numel result, since the sparse
    # array will wind up being dense with an unpredictable fill_value.
    # https://github.com/dask/dask/issues/7169
    numel_lookup.register(sparse.COO, _numel_ndarray)
    nannumel_lookup.register(sparse.COO, _nannumel_sparse)


@tensordot_lookup.register_lazy("scipy")
@concatenate_lookup.register_lazy("scipy")
def register_scipy_sparse():
    import scipy.sparse

    def _concatenate(L, axis=0):
        if axis == 0:
            return scipy.sparse.vstack(L)
        elif axis == 1:
            return scipy.sparse.hstack(L)
        else:
            msg = (
                "Can only concatenate scipy sparse matrices for axis in "
                "{0, 1}.  Got %s" % axis
            )
            raise ValueError(msg)

    concatenate_lookup.register(scipy.sparse.spmatrix, _concatenate)
    tensordot_lookup.register(scipy.sparse.spmatrix, _tensordot_scipy_sparse)


def _tensordot_scipy_sparse(a, b, axes):
    assert a.ndim == b.ndim == 2
    assert len(axes[0]) == len(axes[1]) == 1
    (a_axis,) = axes[0]
    (b_axis,) = axes[1]
    assert a_axis in (0, 1) and b_axis in (0, 1)
    assert a.shape[a_axis] == b.shape[b_axis]
    if a_axis == 0 and b_axis == 0:
        return a.T * b
    elif a_axis == 0 and b_axis == 1:
        return a.T * b.T
    elif a_axis == 1 and b_axis == 0:
        return a * b
    elif a_axis == 1 and b_axis == 1:
        return a * b.T


@numel_lookup.register(np.ma.masked_array)
def _numel_masked(x, **kwargs):
    """Numel implementation for masked arrays."""
    return chunk.sum(np.ones_like(x), **kwargs)


@numel_lookup.register((object, np.ndarray))
def _numel_ndarray(x, **kwargs):
    """Numel implementation for arrays that want to return numel of type ndarray."""
    return _numel(x, coerce_np_ndarray=True, **kwargs)


def _numel_arraylike(x, **kwargs):
    """Numel implementation for arrays that want to return numel of the same type."""
    return _numel(x, coerce_np_ndarray=False, **kwargs)


def _numel(x, coerce_np_ndarray: bool, **kwargs):
    """
    A reduction to count the number of elements.

    This has an additional kwarg in coerce_np_ndarray, which determines
    whether to ensure that the resulting array is a numpy.ndarray, or whether
    we allow it to be other array types via `np.full_like`.
    """
    shape = x.shape
    keepdims = kwargs.get("keepdims", False)
    axis = kwargs.get("axis", None)
    dtype = kwargs.get("dtype", np.float64)

    if axis is None:
        prod = np.prod(shape, dtype=dtype)
        if keepdims is False:
            return prod

        if coerce_np_ndarray:
            return np.full(shape=(1,) * len(shape), fill_value=prod, dtype=dtype)
        else:
            return np.full_like(x, prod, shape=(1,) * len(shape), dtype=dtype)

    if not isinstance(axis, (tuple, list)):
        axis = [axis]

    prod = math.prod(shape[dim] for dim in axis)
    if keepdims is True:
        new_shape = tuple(
            shape[dim] if dim not in axis else 1 for dim in range(len(shape))
        )
    else:
        new_shape = tuple(shape[dim] for dim in range(len(shape)) if dim not in axis)

    if coerce_np_ndarray:
        return np.broadcast_to(np.array(prod, dtype=dtype), new_shape)
    else:
        return np.full_like(x, prod, shape=new_shape, dtype=dtype)


@nannumel_lookup.register((object, np.ndarray))
def _nannumel(x, **kwargs):
    """A reduction to count the number of elements, excluding nans"""
    return chunk.sum(~(np.isnan(x)), **kwargs)


def _nannumel_sparse(x, **kwargs):
    """
    A reduction to count the number of elements in a sparse array, excluding nans.
    This will in general result in a dense matrix with an unpredictable fill value.
    So make it official and convert it to dense.

    https://github.com/dask/dask/issues/7169
    """
    n = _nannumel(x, **kwargs)
    # If all dimensions are contracted, this will just be a number, otherwise we
    # want to densify it.
    return n.todense() if hasattr(n, "todense") else n


class NumpyIOEntrypoint(DaskBackendIOEntrypoint):
    def ones(self, *args, **kwargs):
        return da.wrap.ones_numpy(*args, **kwargs)

    def zeros(self, *args, **kwargs):
        return da.wrap.zeros_numpy(*args, **kwargs)

    def empty(self, *args, **kwargs):
        return da.wrap.empty_numpy(*args, **kwargs)

    def full(self, *args, **kwargs):
        return da.wrap._full_numpy(*args, **kwargs)

    def arange(self, *args, **kwargs):
        return da.creation.arange_numpy(*args, **kwargs)

    def default_random_state(self):
        if not hasattr(self, "_np_random_states"):
            self._np_random_states = da.random.RandomState()
        return self._np_random_states

    def new_random_state(self, state):
        return state

    def from_array(self, *args, **kwargs):
        return da.core.from_array_default(*args, **kwargs)


array_io_dispatch.register_backend("numpy", NumpyIOEntrypoint())


try:
    import cupy

    class CupyIOEntrypoint(DaskBackendIOEntrypoint):
        @cached_property
        def fallback(self):
            return NumpyIOEntrypoint()

        def move_from_fallback(self, x):
            return x.map_blocks(cupy.asarray)

        def ones(self, *args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            return self.fallback.ones(*args, meta=meta, **kwargs)

        def zeros(self, *args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            return self.fallback.zeros(*args, meta=meta, **kwargs)

        def empty(self, *args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            return self.fallback.empty(*args, meta=meta, **kwargs)

        def full(self, *args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            return self.fallback.full(*args, meta=meta, **kwargs)

        def arange(self, *args, like=None, **kwargs):
            like = cupy.empty(()) if like is None else like
            return self.fallback.arange(*args, like=like, **kwargs)

        def default_random_state(self):
            if not hasattr(self, "_cupy_random_states"):
                self._cupy_random_states = da.random.RandomState()
            return self._cupy_random_states

        def new_random_state(self, state):
            if state is None:
                state = cupy.random.RandomState
            return state

        def from_array(self, *args, **kwargs):
            x = self.fallback.from_array(*args, **kwargs)
            # TODO: Only call cupy.asarray if _meta is numpy
            return x.map_blocks(cupy.asarray)

    array_io_dispatch.register_backend("cupy", CupyIOEntrypoint())

except ImportError:
    pass

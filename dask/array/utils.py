from __future__ import absolute_import, division, print_function

import difflib
import functools
import math
import numbers
import os

import numpy as np
from toolz import frequencies, concat

from .core import Array
from ..highlevelgraph import HighLevelGraph
from ..utils import ignoring, is_arraylike

try:
    AxisError = np.AxisError
except AttributeError:
    try:
        np.array([0]).sum(axis=5)
    except Exception as e:
        AxisError = type(e)


def normalize_to_array(x):
    if "cupy" in str(type(x)):  # TODO: avoid explicit reference to cupy
        return x.get()
    else:
        return x


def meta_from_array(x, ndim=None, dtype=None):
    """ Normalize an array to appropriate meta object

    Parameters
    ----------
    x: array-like, callable
        Either an object that looks sufficiently like a Numpy array,
        or a callable that accepts shape and dtype keywords
    ndim: int
        Number of dimensions of the array
    dtype: Numpy dtype
        A valid input for ``np.dtype``

    Returns
    -------
    array-like with zero elements of the correct dtype
    """
    # If using x._meta, x must be a Dask Array, some libraries (e.g. zarr)
    # implement a _meta attribute that are incompatible with Dask Array._meta
    if hasattr(x, "_meta") and isinstance(x, Array):
        x = x._meta

    if dtype is None and x is None:
        raise ValueError("You must specify the meta or dtype of the array")

    if np.isscalar(x):
        x = np.array(x)

    if x is None:
        x = np.ndarray

    if isinstance(x, type):
        x = x(shape=(0,) * (ndim or 0), dtype=dtype)

    if (
        not hasattr(x, "shape")
        or not hasattr(x, "dtype")
        or not isinstance(x.shape, tuple)
    ):
        return x

    if isinstance(x, list) or isinstance(x, tuple):
        ndims = [
            0
            if isinstance(a, numbers.Number)
            else a.ndim
            if hasattr(a, "ndim")
            else len(a)
            for a in x
        ]
        a = [a if nd == 0 else meta_from_array(a, nd) for a, nd in zip(x, ndims)]
        return a if isinstance(x, list) else tuple(x)

    if ndim is None:
        ndim = x.ndim

    try:
        meta = x[tuple(slice(0, 0, None) for _ in range(x.ndim))]
        if meta.ndim != ndim:
            if ndim > x.ndim:
                meta = meta[(Ellipsis,) + tuple(None for _ in range(ndim - meta.ndim))]
                meta = meta[tuple(slice(0, 0, None) for _ in range(meta.ndim))]
            elif ndim == 0:
                meta = meta.sum()
            else:
                meta = meta.reshape((0,) * ndim)
    except Exception:
        meta = np.empty((0,) * ndim, dtype=dtype or x.dtype)

    if np.isscalar(meta):
        meta = np.array(meta)

    if dtype and meta.dtype != dtype:
        meta = meta.astype(dtype)

    return meta


def compute_meta(func, _dtype, *args, **kwargs):
    args_meta = [meta_from_array(x) if is_arraylike(x) else x for x in args]
    kwargs_meta = {
        k: meta_from_array(v) if is_arraylike(v) else v for k, v in kwargs.items()
    }

    # todo: look for alternative to this, causes issues when using map_blocks()
    # with np.vectorize, such as dask.array.routines._isnonzero_vec().
    if isinstance(func, np.vectorize):
        meta = func(*args_meta)
    else:
        try:
            meta = func(*args_meta, **kwargs_meta)
        except TypeError as e:
            if (
                "unexpected keyword argument" in str(e)
                or "is an invalid keyword for" in str(e)
                or "Did not understand the following kwargs" in str(e)
            ):
                raise
            else:
                return None
        except Exception:
            return None

    if _dtype and getattr(meta, "dtype", None) != _dtype:
        with ignoring(AttributeError):
            meta = meta.astype(_dtype)

    if np.isscalar(meta):
        meta = np.array(meta)

    return meta


def allclose(a, b, equal_nan=False, **kwargs):
    a = normalize_to_array(a)
    b = normalize_to_array(b)
    if getattr(a, "dtype", None) != "O":
        return np.allclose(a, b, equal_nan=equal_nan, **kwargs)
    if equal_nan:
        return a.shape == b.shape and all(
            np.isnan(b) if np.isnan(a) else a == b for (a, b) in zip(a.flat, b.flat)
        )
    return (a == b).all()


def same_keys(a, b):
    def key(k):
        if isinstance(k, str):
            return (k, -1, -1, -1)
        else:
            return k

    return sorted(a.dask, key=key) == sorted(b.dask, key=key)


def _not_empty(x):
    return x.shape and 0 not in x.shape


def _check_dsk(dsk):
    """ Check that graph is well named and non-overlapping """
    if not isinstance(dsk, HighLevelGraph):
        return

    assert all(isinstance(k, (tuple, str)) for k in dsk.layers)
    freqs = frequencies(concat(dsk.dicts.values()))
    non_one = {k: v for k, v in freqs.items() if v != 1}
    assert not non_one, non_one


def assert_eq_shape(a, b, check_nan=True):
    for aa, bb in zip(a, b):
        if math.isnan(aa) or math.isnan(bb):
            if check_nan:
                assert math.isnan(aa) == math.isnan(bb)
        else:
            assert aa == bb


def assert_eq(a, b, check_shape=True, check_graph=True, check_meta=True, **kwargs):
    a_original = a
    b_original = b
    if isinstance(a, Array):
        assert a.dtype is not None
        adt = a.dtype
        if check_graph:
            _check_dsk(a.dask)
        a_meta = getattr(a, "_meta", None)
        a = a.compute(scheduler="sync")
        a_computed = a
        if hasattr(a, "todense"):
            a = a.todense()
        if not hasattr(a, "dtype"):
            a = np.array(a, dtype="O")
        if _not_empty(a):
            assert a.dtype == a_original.dtype
        if check_shape:
            assert_eq_shape(a_original.shape, a.shape, check_nan=False)
    else:
        if not hasattr(a, "dtype"):
            a = np.array(a, dtype="O")
        adt = getattr(a, "dtype", None)

    if isinstance(b, Array):
        assert b.dtype is not None
        bdt = b.dtype
        if check_graph:
            _check_dsk(b.dask)
        b_meta = getattr(b, "_meta", None)
        b = b.compute(scheduler="sync")
        b_computed = b
        if not hasattr(b, "dtype"):
            b = np.array(b, dtype="O")
        if hasattr(b, "todense"):
            b = b.todense()
        if _not_empty(b):
            assert b.dtype == b_original.dtype
        if check_shape:
            assert_eq_shape(b_original.shape, b.shape, check_nan=False)
    else:
        if not hasattr(b, "dtype"):
            b = np.array(b, dtype="O")
        bdt = getattr(b, "dtype", None)

    if str(adt) != str(bdt):
        # Ignore check for matching length of flexible dtypes, since Array._meta
        # can't encode that information
        if adt.type == bdt.type and not (adt.type == np.bytes_ or adt.type == np.str_):
            diff = difflib.ndiff(str(adt).splitlines(), str(bdt).splitlines())
            raise AssertionError(
                "string repr are different" + os.linesep + os.linesep.join(diff)
            )

    try:
        assert a.shape == b.shape
        if check_meta:
            if hasattr(a, "_meta") and hasattr(b, "_meta"):
                assert_eq(a._meta, b._meta)
            if hasattr(a_original, "_meta"):
                assert a_original._meta.ndim == a.ndim
                if a_meta is not None:
                    assert type(a_original._meta) == type(a_meta)
                    if not (np.isscalar(a_meta) or np.isscalar(a_computed)):
                        assert type(a_meta) == type(a_computed)
            if hasattr(b_original, "_meta"):
                assert b_original._meta.ndim == b.ndim
                if b_meta is not None:
                    assert type(b_original._meta) == type(b_meta)
                    if not (np.isscalar(b_meta) or np.isscalar(b_computed)):
                        assert type(b_meta) == type(b_computed)
        assert allclose(a, b, **kwargs)
        return True
    except TypeError:
        pass

    c = a == b

    if isinstance(c, np.ndarray):
        assert c.all()
    else:
        assert c

    return True


def safe_wraps(wrapped, assigned=functools.WRAPPER_ASSIGNMENTS):
    """Like functools.wraps, but safe to use even if wrapped is not a function.

    Only needed on Python 2.
    """
    if all(hasattr(wrapped, attr) for attr in assigned):
        return functools.wraps(wrapped, assigned=assigned)
    else:
        return lambda x: x


def empty_like_safe(a, shape, **kwargs):
    """
    Return np.empty_like(a, shape=shape, **kwargs) if the shape argument
    is supported (requires NumPy >= 1.17), otherwise falls back to
    using the old behavior, returning np.empty(shape, **kwargs).
    """
    try:
        return np.empty_like(a, shape=shape, **kwargs)
    except TypeError:
        return np.empty(shape, **kwargs)


def full_like_safe(a, fill_value, shape, **kwargs):
    """
    Return np.full_like(a, fill_value, shape=shape, **kwargs) if the
    shape argument is supported (requires NumPy >= 1.17), otherwise
    falls back to using the old behavior, returning
    np.full(shape, fill_value, **kwargs).
    """
    try:
        return np.full_like(a, fill_value, shape=shape, **kwargs)
    except TypeError:
        return np.full(shape, fill_value, **kwargs)


def ones_like_safe(a, shape, **kwargs):
    """
    Return np.ones_like(a, shape=shape, **kwargs) if the shape argument
    is supported (requires NumPy >= 1.17), otherwise falls back to
    using the old behavior, returning np.ones(shape, **kwargs).
    """
    try:
        return np.ones_like(a, shape=shape, **kwargs)
    except TypeError:
        return np.ones(shape, **kwargs)


def zeros_like_safe(a, shape, **kwargs):
    """
    Return np.zeros_like(a, shape=shape, **kwargs) if the shape argument
    is supported (requires NumPy >= 1.17), otherwise falls back to
    using the old behavior, returning np.zeros(shape, **kwargs).
    """
    try:
        return np.zeros_like(a, shape=shape, **kwargs)
    except TypeError:
        return np.zeros(shape, **kwargs)


def validate_axis(axis, ndim):
    """ Validate an input to axis= keywords """
    if isinstance(axis, (tuple, list)):
        return tuple(validate_axis(ax, ndim) for ax in axis)
    if not isinstance(axis, numbers.Integral):
        raise TypeError("Axis value must be an integer, got %s" % axis)
    if axis < -ndim or axis >= ndim:
        raise AxisError(
            "Axis %d is out of bounds for array of dimension %d" % (axis, ndim)
        )
    if axis < 0:
        axis += ndim
    return axis


def _is_nep18_active():
    class A:
        def __array_function__(self, *args, **kwargs):
            return True

    try:
        return np.concatenate([A()])
    except ValueError:
        return False


IS_NEP18_ACTIVE = _is_nep18_active()

import difflib
import functools
import itertools
import math
import numbers
import os
import warnings

import numpy as np
from tlz import concat, frequencies

from ..highlevelgraph import HighLevelGraph
from ..utils import has_keyword, ignoring, is_arraylike, is_cupy_type
from .core import Array

try:
    AxisError = np.AxisError
except AttributeError:
    try:
        np.array([0]).sum(axis=5)
    except Exception as e:
        AxisError = type(e)


def normalize_to_array(x):
    if is_cupy_type(x):
        return x.get()
    else:
        return x


def meta_from_array(x, ndim=None, dtype=None):
    """Normalize an array to appropriate meta object

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
    elif dtype is None and hasattr(x, "dtype"):
        dtype = x.dtype

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
        try:
            meta = meta.astype(dtype)
        except ValueError as e:
            if (
                any(
                    s in str(e)
                    for s in [
                        "invalid literal",
                        "could not convert string to float",
                    ]
                )
                and meta.dtype.kind in "SU"
            ):
                meta = np.array([]).astype(dtype)
            else:
                raise e

    return meta


def compute_meta(func, _dtype, *args, **kwargs):
    with np.errstate(all="ignore"), warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)

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
                # some reduction functions need to know they are computing meta
                if has_keyword(func, "computing_meta"):
                    kwargs_meta["computing_meta"] = True
                meta = func(*args_meta, **kwargs_meta)
            except TypeError as e:
                if any(
                    s in str(e)
                    for s in [
                        "unexpected keyword argument",
                        "is an invalid keyword for",
                        "Did not understand the following kwargs",
                    ]
                ):
                    raise
                else:
                    return None
            except ValueError as e:
                # min/max functions have no identity, attempt to use the first meta
                if "zero-size array to reduction operation" in str(e):
                    meta = args_meta[0]
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

    dsk.validate()
    assert all(isinstance(k, (tuple, str)) for k in dsk.layers)
    freqs = frequencies(concat(dsk.layers.values()))
    non_one = {k: v for k, v in freqs.items() if v != 1}
    assert not non_one, non_one


def assert_eq_shape(a, b, check_nan=True):
    for aa, bb in zip(a, b):
        if math.isnan(aa) or math.isnan(bb):
            if check_nan:
                assert math.isnan(aa) == math.isnan(bb)
        else:
            assert aa == bb


def _check_chunks(x):
    x = x.persist(scheduler="sync")
    for idx in itertools.product(*(range(len(c)) for c in x.chunks)):
        chunk = x.dask[(x.name,) + idx]
        if not hasattr(chunk, "dtype"):
            chunk = np.array(chunk, dtype="O")
        expected_shape = tuple(c[i] for c, i in zip(x.chunks, idx))
        assert_eq_shape(expected_shape, chunk.shape, check_nan=False)
        assert chunk.dtype == x.dtype
    return x


def _get_dt_meta_computed(x, check_shape=True, check_graph=True, check_chunks=True):
    x_original = x
    x_meta = None
    x_computed = None

    if isinstance(x, Array):
        assert x.dtype is not None
        adt = x.dtype
        if check_graph:
            _check_dsk(x.dask)
        x_meta = getattr(x, "_meta", None)
        if check_chunks:
            # Replace x with persisted version to avoid computing it twice.
            x = _check_chunks(x)
        x = x.compute(scheduler="sync")
        x_computed = x
        if hasattr(x, "todense"):
            x = x.todense()
        if not hasattr(x, "dtype"):
            x = np.array(x, dtype="O")
        if _not_empty(x):
            assert x.dtype == x_original.dtype
        if check_shape:
            assert_eq_shape(x_original.shape, x.shape, check_nan=False)
    else:
        if not hasattr(x, "dtype"):
            x = np.array(x, dtype="O")
        adt = getattr(x, "dtype", None)

    return x, adt, x_meta, x_computed


def assert_eq(
    a,
    b,
    check_shape=True,
    check_graph=True,
    check_meta=True,
    check_chunks=True,
    **kwargs,
):
    a_original = a
    b_original = b

    a, adt, a_meta, a_computed = _get_dt_meta_computed(
        a, check_shape=check_shape, check_graph=check_graph, check_chunks=check_chunks
    )
    b, bdt, b_meta, b_computed = _get_dt_meta_computed(
        b, check_shape=check_shape, check_graph=check_graph, check_chunks=check_chunks
    )

    if str(adt) != str(bdt):
        # Ignore check for matching length of flexible dtypes, since Array._meta
        # can't encode that information
        if adt.type == bdt.type and not (adt.type == np.bytes_ or adt.type == np.str_):
            diff = difflib.ndiff(str(adt).splitlines(), str(bdt).splitlines())
            raise AssertionError(
                "string repr are different" + os.linesep + os.linesep.join(diff)
            )

    try:
        assert (
            a.shape == b.shape
        ), f"a and b have different shapes (a: {a.shape}, b: {b.shape})"
        if check_meta:
            if hasattr(a, "_meta") and hasattr(b, "_meta"):
                assert_eq(a._meta, b._meta)
            if hasattr(a_original, "_meta"):
                msg = (
                    f"compute()-ing 'a' changes its number of dimensions "
                    f"(before: {a_original._meta.ndim}, after: {a.ndim})"
                )
                assert a_original._meta.ndim == a.ndim, msg
                if a_meta is not None:
                    msg = (
                        f"compute()-ing 'a' changes its type "
                        f"(before: {type(a_original._meta)}, after: {type(a_meta)})"
                    )
                    assert type(a_original._meta) == type(a_meta), msg
                    if not (np.isscalar(a_meta) or np.isscalar(a_computed)):
                        msg = (
                            f"compute()-ing 'a' results in a different type than implied by its metadata "
                            f"(meta: {type(a_meta)}, computed: {type(a_computed)})"
                        )
                        assert type(a_meta) == type(a_computed), msg
            if hasattr(b_original, "_meta"):
                msg = (
                    f"compute()-ing 'b' changes its number of dimensions "
                    f"(before: {b_original._meta.ndim}, after: {b.ndim})"
                )
                assert b_original._meta.ndim == b.ndim, msg
                if b_meta is not None:
                    msg = (
                        f"compute()-ing 'b' changes its type "
                        f"(before: {type(b_original._meta)}, after: {type(b_meta)})"
                    )
                    assert type(b_original._meta) == type(b_meta), msg
                    if not (np.isscalar(b_meta) or np.isscalar(b_computed)):
                        msg = (
                            f"compute()-ing 'b' results in a different type than implied by its metadata "
                            f"(meta: {type(b_meta)}, computed: {type(b_computed)})"
                        )
                        assert type(b_meta) == type(b_computed), msg
        msg = "found values in 'a' and 'b' which differ by more than the allowed amount"
        assert allclose(a, b, **kwargs), msg
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


def _dtype_of(a):
    """Determine dtype of an array-like."""
    try:
        # Check for the attribute before using asanyarray, because some types
        # (notably sparse arrays) don't work with it.
        return a.dtype
    except AttributeError:
        return np.asanyarray(a).dtype


def empty_like_safe(a, shape, **kwargs):
    """
    Return np.empty_like(a, shape=shape, **kwargs) if the shape argument
    is supported (requires NumPy >= 1.17), otherwise falls back to
    using the old behavior, returning np.empty(shape, **kwargs).
    """
    try:
        return np.empty_like(a, shape=shape, **kwargs)
    except TypeError:
        kwargs.setdefault("dtype", _dtype_of(a))
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
        kwargs.setdefault("dtype", _dtype_of(a))
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
        kwargs.setdefault("dtype", _dtype_of(a))
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
        kwargs.setdefault("dtype", _dtype_of(a))
        return np.zeros(shape, **kwargs)


def arange_safe(*args, like, **kwargs):
    """
    Use the `like=` from `np.arange` to create a new array dispatching
    to the downstream library. If that fails, falls back to the
    default NumPy behavior, resulting in a `numpy.ndarray`.
    """
    if like is None:
        return np.arange(*args, **kwargs)
    else:
        try:
            return np.arange(*args, like=meta_from_array(like), **kwargs)
        except TypeError:
            return np.arange(*args, **kwargs)


def _array_like_safe(np_func, da_func, a, like, **kwargs):
    if like is a and hasattr(a, "__array_function__"):
        return a

    if isinstance(like, Array):
        return da_func(a, **kwargs)
    elif isinstance(a, Array):
        if is_cupy_type(a._meta):
            a = a.compute(scheduler="sync")

    try:
        return np_func(a, like=meta_from_array(like), **kwargs)
    except TypeError:
        return np_func(a, **kwargs)


def array_safe(a, like, **kwargs):
    """
    If `a` is `dask.array`, return `dask.array.asarray(a, **kwargs)`,
    otherwise return `np.asarray(a, like=like, **kwargs)`, dispatching
    the call to the library that implements the like array. Note that
    when `a` is a `dask.Array` backed by `cupy.ndarray` but `like`
    isn't, this function will call `a.compute(scheduler="sync")`
    before `np.array`, as downstream libraries are unlikely to know how
    to convert a `dask.Array` and CuPy doesn't implement `__array__` to
    prevent implicit copies to host.
    """
    from .routines import array

    return _array_like_safe(np.array, array, a, like, **kwargs)


def asarray_safe(a, like, **kwargs):
    """
    If a is dask.array, return dask.array.asarray(a, **kwargs),
    otherwise return np.asarray(a, like=like, **kwargs), dispatching
    the call to the library that implements the like array. Note that
    when a is a dask.Array but like isn't, this function will call
    a.compute(scheduler="sync") before np.asarray, as downstream
    libraries are unlikely to know how to convert a dask.Array.
    """
    from .core import asarray

    return _array_like_safe(np.asarray, asarray, a, like, **kwargs)


def asanyarray_safe(a, like, **kwargs):
    """
    If a is dask.array, return dask.array.asanyarray(a, **kwargs),
    otherwise return np.asanyarray(a, like=like, **kwargs), dispatching
    the call to the library that implements the like array. Note that
    when a is a dask.Array but like isn't, this function will call
    a.compute(scheduler="sync") before np.asanyarray, as downstream
    libraries are unlikely to know how to convert a dask.Array.
    """
    from .core import asanyarray

    return _array_like_safe(np.asanyarray, asanyarray, a, like, **kwargs)


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


def svd_flip(u, v, u_based_decision=False):
    """Sign correction to ensure deterministic output from SVD.

    This function is useful for orienting eigenvectors such that
    they all lie in a shared but arbitrary half-space. This makes
    it possible to ensure that results are equivalent across SVD
    implementations and random number generator states.

    Parameters
    ----------

    u : (M, K) array_like
        Left singular vectors (in columns)
    v : (K, N) array_like
        Right singular vectors (in rows)
    u_based_decision: bool
        Whether or not to choose signs based
        on `u` rather than `v`, by default False

    Returns
    -------

    u : (M, K) array_like
        Left singular vectors with corrected sign
    v:  (K, N) array_like
        Right singular vectors with corrected sign
    """
    # Determine half-space in which all singular vectors
    # lie relative to an arbitrary vector; summation
    # equivalent to dot product with row vector of ones
    if u_based_decision:
        dtype = u.dtype
        signs = np.sum(u, axis=0, keepdims=True)
    else:
        dtype = v.dtype
        signs = np.sum(v, axis=1, keepdims=True).T
    signs = 2.0 * ((signs >= 0) - 0.5).astype(dtype)
    # Force all singular vectors into same half-space
    u, v = u * signs, v * signs.T
    return u, v


def _is_nep18_active():
    class A:
        def __array_function__(self, *args, **kwargs):
            return True

    try:
        return np.concatenate([A()])
    except ValueError:
        return False


IS_NEP18_ACTIVE = _is_nep18_active()

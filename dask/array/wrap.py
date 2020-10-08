from functools import partial
from itertools import product

import numpy as np

from tlz import curry

from ..base import tokenize
from ..utils import funcname
from .core import Array, normalize_chunks
from .utils import (
    meta_from_array,
    empty_like_safe,
    full_like_safe,
    ones_like_safe,
    zeros_like_safe,
)


def _parse_wrap_args(func, args, kwargs, shape):
    if isinstance(shape, np.ndarray):
        shape = shape.tolist()

    if not isinstance(shape, (tuple, list)):
        shape = (shape,)

    name = kwargs.pop("name", None)
    chunks = kwargs.pop("chunks", "auto")

    dtype = kwargs.pop("dtype", None)
    if dtype is None:
        dtype = func(shape, *args, **kwargs).dtype
    dtype = np.dtype(dtype)

    chunks = normalize_chunks(chunks, shape, dtype=dtype)

    name = name or funcname(func) + "-" + tokenize(
        func, shape, chunks, dtype, args, kwargs
    )

    return {
        "shape": shape,
        "dtype": dtype,
        "kwargs": kwargs,
        "chunks": chunks,
        "name": name,
    }


def wrap_func_shape_as_first_arg(func, *args, **kwargs):
    """
    Transform np creation function into blocked version
    """
    if "shape" not in kwargs:
        shape, args = args[0], args[1:]
    else:
        shape = kwargs.pop("shape")

    if isinstance(shape, Array):
        raise TypeError(
            "Dask array input not supported. "
            "Please use tuple, list, or a 1D numpy array instead."
        )

    parsed = _parse_wrap_args(func, args, kwargs, shape)
    shape = parsed["shape"]
    dtype = parsed["dtype"]
    chunks = parsed["chunks"]
    name = parsed["name"]
    kwargs = parsed["kwargs"]

    keys = product([name], *[range(len(bd)) for bd in chunks])
    shapes = product(*chunks)
    func = partial(func, dtype=dtype, **kwargs)
    vals = ((func,) + (s,) + args for s in shapes)

    dsk = dict(zip(keys, vals))
    return Array(dsk, name, chunks, dtype=dtype, meta=kwargs.get("meta", None))


def wrap_func_like(func, *args, **kwargs):
    """
    Transform np creation function into blocked version
    """
    x = args[0]
    meta = meta_from_array(x)
    shape = kwargs.get("shape", x.shape)

    parsed = _parse_wrap_args(func, args, kwargs, shape)
    shape = parsed["shape"]
    dtype = parsed["dtype"]
    chunks = parsed["chunks"]
    name = parsed["name"]
    kwargs = parsed["kwargs"]

    keys = product([name], *[range(len(bd)) for bd in chunks])
    shapes = product(*chunks)
    shapes = list(shapes)
    kw = [kwargs for _ in shapes]
    for i, s in enumerate(list(shapes)):
        kw[i]["shape"] = s
    vals = ((partial(func, dtype=dtype, **k),) + args for (k, s) in zip(kw, shapes))

    dsk = dict(zip(keys, vals))

    return Array(dsk, name, chunks, meta=meta.astype(dtype))


def wrap_func_like_safe(func, func_like, *args, **kwargs):
    """
    Safe implementation for wrap_func_like(), attempts to use func_like(),
    if the shape keyword argument, falls back to func().
    """
    try:
        return func_like(*args, **kwargs)
    except TypeError:
        return func(*args, **kwargs)


@curry
def wrap(wrap_func, func, **kwargs):
    func_like = kwargs.pop("func_like", None)
    if func_like is None:
        f = partial(wrap_func, func, **kwargs)
    else:
        f = partial(wrap_func, func_like, **kwargs)
    template = """
    Blocked variant of %(name)s

    Follows the signature of %(name)s exactly except that it also features
    optional keyword arguments ``chunks: int, tuple, or dict`` and ``name: str``.

    Original signature follows below.
    """
    if func.__doc__ is not None:
        f.__doc__ = template % {"name": func.__name__} + func.__doc__
        f.__name__ = "blocked_" + func.__name__
    return f


w = wrap(wrap_func_shape_as_first_arg)


@curry
def _broadcast_trick_inner(func, shape, meta=(), *args, **kwargs):
    if shape == ():
        return np.broadcast_to(func(meta, shape=(), *args, **kwargs), shape)
    else:
        return np.broadcast_to(func(meta, shape=1, *args, **kwargs), shape)


def broadcast_trick(func):
    """
    Provide a decorator to wrap common numpy function with a broadcast trick.

    Dask arrays are currently immutable; thus when we know an array is uniform,
    we can replace the actual data by a single value and have all elements point
    to it, thus reducing the size.

    >>> x = np.broadcast_to(1, (100,100,100))
    >>> x.base.nbytes
    8

    Those array are not only more efficient locally, but dask serialisation is
    aware of the _real_ size of those array and thus can send them around
    efficiently and schedule accordingly.

    Note that those array are read-only and numpy will refuse to assign to them,
    so should be safe.
    """

    inner = _broadcast_trick_inner(func)

    if func.__doc__ is not None:
        inner.__doc__ = func.__doc__
        inner.__name__ = func.__name__
        if inner.__name__.endswith("_like_safe"):
            inner.__name__ = inner.__name__[:-10]
    return inner


ones = w(broadcast_trick(ones_like_safe), dtype="f8")
zeros = w(broadcast_trick(zeros_like_safe), dtype="f8")
empty = w(broadcast_trick(empty_like_safe), dtype="f8")


w_like = wrap(wrap_func_like_safe)


empty_like = w_like(np.empty, func_like=np.empty_like)


# full and full_like require special casing due to argument check on fill_value
# Generate wrapped functions only once
_full = w(broadcast_trick(full_like_safe))
_full_like = w_like(np.full, func_like=np.full_like)

# workaround for numpy doctest failure: https://github.com/numpy/numpy/pull/17472
_full.__doc__ = _full.__doc__.replace(
    "array([0.1,  0.1,  0.1,  0.1,  0.1,  0.1])",
    "array([0.1, 0.1, 0.1, 0.1, 0.1, 0.1])",
)


def full(shape, fill_value, *args, **kwargs):
    # np.isscalar has somewhat strange behavior:
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.isscalar.html
    if np.ndim(fill_value) != 0:
        raise ValueError(
            f"fill_value must be scalar. Received {type(fill_value).__name__} instead."
        )
    return _full(shape=shape, fill_value=fill_value, *args, **kwargs)


def full_like(a, fill_value, *args, **kwargs):
    if np.ndim(fill_value) != 0:
        raise ValueError(
            f"fill_value must be scalar. Received {type(fill_value).__name__} instead."
        )
    return _full_like(
        a=a,
        fill_value=fill_value,
        *args,
        **kwargs,
    )


full.__doc__ = _full.__doc__
full_like.__doc__ = _full_like.__doc__

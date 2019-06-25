from __future__ import absolute_import, division, print_function

from functools import partial
from itertools import product

import numpy as np

try:
    from cytoolz import curry
except ImportError:
    from toolz import curry

from ..base import tokenize
from ..utils import funcname
from .core import Array, normalize_chunks
from .utils import meta_from_array


def _parse_wrap_args(func, args, kwargs, shape):
    if isinstance(shape, np.ndarray):
        shape = shape.tolist()

    if not isinstance(shape, (tuple, list)):
        shape = (shape,)

    chunks = kwargs.pop("chunks", "auto")

    dtype = kwargs.pop("dtype", None)
    if dtype is None:
        dtype = func(shape, *args, **kwargs).dtype
    dtype = np.dtype(dtype)

    chunks = normalize_chunks(chunks, shape, dtype=dtype)
    name = kwargs.pop("name", None)

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
    return Array(dsk, name, chunks, dtype=dtype)


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

    Follows the signature of %(name)s exactly except that it also requires a
    keyword argument chunks=(...)

    Original signature follows below.
    """
    if func.__doc__ is not None:
        f.__doc__ = template % {"name": func.__name__} + func.__doc__
        f.__name__ = "blocked_" + func.__name__
    return f


w = wrap(wrap_func_shape_as_first_arg)

ones = w(np.ones, dtype="f8")
zeros = w(np.zeros, dtype="f8")
empty = w(np.empty, dtype="f8")
full = w(np.full)


w_like = wrap(wrap_func_like_safe)


ones_like = w_like(np.ones, func_like=np.ones_like)
zeros_like = w_like(np.zeros, func_like=np.zeros_like)
empty_like = w_like(np.empty, func_like=np.empty_like)
full_like = w_like(np.full, func_like=np.full_like)

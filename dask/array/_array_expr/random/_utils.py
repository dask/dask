from __future__ import annotations

import importlib

import numpy as np

from dask._collections import new_collection
from dask.array._array_expr._collection import Array
from dask.array.core import normalize_chunks
from dask.utils import typename


def _rng_from_bitgen(bitgen):
    # Assumes typename(bitgen) starts with importable
    # library name (e.g. "numpy" or "cupy")
    backend_name = typename(bitgen).split(".")[0]
    backend_lib = importlib.import_module(backend_name)
    return backend_lib.random.default_rng(bitgen)


def _shuffle(bit_generator, x, axis=0):
    """Shuffle array in place and advance bit generator state."""
    state_data = bit_generator.state
    new_bitgen = type(bit_generator)()
    new_bitgen.state = state_data
    state = _rng_from_bitgen(new_bitgen)
    state.shuffle(x, axis=axis)
    # Copy advanced state back to original so subsequent calls get different results
    bit_generator.state = new_bitgen.state


def _broadcast_array_arg(arg, size, target_chunks):
    """Broadcast and rechunk an array argument to match output shape."""
    from dask.array._array_expr._broadcast import broadcast_to
    from dask.array._array_expr.core._conversion import from_array

    if isinstance(arg, np.ndarray) and arg.shape:
        arg = from_array(arg, chunks=arg.shape)
        arg = broadcast_to(arg, size).rechunk(target_chunks)
    elif isinstance(arg, Array):
        arg = broadcast_to(arg, size).rechunk(target_chunks)
    return arg


def _wrap_func(
    rng, funcname, *args, size=None, chunks="auto", extra_chunks=(), **kwargs
):
    from ._expr import RandomNormal, RandomPoisson

    if size is not None and not isinstance(size, (tuple, list)):
        size = (size,)

    # Collect shapes from array arguments for broadcasting
    shapes = []
    for arg in args:
        if isinstance(arg, (np.ndarray, Array)) and arg.shape:
            shapes.append(arg.shape)
    for v in kwargs.values():
        if isinstance(v, (np.ndarray, Array)) and v.shape:
            shapes.append(v.shape)

    # Validate that all shapes can be broadcast together with size
    if size is not None and shapes:
        np.broadcast_shapes(*shapes, size)  # Raises ValueError if incompatible
    elif size is None and shapes:
        size = np.broadcast_shapes(*shapes)

    # Broadcast and rechunk array arguments to match output shape/chunks
    if size is not None and shapes:
        target_chunks = normalize_chunks(
            chunks, size, dtype=kwargs.get("dtype", np.float64)
        )
        args = tuple(_broadcast_array_arg(arg, size, target_chunks) for arg in args)
        kwargs = {
            k: _broadcast_array_arg(v, size, target_chunks) for k, v in kwargs.items()
        }

    # Dispatch to specific subclass if available
    if funcname == "normal":
        loc = kwargs.pop("loc", args[0] if len(args) > 0 else 0.0)
        scale = kwargs.pop("scale", args[1] if len(args) > 1 else 1.0)
        return new_collection(RandomNormal(rng, size, chunks, extra_chunks, loc, scale))
    elif funcname == "poisson":
        lam = args[0] if len(args) > 0 else kwargs.pop("lam", 1.0)
        return new_collection(RandomPoisson(rng, size, chunks, extra_chunks, lam))

    # Fallback: use generic Random with args/kwargs tuples
    from ._expr import Random

    return new_collection(
        Random(rng, funcname, size, chunks, extra_chunks, args, kwargs)
    )

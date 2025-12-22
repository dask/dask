from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asarray, concatenate
from dask.array.creation import (
    expand_pad_value,
    get_pad_shapes_chunks,
    linear_ramp_chunk,
    wrapped_pad_func,
)
from dask.array.utils import meta_from_array
from dask.utils import derived_from


def _pad_reuse_expr(array, pad_width, mode, **kwargs):
    """
    Helper function for padding boundaries with values in the array.

    Handles the cases where the padding is constructed from values in
    the array. Namely by reflecting them or tiling them to create periodic
    boundary constraints.
    """
    from dask.array._array_expr._collection import block
    from dask.array._array_expr.manipulation._flip import flip

    if mode in {"reflect", "symmetric"}:
        reflect_type = kwargs.get("reflect", "even")
        if reflect_type == "odd":
            raise NotImplementedError("`pad` does not support `reflect_type` of `odd`.")
        if reflect_type != "even":
            raise ValueError(
                "unsupported value for reflect_type, must be one of (`even`, `odd`)"
            )

    result = np.empty(array.ndim * (3,), dtype=object)
    for idx in np.ndindex(result.shape):
        select = []
        flip_axes = []
        for axis, (i, s, pw) in enumerate(zip(idx, array.shape, pad_width)):
            if mode == "wrap":
                pw = pw[::-1]

            if i < 1:
                if mode == "reflect":
                    select.append(slice(1, pw[0] + 1, None))
                else:
                    select.append(slice(None, pw[0], None))
            elif i > 1:
                if mode == "reflect":
                    select.append(slice(s - pw[1] - 1, s - 1, None))
                else:
                    select.append(slice(s - pw[1], None, None))
            else:
                select.append(slice(None))

            if i != 1 and mode in ["reflect", "symmetric"]:
                flip_axes.append(axis)

        select = tuple(select)

        if mode == "wrap":
            idx = tuple(2 - i for i in idx)

        chunk = array[select]
        # Apply flips for each axis that needs reversal
        for axis in flip_axes:
            chunk = flip(chunk, axis)
        result[idx] = chunk

    result = block(result.tolist())

    return result


def _pad_stats_expr(array, pad_width, mode, stat_length):
    """
    Helper function for padding boundaries with statistics from the array.

    In cases where the padding requires computations of statistics from part
    or all of the array, this function helps compute those statistics as
    requested and then adds those statistics onto the boundaries of the array.
    """
    from dask.array._array_expr._collection import block, broadcast_to
    from dask.array._array_expr._ufunc import rint

    if mode == "median":
        raise NotImplementedError("`pad` does not support `mode` of `median`.")

    stat_length = expand_pad_value(array, stat_length)

    result = np.empty(array.ndim * (3,), dtype=object)
    for idx in np.ndindex(result.shape):
        axes = []
        select = []
        pad_shape = []
        pad_chunks = []
        for d, (i, s, c, w, l) in enumerate(
            zip(idx, array.shape, array.chunks, pad_width, stat_length)
        ):
            if i < 1:
                axes.append(d)
                select.append(slice(None, l[0], None))
                pad_shape.append(w[0])
                pad_chunks.append(w[0])
            elif i > 1:
                axes.append(d)
                select.append(slice(s - l[1], None, None))
                pad_shape.append(w[1])
                pad_chunks.append(w[1])
            else:
                select.append(slice(None))
                pad_shape.append(s)
                pad_chunks.append(c)

        axes = tuple(axes)
        select = tuple(select)
        pad_shape = tuple(pad_shape)
        pad_chunks = tuple(pad_chunks)

        result_idx = array[select]
        if axes:
            stat_funcs = {"maximum": "max", "mean": "mean", "minimum": "min"}
            result_idx = getattr(result_idx, stat_funcs[mode])(axis=axes, keepdims=True)
            result_idx = broadcast_to(result_idx, pad_shape, chunks=pad_chunks)

            if mode == "mean":
                if np.issubdtype(array.dtype, np.integer):
                    result_idx = rint(result_idx)
                result_idx = result_idx.astype(array.dtype)

        result[idx] = result_idx

    result = block(result.tolist())

    return result


def _pad_udf_expr(array, pad_width, mode, **kwargs):
    """
    Helper function for padding boundaries with a user defined function.

    In cases where the padding requires a custom user defined function be
    applied to the array, this function assists in the prepping and
    application of this function to the Dask Array to construct the desired
    boundaries.
    """
    result = _pad_edge_expr(array, pad_width, "constant", constant_values=0)

    chunks = result.chunks
    for d in range(result.ndim):
        result = result.rechunk(
            chunks[:d] + (result.shape[d : d + 1],) + chunks[d + 1 :]
        )

        result = result.map_blocks(
            wrapped_pad_func,
            name="pad",
            dtype=result.dtype,
            pad_func=mode,
            iaxis_pad_width=pad_width[d],
            iaxis=d,
            pad_func_kwargs=kwargs,
        )

        result = result.rechunk(chunks)

    return result


def _pad_edge_expr(array, pad_width, mode, **kwargs):
    """
    Helper function for padding edges - array-expr version.

    Handles the cases where the only the values on the edge are needed.
    """
    from dask.array._array_expr._collection import broadcast_to
    from dask.array.utils import asarray_safe

    from ._ones_zeros import empty_like

    kwargs = {k: expand_pad_value(array, v) for k, v in kwargs.items()}

    result = array
    for d in range(array.ndim):
        pad_shapes, pad_chunks = get_pad_shapes_chunks(
            result, pad_width, (d,), mode=mode
        )
        pad_arrays = [result, result]

        if mode == "constant":
            constant_values = kwargs["constant_values"][d]
            constant_values = [
                asarray_safe(c, like=meta_from_array(array), dtype=result.dtype)
                for c in constant_values
            ]

            pad_arrays = [
                broadcast_to(v, s, c)
                for v, s, c in zip(constant_values, pad_shapes, pad_chunks)
            ]
        elif mode in ["edge", "linear_ramp"]:
            pad_slices = [result.ndim * [slice(None)], result.ndim * [slice(None)]]
            pad_slices[0][d] = slice(None, 1, None)
            pad_slices[1][d] = slice(-1, None, None)
            pad_slices = [tuple(sl) for sl in pad_slices]

            pad_arrays = [result[sl] for sl in pad_slices]

            if mode == "edge":
                pad_arrays = [
                    broadcast_to(a, s, c)
                    for a, s, c in zip(pad_arrays, pad_shapes, pad_chunks)
                ]
            elif mode == "linear_ramp":
                end_values = kwargs["end_values"][d]

                pad_arrays = [
                    a.map_blocks(
                        linear_ramp_chunk,
                        ev,
                        pw,
                        chunks=c,
                        dtype=result.dtype,
                        dim=d,
                        step=(2 * i - 1),
                    )
                    for i, (a, ev, pw, c) in enumerate(
                        zip(pad_arrays, end_values, pad_width[d], pad_chunks)
                    )
                ]
        elif mode == "empty":
            pad_arrays = [
                empty_like(array, shape=s, dtype=array.dtype, chunks=c)
                for s, c in zip(pad_shapes, pad_chunks)
            ]

        result = concatenate([pad_arrays[0], result, pad_arrays[1]], axis=d)

    return result


@derived_from(np)
def pad(array, pad_width, mode="constant", **kwargs):
    array = asarray(array)

    pad_width = expand_pad_value(array, pad_width)

    if callable(mode):
        return _pad_udf_expr(array, pad_width, mode, **kwargs)

    # Make sure that no unsupported keywords were passed for the current mode
    allowed_kwargs = {
        "empty": [],
        "edge": [],
        "wrap": [],
        "constant": ["constant_values"],
        "linear_ramp": ["end_values"],
        "maximum": ["stat_length"],
        "mean": ["stat_length"],
        "median": ["stat_length"],
        "minimum": ["stat_length"],
        "reflect": ["reflect_type"],
        "symmetric": ["reflect_type"],
    }
    try:
        unsupported_kwargs = set(kwargs) - set(allowed_kwargs[mode])
    except KeyError as e:
        raise ValueError(f"mode '{mode}' is not supported") from e
    if unsupported_kwargs:
        raise ValueError(
            f"unsupported keyword arguments for mode '{mode}': {unsupported_kwargs}"
        )

    if mode in {"maximum", "mean", "median", "minimum"}:
        stat_length = kwargs.get("stat_length", tuple((n, n) for n in array.shape))
        return _pad_stats_expr(array, pad_width, mode, stat_length)
    elif mode == "constant":
        kwargs.setdefault("constant_values", 0)
        return _pad_edge_expr(array, pad_width, mode, **kwargs)
    elif mode == "linear_ramp":
        kwargs.setdefault("end_values", 0)
        return _pad_edge_expr(array, pad_width, mode, **kwargs)
    elif mode in {"edge", "empty"}:
        return _pad_edge_expr(array, pad_width, mode)
    elif mode in ["reflect", "symmetric", "wrap"]:
        return _pad_reuse_expr(array, pad_width, mode, **kwargs)

    raise RuntimeError("unreachable")

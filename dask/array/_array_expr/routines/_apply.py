"""Apply functions for array-expr."""

from __future__ import annotations

import numpy as np

from dask.array._array_expr._collection import asarray
from dask.utils import derived_from, funcname


def _inner_apply_along_axis(arr, func1d, func1d_axis, func1d_args, func1d_kwargs):
    return np.apply_along_axis(func1d, func1d_axis, arr, *func1d_args, **func1d_kwargs)


@derived_from(np)
def apply_along_axis(func1d, axis, arr, *args, dtype=None, shape=None, **kwargs):
    """
    This is a blocked variant of :func:`numpy.apply_along_axis` implemented via
    :func:`dask.array.map_blocks`

    Notes
    -----
    If either of `dtype` or `shape` are not provided, Dask attempts to
    determine them by calling `func1d` on a dummy array. This may produce
    incorrect values for `dtype` or `shape`, so we recommend providing them.
    """
    arr = asarray(arr)

    # Verify that axis is valid and throw an error otherwise
    axis = len(arr.shape[:axis])

    # If necessary, infer dtype and shape of the output of func1d by calling it on test data.
    if shape is None or dtype is None:
        test_data = np.ones((1,), dtype=arr.dtype)
        test_result = np.array(func1d(test_data, *args, **kwargs))
        if shape is None:
            shape = test_result.shape
        if dtype is None:
            dtype = test_result.dtype

    # Rechunk so that func1d is applied over the full axis.
    arr = arr.rechunk(
        arr.chunks[:axis] + (arr.shape[axis : axis + 1],) + arr.chunks[axis + 1 :]
    )

    # Map func1d over the data to get the result
    # Adds other axes as needed.
    result = arr.map_blocks(
        _inner_apply_along_axis,
        name=funcname(func1d) + "-along-axis",
        dtype=dtype,
        chunks=(arr.chunks[:axis] + shape + arr.chunks[axis + 1 :]),
        drop_axis=axis,
        new_axis=list(range(axis, axis + len(shape), 1)),
        func1d=func1d,
        func1d_axis=axis,
        func1d_args=args,
        func1d_kwargs=kwargs,
    )

    return result


@derived_from(np)
def apply_over_axes(func, a, axes):
    """Apply a function repeatedly over multiple axes."""
    a = asarray(a)
    try:
        axes = tuple(axes)
    except TypeError:
        axes = (axes,)

    sl = a.ndim * (slice(None),)

    # Compute using `apply_along_axis`.
    result = a
    for i in axes:
        result = apply_along_axis(func, i, result, 0)

        # Restore original dimensionality or error.
        if result.ndim == (a.ndim - 1):
            result = result[sl[:i] + (None,)]
        elif result.ndim != a.ndim:
            raise ValueError(
                "func must either preserve dimensionality of the input"
                " or reduce it by one."
            )

    return result

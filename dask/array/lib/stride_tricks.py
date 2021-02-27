import numpy as np
from ..overlap import ensure_minimum_chunksize, map_overlap
from ..numpy_compat import sliding_window_view as _np_sliding_window_view


def sliding_window_view(arr, window_shape, axis):
    """ Dask wrapper for numpy.lib.stride_tricks.sliding_window_view."""

    from numpy.core.numeric import normalize_axis_tuple

    window_shape = tuple(window_shape) if np.iterable(window_shape) else (window_shape,)

    window_shape_array = np.array(window_shape)
    if np.any(window_shape_array < 0):
        raise ValueError("`window_shape` cannot contain negative values")

    if axis is None:
        axis = tuple(range(arr.ndim))
        if len(window_shape) != len(axis):
            raise ValueError(
                f"Since axis is `None`, must provide "
                f"window_shape for all dimensions of `x`; "
                f"got {len(window_shape)} window_shape elements "
                f"and `arr.ndim` is {arr.ndim}."
            )
    else:
        axis = normalize_axis_tuple(axis, arr.ndim, allow_duplicate=True)
        if len(window_shape) != len(axis):
            raise ValueError(
                f"Must provide matching length window_shape and "
                f"axis; got {len(window_shape)} window_shape "
                f"elements and {len(axis)} axes elements."
            )

    depths = [0] * arr.ndim
    for ax, window in zip(axis, window_shape):
        depths[ax] += window - 1

    # Ensure that each chunk is big enough to leave at least a size-1 chunk
    # after windowing (this is only really necessary for the last chunk).
    safe_chunks = tuple(
        ensure_minimum_chunksize(d + 1, c) for d, c in zip(depths, arr.chunks)
    )
    arr = arr.rechunk(safe_chunks)

    # result.shape = arr_shape_trimmed + window_shape,
    # where arr_shape_trimmed is arr.shape with every entry
    # reduced by one less than the corresponding window size.
    # trim chunks to match x_shape_trimmed
    newchunks = tuple(
        c[:-1] + (c[-1] - d,) for d, c in zip(depths, arr.chunks)
    ) + tuple((window,) for window in window_shape)

    return map_overlap(
        _np_sliding_window_view,
        arr,
        depth=tuple((0, d) for d in depths),  # Overlap on +ve side only
        boundary="none",
        meta=arr._meta,
        new_axis=range(arr.ndim, arr.ndim + len(axis)),
        chunks=newchunks,
        trim=False,
        align_arrays=False,
        window_shape=window_shape,
        axis=axis,
    )

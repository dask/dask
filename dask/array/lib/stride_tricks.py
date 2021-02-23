from ..overlap import rechunk_for_overlap, map_overlap, trim_chunks


def _window_view(arr, window_shape, axis, fix_first_block, block_info):
    """ core sliding window function"""
    from ..numpy_compat import sliding_window_view

    block_info = block_info[0]
    slicer = [slice(None, None)] * arr.ndim
    for ax, window, fix_first in zip(axis, window_shape, fix_first_block):
        start, stop = None, None
        is_last_chunk = (
            block_info["num-chunks"][ax] - 1 - block_info["chunk-location"][ax]
        ) == 0

        locs = block_info["array-location"][ax]
        size = locs[1] - locs[0]
        if not is_last_chunk:
            stop = -1 * max(window // 2 - 1, 1)

        # when depth == chunk size of first chunk along some axis
        # we need to make some confusing empirical adjustments.
        # could be avoided if we rechunked so that min
        # chunk size is depth+1 (which would also be more efficient)
        # 1) don't trim the first block along that axis
        if size == window and block_info["chunk-location"][ax] == 0:
            stop = None
        # 2) we need to skip the first "row" of the second block
        if fix_first and block_info["chunk-location"][ax] == 1:
            start = 1
        slicer[ax] = slice(start, stop)

    view = sliding_window_view(arr[tuple(slicer)], window_shape, axis)
    return view


def sliding_window_view(arr, window_shape, axis):
    """ Dask wrapper for numpy.lib.stride_tricks.sliding_window_view."""

    ax_wind = {ax: (window, 0) for ax, window in zip(axis, window_shape)}
    depths = {ax: ax_wind.get(ax, (0, 0)) for ax in range(arr.ndim)}

    # The only way (that I've found) to set chunks in the map_overlap call
    # is to do the rechunking that overlap would do in any case.
    arr = rechunk_for_overlap(arr, depths)

    # result.shape = arr_shape_trimmed + window_shape,
    # where arr_shape_trimmed is arr.shape with every entry
    # reduced by one less than the corresponding window size.
    # trim chunks to match x_shape_trimmed
    trims = [w[0] - 1 if w[0] > 0 else 0 for w in depths.values()]
    newchunks = trim_chunks(arr.chunks, trims) + tuple(
        (window,) for window in window_shape
    )

    return map_overlap(
        _window_view,
        arr,
        depth=depths,
        boundary="none",
        meta=arr._meta,
        new_axis=range(arr.ndim, arr.ndim + len(axis)),
        chunks=newchunks,
        trim=False,
        align_arrays=False,
        window_shape=window_shape,
        axis=axis,
        fix_first_block=tuple(
            arr.chunks[ax][0] == window for ax, window in zip(axis, window_shape)
        ),
    )

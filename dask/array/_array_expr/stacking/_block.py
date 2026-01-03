"""Block operation."""

from __future__ import annotations


def block(arrays, allow_unknown_chunksizes=False):
    """
    Assemble an nd-array from nested lists of blocks.

    Blocks in the innermost lists are concatenated along the last
    dimension (-1), then these are concatenated along the second-last
    dimension (-2), and so on until the outermost list is reached.

    See Also
    --------
    numpy.block
    """
    # Import here to avoid circular imports
    from dask.array._array_expr._collection import asanyarray, concatenate
    from dask.array.numpy_compat import _Recurser

    def atleast_nd(x, ndim):
        x = asanyarray(x)
        diff = max(ndim - x.ndim, 0)
        if diff == 0:
            return x
        else:
            return x[(None,) * diff + (Ellipsis,)]

    def format_index(index):
        return "arrays" + "".join(f"[{i}]" for i in index)

    rec = _Recurser(recurse_if=lambda x: type(x) is list)

    # Ensure that the lists are all matched in depth
    list_ndim = None
    any_empty = False
    for index, value, entering in rec.walk(arrays):
        if type(value) is tuple:
            raise TypeError(
                f"{format_index(index)} is a tuple. "
                "Only lists can be used to arrange blocks, and np.block does "
                "not allow implicit conversion from tuple to ndarray."
            )
        if not entering:
            curr_depth = len(index)
        elif len(value) == 0:
            curr_depth = len(index) + 1
            any_empty = True
        else:
            continue

        if list_ndim is not None and list_ndim != curr_depth:
            raise ValueError(
                f"List depths are mismatched. First element was at depth {list_ndim}, "
                f"but there is an element at depth {curr_depth} ({format_index(index)})"
            )
        list_ndim = curr_depth

    # Do this here so we catch depth mismatches first
    if any_empty:
        raise ValueError("Lists cannot be empty")

    # Convert all the arrays to ndarrays
    arrays = rec.map_reduce(arrays, f_map=asanyarray, f_reduce=list)

    # Determine the maximum dimension of the elements
    elem_ndim = rec.map_reduce(arrays, f_map=lambda xi: xi.ndim, f_reduce=max)
    ndim = max(list_ndim, elem_ndim)

    # First axis to concatenate along
    first_axis = ndim - list_ndim

    # Make all the elements the same dimension
    arrays = rec.map_reduce(
        arrays, f_map=lambda xi: atleast_nd(xi, ndim), f_reduce=list
    )

    # Concatenate innermost lists on the right, outermost on the left
    return rec.map_reduce(
        arrays,
        f_reduce=lambda xs, axis: concatenate(
            list(xs), axis=axis, allow_unknown_chunksizes=allow_unknown_chunksizes
        ),
        f_kwargs=lambda axis: dict(axis=(axis + 1)),
        axis=first_axis,
    )

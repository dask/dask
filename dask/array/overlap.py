from operator import getitem
from itertools import product
from numbers import Integral
from toolz import merge, pipe, concat, partial
from toolz.curried import map

from . import chunk, wrap
from .core import Array, map_blocks, concatenate, concatenate3, reshapelist
from ..highlevelgraph import HighLevelGraph
from ..base import tokenize
from ..core import flatten
from ..utils import concrete


def fractional_slice(task, axes):
    """

    >>> fractional_slice(('x', 5.1), {0: 2})  # doctest: +SKIP
    (getitem, ('x', 6), (slice(0, 2),))

    >>> fractional_slice(('x', 3, 5.1), {0: 2, 1: 3})  # doctest: +SKIP
    (getitem, ('x', 3, 5), (slice(None, None, None), slice(-3, None)))

    >>> fractional_slice(('x', 2.9, 5.1), {0: 2, 1: 3})  # doctest: +SKIP
    (getitem, ('x', 3, 5), (slice(0, 2), slice(-3, None)))
    """
    rounded = (task[0],) + tuple(int(round(i)) for i in task[1:])

    index = []
    for i, (t, r) in enumerate(zip(task[1:], rounded[1:])):
        depth = axes.get(i, 0)
        if isinstance(depth, tuple):
            left_depth = depth[0]
            right_depth = depth[1]
        else:
            left_depth = depth
            right_depth = depth

        if t == r:
            index.append(slice(None, None, None))
        elif t < r and right_depth:
            index.append(slice(0, right_depth))
        elif t > r and left_depth:
            index.append(slice(-left_depth, None))
        else:
            index.append(slice(0, 0))
    index = tuple(index)

    if all(ind == slice(None, None, None) for ind in index):
        return task
    else:
        return (getitem, rounded, index)


def expand_key(k, dims, name=None, axes=None):
    """ Get all neighboring keys around center

    Parameters
    ----------
    k: tuple
        They key around which to generate new keys
    dims: Sequence[int]
        The number of chunks in each dimension
    name: Option[str]
        The name to include in the output keys, or none to include no name
    axes: Dict[int, int]
        The axes active in the expansion.  We don't expand on non-active axes

    Examples
    --------
    >>> expand_key(('x', 2, 3), dims=[5, 5], name='y', axes={0: 1, 1: 1})  # doctest: +NORMALIZE_WHITESPACE
    [[('y', 1.1, 2.1), ('y', 1.1, 3), ('y', 1.1, 3.9)],
     [('y',   2, 2.1), ('y',   2, 3), ('y',   2, 3.9)],
     [('y', 2.9, 2.1), ('y', 2.9, 3), ('y', 2.9, 3.9)]]

    >>> expand_key(('x', 0, 4), dims=[5, 5], name='y', axes={0: 1, 1: 1})  # doctest: +NORMALIZE_WHITESPACE
    [[('y',   0, 3.1), ('y',   0,   4)],
     [('y', 0.9, 3.1), ('y', 0.9,   4)]]
    """

    def inds(i, ind):
        rv = []
        if ind - 0.9 > 0:
            rv.append(ind - 0.9)
        rv.append(ind)
        if ind + 0.9 < dims[i] - 1:
            rv.append(ind + 0.9)
        return rv

    shape = []
    for i, ind in enumerate(k[1:]):
        num = 1
        if ind > 0:
            num += 1
        if ind < dims[i] - 1:
            num += 1
        shape.append(num)

    args = [
        inds(i, ind) if any((axes.get(i, 0),)) else [ind] for i, ind in enumerate(k[1:])
    ]
    if name is not None:
        args = [[name]] + args
    seq = list(product(*args))
    shape2 = [d if any((axes.get(i, 0),)) else 1 for i, d in enumerate(shape)]
    result = reshapelist(shape2, seq)
    return result


def overlap_internal(x, axes):
    """ Share boundaries between neighboring blocks

    Parameters
    ----------

    x: da.Array
        A dask array
    axes: dict
        The size of the shared boundary per axis

    The axes input informs how many cells to overlap between neighboring blocks
    {0: 2, 2: 5} means share two cells in 0 axis, 5 cells in 2 axis
    """
    dims = list(map(len, x.chunks))
    expand_key2 = partial(expand_key, dims=dims, axes=axes)

    # Make keys for each of the surrounding sub-arrays
    interior_keys = pipe(
        x.__dask_keys__(), flatten, map(expand_key2), map(flatten), concat, list
    )

    name = "overlap-" + tokenize(x, axes)
    getitem_name = "getitem-" + tokenize(x, axes)
    interior_slices = {}
    overlap_blocks = {}
    for k in interior_keys:
        frac_slice = fractional_slice((x.name,) + k, axes)
        if (x.name,) + k != frac_slice:
            interior_slices[(getitem_name,) + k] = frac_slice
        else:
            interior_slices[(getitem_name,) + k] = (x.name,) + k
            overlap_blocks[(name,) + k] = (
                concatenate3,
                (concrete, expand_key2((None,) + k, name=getitem_name)),
            )

    chunks = []
    for i, bds in enumerate(x.chunks):
        depth = axes.get(i, 0)
        if isinstance(depth, tuple):
            left_depth = depth[0]
            right_depth = depth[1]
        else:
            left_depth = depth
            right_depth = depth

        if len(bds) == 1:
            chunks.append(bds)
        else:
            left = [bds[0] + right_depth]
            right = [bds[-1] + left_depth]
            mid = []
            for bd in bds[1:-1]:
                mid.append(bd + left_depth + right_depth)
            chunks.append(left + mid + right)

    dsk = merge(interior_slices, overlap_blocks)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[x])

    return Array(graph, name, chunks, meta=x)


def trim_overlap(x, depth, boundary=None):
    """Trim sides from each block.

    This couples well with the ``map_overlap`` operation which may leave
    excess data on each block.

    See also
    --------
    dask.array.overlap.map_overlap

    """

    # parameter to be passed to trim_internal
    axes = coerce_depth(x.ndim, depth)
    boundary2 = coerce_boundary(x.ndim, boundary)
    return trim_internal(x, axes=axes, boundary=boundary2)


def trim_internal(x, axes, boundary=None):
    """ Trim sides from each block

    This couples well with the overlap operation, which may leave excess data on
    each block

    See also
    --------
    dask.array.chunk.trim
    dask.array.map_blocks
    """
    boundary = coerce_boundary(x.ndim, boundary)

    olist = []
    for i, bd in enumerate(x.chunks):
        bdy = boundary.get(i, "none")
        overlap = axes.get(i, 0)
        ilist = []
        for j, d in enumerate(bd):
            if bdy != "none":
                if isinstance(overlap, tuple):
                    d = d - sum(overlap)
                else:
                    d = d - overlap * 2

            else:
                if isinstance(overlap, tuple):
                    d = d - overlap[0] if j != 0 else d
                    d = d - overlap[1] if j != len(bd) - 1 else d
                else:
                    d = d - overlap if j != 0 else d
                    d = d - overlap if j != len(bd) - 1 else d

            ilist.append(d)
        olist.append(tuple(ilist))
    chunks = tuple(olist)

    return map_blocks(
        partial(_trim, axes=axes, boundary=boundary), x, chunks=chunks, dtype=x.dtype
    )


def _trim(x, axes, boundary, block_info):
    """Similar to dask.array.chunk.trim but requires one to specificy the
    boundary condition.

    ``axes``, and ``boundary`` are assumed to have been coerced.

    """
    axes = [axes.get(i, 0) for i in range(x.ndim)]
    axes_front = (ax[0] if isinstance(ax, tuple) else ax for ax in axes)
    axes_back = (
        -ax[1]
        if isinstance(ax, tuple) and ax[1]
        else -ax
        if isinstance(ax, Integral) and ax
        else None
        for ax in axes
    )

    trim_front = (
        0 if (chunk_location == 0 and boundary.get(i, "none") == "none") else ax
        for i, (chunk_location, ax) in enumerate(
            zip(block_info[0]["chunk-location"], axes_front)
        )
    )
    trim_back = (
        None
        if (chunk_location == chunks - 1 and boundary.get(i, "none") == "none")
        else ax
        for i, (chunks, chunk_location, ax) in enumerate(
            zip(block_info[0]["num-chunks"], block_info[0]["chunk-location"], axes_back)
        )
    )
    ind = tuple(slice(front, back) for front, back in zip(trim_front, trim_back))
    return x[ind]


def periodic(x, axis, depth):
    """ Copy a slice of an array around to its other side

    Useful to create periodic boundary conditions for overlap
    """

    left = (
        (slice(None, None, None),) * axis
        + (slice(0, depth),)
        + (slice(None, None, None),) * (x.ndim - axis - 1)
    )
    right = (
        (slice(None, None, None),) * axis
        + (slice(-depth, None),)
        + (slice(None, None, None),) * (x.ndim - axis - 1)
    )
    l = x[left]
    r = x[right]

    l, r = _remove_overlap_boundaries(l, r, axis, depth)

    return concatenate([r, x, l], axis=axis)


def reflect(x, axis, depth):
    """ Reflect boundaries of array on the same side

    This is the converse of ``periodic``
    """
    if depth == 1:
        left = (
            (slice(None, None, None),) * axis
            + (slice(0, 1),)
            + (slice(None, None, None),) * (x.ndim - axis - 1)
        )
    else:
        left = (
            (slice(None, None, None),) * axis
            + (slice(depth - 1, None, -1),)
            + (slice(None, None, None),) * (x.ndim - axis - 1)
        )
    right = (
        (slice(None, None, None),) * axis
        + (slice(-1, -depth - 1, -1),)
        + (slice(None, None, None),) * (x.ndim - axis - 1)
    )
    l = x[left]
    r = x[right]

    l, r = _remove_overlap_boundaries(l, r, axis, depth)

    return concatenate([l, x, r], axis=axis)


def nearest(x, axis, depth):
    """ Each reflect each boundary value outwards

    This mimics what the skimage.filters.gaussian_filter(... mode="nearest")
    does.
    """
    left = (
        (slice(None, None, None),) * axis
        + (slice(0, 1),)
        + (slice(None, None, None),) * (x.ndim - axis - 1)
    )
    right = (
        (slice(None, None, None),) * axis
        + (slice(-1, -2, -1),)
        + (slice(None, None, None),) * (x.ndim - axis - 1)
    )

    l = concatenate([x[left]] * depth, axis=axis)
    r = concatenate([x[right]] * depth, axis=axis)

    l, r = _remove_overlap_boundaries(l, r, axis, depth)

    return concatenate([l, x, r], axis=axis)


def constant(x, axis, depth, value):
    """ Add constant slice to either side of array """
    chunks = list(x.chunks)
    chunks[axis] = (depth,)

    try:
        c = wrap.full_like(
            getattr(x, "_meta", x),
            value,
            shape=tuple(map(sum, chunks)),
            chunks=tuple(chunks),
            dtype=x.dtype,
        )
    except TypeError:
        c = wrap.full(
            tuple(map(sum, chunks)), value, chunks=tuple(chunks), dtype=x.dtype
        )

    return concatenate([c, x, c], axis=axis)


def _remove_overlap_boundaries(l, r, axis, depth):
    lchunks = list(l.chunks)
    lchunks[axis] = (depth,)
    rchunks = list(r.chunks)
    rchunks[axis] = (depth,)

    l = l.rechunk(tuple(lchunks))
    r = r.rechunk(tuple(rchunks))
    return l, r


def boundaries(x, depth=None, kind=None):
    """ Add boundary conditions to an array before overlaping

    See Also
    --------
    periodic
    constant
    """
    if not isinstance(kind, dict):
        kind = dict((i, kind) for i in range(x.ndim))
    if not isinstance(depth, dict):
        depth = dict((i, depth) for i in range(x.ndim))

    for i in range(x.ndim):
        d = depth.get(i, 0)
        if d == 0:
            continue

        this_kind = kind.get(i, "none")
        if this_kind == "none":
            continue
        elif this_kind == "periodic":
            x = periodic(x, i, d)
        elif this_kind == "reflect":
            x = reflect(x, i, d)
        elif this_kind == "nearest":
            x = nearest(x, i, d)
        elif i in kind:
            x = constant(x, i, d, kind[i])

    return x


def overlap(x, depth, boundary):
    """ Share boundaries between neighboring blocks

    Parameters
    ----------

    x: da.Array
        A dask array
    depth: dict
        The size of the shared boundary per axis
    boundary: dict
        The boundary condition on each axis. Options are 'reflect', 'periodic',
        'nearest', 'none', or an array value.  Such a value will fill the
        boundary with that value.

    The depth input informs how many cells to overlap between neighboring
    blocks ``{0: 2, 2: 5}`` means share two cells in 0 axis, 5 cells in 2 axis.
    Axes missing from this input will not be overlapped.

    Examples
    --------
    >>> import numpy as np
    >>> import dask.array as da

    >>> x = np.arange(64).reshape((8, 8))
    >>> d = da.from_array(x, chunks=(4, 4))
    >>> d.chunks
    ((4, 4), (4, 4))

    >>> g = da.overlap.overlap(d, depth={0: 2, 1: 1},
    ...                       boundary={0: 100, 1: 'reflect'})
    >>> g.chunks
    ((8, 8), (6, 6))

    >>> np.array(g)
    array([[100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
           [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
           [  0,   0,   1,   2,   3,   4,   3,   4,   5,   6,   7,   7],
           [  8,   8,   9,  10,  11,  12,  11,  12,  13,  14,  15,  15],
           [ 16,  16,  17,  18,  19,  20,  19,  20,  21,  22,  23,  23],
           [ 24,  24,  25,  26,  27,  28,  27,  28,  29,  30,  31,  31],
           [ 32,  32,  33,  34,  35,  36,  35,  36,  37,  38,  39,  39],
           [ 40,  40,  41,  42,  43,  44,  43,  44,  45,  46,  47,  47],
           [ 16,  16,  17,  18,  19,  20,  19,  20,  21,  22,  23,  23],
           [ 24,  24,  25,  26,  27,  28,  27,  28,  29,  30,  31,  31],
           [ 32,  32,  33,  34,  35,  36,  35,  36,  37,  38,  39,  39],
           [ 40,  40,  41,  42,  43,  44,  43,  44,  45,  46,  47,  47],
           [ 48,  48,  49,  50,  51,  52,  51,  52,  53,  54,  55,  55],
           [ 56,  56,  57,  58,  59,  60,  59,  60,  61,  62,  63,  63],
           [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
           [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100]])
    """
    depth2 = coerce_depth(x.ndim, depth)
    boundary2 = coerce_boundary(x.ndim, boundary)

    # is depth larger than chunk size?
    depth_values = [depth2.get(i, 0) for i in range(x.ndim)]
    for d, c in zip(depth_values, x.chunks):
        maxd = max(d) if isinstance(d, tuple) else d
        if maxd > min(c):
            raise ValueError(
                "The overlapping depth %d is larger than your\n"
                "smallest chunk size %d. Rechunk your array\n"
                "with a larger chunk size or a chunk size that\n"
                "more evenly divides the shape of your array." % (d, min(c))
            )
    x2 = boundaries(x, depth2, boundary2)
    x3 = overlap_internal(x2, depth2)
    trim = dict(
        (k, v * 2 if boundary2.get(k, "none") != "none" else 0)
        for k, v in depth2.items()
    )
    x4 = chunk.trim(x3, trim)
    return x4


def add_dummy_padding(x, depth, boundary):
    """
    Pads an array which has 'none' as the boundary type.
    Used to simplify trimming arrays which use 'none'.

    >>> import dask.array as da
    >>> x = da.arange(6, chunks=3)
    >>> add_dummy_padding(x, {0: 1}, {0: 'none'}).compute()  # doctest: +NORMALIZE_WHITESPACE
    array([..., 0, 1, 2, 3, 4, 5, ...])
    """
    for k, v in boundary.items():
        d = depth.get(k, 0)
        if v == "none" and d > 0:
            empty_shape = list(x.shape)
            empty_shape[k] = d

            empty_chunks = list(x.chunks)
            empty_chunks[k] = (d,)

            try:
                empty = wrap.empty_like(
                    getattr(x, "_meta", x),
                    shape=empty_shape,
                    chunks=empty_chunks,
                    dtype=x.dtype,
                )
            except TypeError:
                empty = wrap.empty(empty_shape, chunks=empty_chunks, dtype=x.dtype)

            out_chunks = list(x.chunks)
            ax_chunks = list(out_chunks[k])
            ax_chunks[0] += d
            ax_chunks[-1] += d
            out_chunks[k] = tuple(ax_chunks)

            x = concatenate([empty, x, empty], axis=k)
            x = x.rechunk(out_chunks)
    return x


def map_overlap(x, func, depth, boundary=None, trim=True, **kwargs):
    """ Map a function over blocks of the array with some overlap

    We share neighboring zones between blocks of the array, then map a
    function, then trim away the neighboring strips.

    Parameters
    ----------
    func: function
        The function to apply to each extended block
    depth: int, tuple, or dict
        The number of elements that each block should share with its neighbors
        If a tuple or dict then this can be different per axis.
        Asymmetric depths may be specified using a dict value of (-/+) tuples.
        Note that asymmetric depths are currently only supported when
        ``boundary`` is 'none'.
    boundary: str, tuple, dict
        How to handle the boundaries.
        Values include 'reflect', 'periodic', 'nearest', 'none',
        or any constant value like 0 or np.nan
    trim: bool
        Whether or not to trim ``depth`` elements from each block after
        calling the map function.
        Set this to False if your mapping function already does this for you
    **kwargs:
        Other keyword arguments valid in ``map_blocks``

    Examples
    --------
    >>> import numpy as np
    >>> import dask.array as da

    >>> x = np.array([1, 1, 2, 3, 3, 3, 2, 1, 1])
    >>> x = da.from_array(x, chunks=5)
    >>> def derivative(x):
    ...     return x - np.roll(x, 1)

    >>> y = x.map_overlap(derivative, depth=1, boundary=0)
    >>> y.compute()
    array([ 1,  0,  1,  1,  0,  0, -1, -1,  0])

    >>> x = np.arange(16).reshape((4, 4))
    >>> d = da.from_array(x, chunks=(2, 2))
    >>> d.map_overlap(lambda x: x + x.size, depth=1).compute()
    array([[16, 17, 18, 19],
           [20, 21, 22, 23],
           [24, 25, 26, 27],
           [28, 29, 30, 31]])

    >>> func = lambda x: x + x.size
    >>> depth = {0: 1, 1: 1}
    >>> boundary = {0: 'reflect', 1: 'none'}
    >>> d.map_overlap(func, depth, boundary).compute()  # doctest: +NORMALIZE_WHITESPACE
    array([[12,  13,  14,  15],
           [16,  17,  18,  19],
           [20,  21,  22,  23],
           [24,  25,  26,  27]])
    """
    depth2 = coerce_depth(x.ndim, depth)
    boundary2 = coerce_boundary(x.ndim, boundary)

    for i in range(x.ndim):
        if isinstance(depth2[i], tuple) and boundary2[i] != "none":
            raise NotImplementedError(
                "Asymmetric overlap is currently only implemented "
                "for boundary='none', however boundary for dimension "
                "{} is {}".format(i, boundary2[i])
            )

    assert all(type(c) is int for cc in x.chunks for c in cc)
    g = overlap(x, depth=depth2, boundary=boundary2)
    assert all(type(c) is int for cc in g.chunks for c in cc)
    g2 = g.map_blocks(func, **kwargs)
    assert all(type(c) is int for cc in g2.chunks for c in cc)
    if trim:
        return trim_internal(g2, depth2, boundary2)
    else:
        return g2


def coerce_depth(ndim, depth):
    if isinstance(depth, Integral):
        depth = (depth,) * ndim
    if isinstance(depth, tuple):
        depth = dict(zip(range(ndim), depth))
    if isinstance(depth, dict):
        for i in range(ndim):
            if i not in depth:
                depth.update({i: 0})
    return depth


def coerce_boundary(ndim, boundary):
    default = "reflect"
    if boundary is None:
        boundary = default
    if not isinstance(boundary, (tuple, dict)):
        boundary = (boundary,) * ndim
    if isinstance(boundary, tuple):
        boundary = dict(zip(range(ndim), boundary))
    if isinstance(boundary, dict):
        for i in range(ndim):
            if i not in boundary:
                boundary.update({i: default})
    return boundary

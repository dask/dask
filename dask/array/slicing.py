import bisect
import functools
import math
import warnings
from itertools import product
from numbers import Integral, Number
from operator import add, getitem, itemgetter

import numpy as np
from tlz import accumulate, concat, memoize, merge, pluck

from .. import config, core, utils
from ..base import is_dask_collection, tokenize
from ..highlevelgraph import HighLevelGraph
from ..utils import is_arraylike

colon = slice(None, None, None)


def _sanitize_index_element(ind):
    """Sanitize a one-element index."""
    if isinstance(ind, Number):
        ind2 = int(ind)
        if ind2 != ind:
            raise IndexError("Bad index.  Must be integer-like: %s" % ind)
        else:
            return ind2
    elif ind is None:
        return None
    else:
        raise TypeError("Invalid index type", type(ind), ind)


def sanitize_index(ind):
    """Sanitize the elements for indexing along one axis

    >>> sanitize_index([2, 3, 5])
    array([2, 3, 5])
    >>> sanitize_index([True, False, True, False])
    array([0, 2])
    >>> sanitize_index(np.array([1, 2, 3]))
    array([1, 2, 3])
    >>> sanitize_index(np.array([False, True, True]))
    array([1, 2])
    >>> type(sanitize_index(np.int32(0)))
    <class 'int'>
    >>> sanitize_index(1.0)
    1
    >>> sanitize_index(0.5)
    Traceback (most recent call last):
    ...
    IndexError: Bad index.  Must be integer-like: 0.5
    """
    from .utils import asanyarray_safe

    if ind is None:
        return None
    elif isinstance(ind, slice):
        return slice(
            _sanitize_index_element(ind.start),
            _sanitize_index_element(ind.stop),
            _sanitize_index_element(ind.step),
        )
    elif isinstance(ind, Number):
        return _sanitize_index_element(ind)
    elif is_dask_collection(ind):
        return ind
    index_array = asanyarray_safe(ind, like=ind)
    if index_array.dtype == bool:
        nonzero = np.nonzero(index_array)
        if len(nonzero) == 1:
            # If a 1-element tuple, unwrap the element
            nonzero = nonzero[0]
        if is_arraylike(nonzero):
            return nonzero
        else:
            return np.asanyarray(nonzero)
    elif np.issubdtype(index_array.dtype, np.integer):
        return index_array
    elif np.issubdtype(index_array.dtype, np.floating):
        int_index = index_array.astype(np.intp)
        if np.allclose(index_array, int_index):
            return int_index
        else:
            check_int = np.isclose(index_array, int_index)
            first_err = index_array.ravel()[np.flatnonzero(~check_int)[0]]
            raise IndexError("Bad index.  Must be integer-like: %s" % first_err)
    else:
        raise TypeError("Invalid index type", type(ind), ind)


def slice_array(out_name, in_name, blockdims, index, itemsize):
    """
    Main function for array slicing

    This function makes a new dask that slices blocks along every
    dimension and aggregates (via cartesian product) each dimension's
    slices so that the resulting block slices give the same results
    as the original slice on the original structure

    Index must be a tuple.  It may contain the following types

        int, slice, list (at most one list), None

    Parameters
    ----------
    in_name - string
      This is the dask variable name that will be used as input
    out_name - string
      This is the dask variable output name
    blockshape - iterable of integers
    index - iterable of integers, slices, lists, or None
    itemsize : int
        The number of bytes required for each element of the array.

    Returns
    -------
    Dict where the keys are tuples of

        (out_name, dim_index[, dim_index[, ...]])

    and the values are

        (function, (in_name, dim_index, dim_index, ...),
                   (slice(...), [slice()[,...]])

    Also new blockdims with shapes of each block

        ((10, 10, 10, 10), (20, 20))

    Examples
    --------
    >>> dsk, blockdims = slice_array('y', 'x', [(20, 20, 20, 20, 20)],
    ...                              (slice(10, 35),))  #  doctest: +SKIP
    >>> dsk  # doctest: +SKIP
    {('y', 0): (getitem, ('x', 0), (slice(10, 20),)),
     ('y', 1): (getitem, ('x', 1), (slice(0, 15),))}
    >>> blockdims  # doctest: +SKIP
    ((10, 15),)

    See Also
    --------
    This function works by successively unwrapping cases and passing down
    through a sequence of functions.

    slice_with_newaxis : handle None/newaxis case
    slice_wrap_lists : handle fancy indexing with lists
    slice_slices_and_integers : handle everything else
    """
    blockdims = tuple(map(tuple, blockdims))

    # x[:, :, :] - Punt and return old value
    if all(
        isinstance(index, slice) and index == slice(None, None, None) for index in index
    ):
        suffixes = product(*[range(len(bd)) for bd in blockdims])
        dsk = dict(((out_name,) + s, (in_name,) + s) for s in suffixes)
        return dsk, blockdims

    # Add in missing colons at the end as needed.  x[5] -> x[5, :, :]
    not_none_count = sum(i is not None for i in index)
    missing = len(blockdims) - not_none_count
    index += (slice(None, None, None),) * missing

    # Pass down to next function
    dsk_out, bd_out = slice_with_newaxes(out_name, in_name, blockdims, index, itemsize)

    bd_out = tuple(map(tuple, bd_out))
    return dsk_out, bd_out


def slice_with_newaxes(out_name, in_name, blockdims, index, itemsize):
    """
    Handle indexing with Nones

    Strips out Nones then hands off to slice_wrap_lists
    """
    # Strip Nones from index
    index2 = tuple([ind for ind in index if ind is not None])
    where_none = [i for i, ind in enumerate(index) if ind is None]
    where_none_orig = list(where_none)
    for i, x in enumerate(where_none):
        n = sum(isinstance(ind, Integral) for ind in index[:x])
        if n:
            where_none[i] -= n

    # Pass down and do work
    dsk, blockdims2 = slice_wrap_lists(out_name, in_name, blockdims, index2, itemsize)

    if where_none:
        expand = expander(where_none)
        expand_orig = expander(where_none_orig)

        # Insert ",0" into the key:  ('x', 2, 3) -> ('x', 0, 2, 0, 3)
        dsk2 = {
            (out_name,) + expand(k[1:], 0): (v[:2] + (expand_orig(v[2], None),))
            for k, v in dsk.items()
            if k[0] == out_name
        }

        # Add back intermediate parts of the dask that weren't the output
        dsk3 = merge(dsk2, {k: v for k, v in dsk.items() if k[0] != out_name})

        # Insert (1,) into blockdims:  ((2, 2), (3, 3)) -> ((2, 2), (1,), (3, 3))
        blockdims3 = expand(blockdims2, (1,))

        return dsk3, blockdims3

    else:
        return dsk, blockdims2


def slice_wrap_lists(out_name, in_name, blockdims, index, itemsize):
    """
    Fancy indexing along blocked array dasks

    Handles index of type list.  Calls slice_slices_and_integers for the rest

    See Also
    --------

    take : handle slicing with lists ("fancy" indexing)
    slice_slices_and_integers : handle slicing with slices and integers
    """
    assert all(isinstance(i, (slice, list, Integral)) or is_arraylike(i) for i in index)
    if not len(blockdims) == len(index):
        raise IndexError("Too many indices for array")

    # Do we have more than one list in the index?
    where_list = [
        i for i, ind in enumerate(index) if is_arraylike(ind) and ind.ndim > 0
    ]
    if len(where_list) > 1:
        raise NotImplementedError("Don't yet support nd fancy indexing")
    # Is the single list an empty list? In this case just treat it as a zero
    # length slice
    if where_list and not index[where_list[0]].size:
        index = list(index)
        index[where_list.pop()] = slice(0, 0, 1)
        index = tuple(index)

    # No lists, hooray! just use slice_slices_and_integers
    if not where_list:
        return slice_slices_and_integers(out_name, in_name, blockdims, index)

    # Replace all lists with full slices  [3, 1, 0] -> slice(None, None, None)
    index_without_list = tuple(
        slice(None, None, None) if is_arraylike(i) else i for i in index
    )

    # lists and full slices.  Just use take
    if all(is_arraylike(i) or i == slice(None, None, None) for i in index):
        axis = where_list[0]
        blockdims2, dsk3 = take(
            out_name, in_name, blockdims, index[where_list[0]], itemsize, axis=axis
        )
    # Mixed case. Both slices/integers and lists. slice/integer then take
    else:
        # Do first pass without lists
        tmp = "slice-" + tokenize((out_name, in_name, blockdims, index))
        dsk, blockdims2 = slice_slices_and_integers(
            tmp, in_name, blockdims, index_without_list
        )

        # After collapsing some axes due to int indices, adjust axis parameter
        axis = where_list[0]
        axis2 = axis - sum(
            1 for i, ind in enumerate(index) if i < axis and isinstance(ind, Integral)
        )

        # Do work
        blockdims2, dsk2 = take(out_name, tmp, blockdims2, index[axis], 8, axis=axis2)
        dsk3 = merge(dsk, dsk2)

    return dsk3, blockdims2


def slice_slices_and_integers(out_name, in_name, blockdims, index):
    """
    Dask array indexing with slices and integers

    See Also
    --------

    _slice_1d
    """
    from .core import unknown_chunk_message

    shape = tuple(cached_cumsum(dim, initial_zero=True)[-1] for dim in blockdims)

    for dim, ind in zip(shape, index):
        if np.isnan(dim) and ind != slice(None, None, None):
            raise ValueError(
                "Arrays chunk sizes are unknown: %s%s" % (shape, unknown_chunk_message)
            )

    assert all(isinstance(ind, (slice, Integral)) for ind in index)
    assert len(index) == len(blockdims)

    # Get a list (for each dimension) of dicts{blocknum: slice()}
    block_slices = list(map(_slice_1d, shape, blockdims, index))
    sorted_block_slices = [sorted(i.items()) for i in block_slices]

    # (in_name, 1, 1, 2), (in_name, 1, 1, 4), (in_name, 2, 1, 2), ...
    in_names = list(product([in_name], *[pluck(0, s) for s in sorted_block_slices]))

    # (out_name, 0, 0, 0), (out_name, 0, 0, 1), (out_name, 0, 1, 0), ...
    out_names = list(
        product(
            [out_name],
            *[
                range(len(d))[::-1] if i.step and i.step < 0 else range(len(d))
                for d, i in zip(block_slices, index)
                if not isinstance(i, Integral)
            ],
        )
    )

    all_slices = list(product(*[pluck(1, s) for s in sorted_block_slices]))

    dsk_out = {
        out_name: (getitem, in_name, slices)
        for out_name, in_name, slices in zip(out_names, in_names, all_slices)
    }

    new_blockdims = [
        new_blockdim(d, db, i)
        for d, i, db in zip(shape, index, blockdims)
        if not isinstance(i, Integral)
    ]

    return dsk_out, new_blockdims


def _slice_1d(dim_shape, lengths, index):
    """Returns a dict of {blocknum: slice}

    This function figures out where each slice should start in each
    block for a single dimension. If the slice won't return any elements
    in the block, that block will not be in the output.

    Parameters
    ----------

    dim_shape - the number of elements in this dimension.
      This should be a positive, non-zero integer
    blocksize - the number of elements per block in this dimension
      This should be a positive, non-zero integer
    index - a description of the elements in this dimension that we want
      This might be an integer, a slice(), or an Ellipsis

    Returns
    -------

    dictionary where the keys are the integer index of the blocks that
      should be sliced and the values are the slices

    Examples
    --------

    Trivial slicing

    >>> _slice_1d(100, [60, 40], slice(None, None, None))
    {0: slice(None, None, None), 1: slice(None, None, None)}

    100 length array cut into length 20 pieces, slice 0:35

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(0, 35))
    {0: slice(None, None, None), 1: slice(0, 15, 1)}

    Support irregular blocks and various slices

    >>> _slice_1d(100, [20, 10, 10, 10, 25, 25], slice(10, 35))
    {0: slice(10, 20, 1), 1: slice(None, None, None), 2: slice(0, 5, 1)}

    Support step sizes

    >>> _slice_1d(100, [15, 14, 13], slice(10, 41, 3))
    {0: slice(10, 15, 3), 1: slice(1, 14, 3), 2: slice(2, 12, 3)}

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(0, 100, 40))  # step > blocksize
    {0: slice(0, 20, 40), 2: slice(0, 20, 40), 4: slice(0, 20, 40)}

    Also support indexing single elements

    >>> _slice_1d(100, [20, 20, 20, 20, 20], 25)
    {1: 5}

    And negative slicing

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(100, 0, -3)) # doctest: +NORMALIZE_WHITESPACE
    {4: slice(-1, -21, -3),
     3: slice(-2, -21, -3),
     2: slice(-3, -21, -3),
     1: slice(-1, -21, -3),
     0: slice(-2, -20, -3)}

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(100, 12, -3)) # doctest: +NORMALIZE_WHITESPACE
    {4: slice(-1, -21, -3),
     3: slice(-2, -21, -3),
     2: slice(-3, -21, -3),
     1: slice(-1, -21, -3),
     0: slice(-2, -8, -3)}

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(100, -12, -3))
    {4: slice(-1, -12, -3)}
    """
    chunk_boundaries = cached_cumsum(lengths)

    if isinstance(index, Integral):
        # use right-side search to be consistent with previous result
        i = bisect.bisect_right(chunk_boundaries, index)
        if i > 0:
            # the very first chunk has no relative shift
            ind = index - chunk_boundaries[i - 1]
        else:
            ind = index
        return {int(i): int(ind)}

    assert isinstance(index, slice)

    if index == colon:
        return {k: colon for k in range(len(lengths))}

    step = index.step or 1
    if step > 0:
        start = index.start or 0
        stop = index.stop if index.stop is not None else dim_shape
    else:
        start = index.start if index.start is not None else dim_shape - 1
        start = dim_shape - 1 if start >= dim_shape else start
        stop = -(dim_shape + 1) if index.stop is None else index.stop

    # posify start and stop
    if start < 0:
        start += dim_shape
    if stop < 0:
        stop += dim_shape

    d = dict()
    if step > 0:
        istart = bisect.bisect_right(chunk_boundaries, start)
        istop = bisect.bisect_left(chunk_boundaries, stop)

        # the bound is not exactly tight; make it tighter?
        istop = min(istop + 1, len(lengths))

        # jump directly to istart
        if istart > 0:
            start = start - chunk_boundaries[istart - 1]
            stop = stop - chunk_boundaries[istart - 1]

        for i in range(istart, istop):
            length = lengths[i]
            if start < length and stop > 0:
                d[i] = slice(start, min(stop, length), step)
                start = (start - length) % step
            else:
                start = start - length
            stop -= length
    else:
        rstart = start  # running start

        istart = bisect.bisect_left(chunk_boundaries, start)
        istop = bisect.bisect_right(chunk_boundaries, stop)

        # the bound is not exactly tight; make it tighter?
        istart = min(istart + 1, len(chunk_boundaries) - 1)
        istop = max(istop - 1, -1)

        for i in range(istart, istop, -1):
            chunk_stop = chunk_boundaries[i]
            # create a chunk start and stop
            if i == 0:
                chunk_start = 0
            else:
                chunk_start = chunk_boundaries[i - 1]

            # if our slice is in this chunk
            if (chunk_start <= rstart < chunk_stop) and (rstart > stop):
                d[i] = slice(
                    rstart - chunk_stop,
                    max(chunk_start - chunk_stop - 1, stop - chunk_stop),
                    step,
                )

                # compute the next running start point,
                offset = (rstart - (chunk_start - 1)) % step
                rstart = chunk_start + offset - 1

    # replace 0:20:1 with : if appropriate
    for k, v in d.items():
        if v == slice(0, lengths[k], 1):
            d[k] = slice(None, None, None)

    if not d:  # special case x[:0]
        d[0] = slice(0, 0, 1)

    return d


def partition_by_size(sizes, seq):
    """

    >>> partition_by_size([10, 20, 10], [1, 5, 9, 12, 29, 35])
    [array([1, 5, 9]), array([ 2, 19]), array([5])]
    """
    if not is_arraylike(seq):
        seq = np.asanyarray(seq)
    left = np.empty(len(sizes) + 1, dtype=int)
    left[0] = 0

    right = np.cumsum(sizes, out=left[1:])
    locations = np.empty(len(sizes) + 1, dtype=int)
    locations[0] = 0
    locations[1:] = np.searchsorted(seq, right)
    return [(seq[j:k] - l) for j, k, l in zip(locations[:-1], locations[1:], left)]


def issorted(seq):
    """Is sequence sorted?

    >>> issorted([1, 2, 3])
    True
    >>> issorted([3, 1, 2])
    False
    """
    if len(seq) == 0:
        return True
    return np.all(seq[:-1] <= seq[1:])


def slicing_plan(chunks, index):
    """Construct a plan to slice chunks with the given index

    Parameters
    ----------
    chunks : Tuple[int]
        One dimensions worth of chunking information
    index : np.ndarray[int]
        The index passed to slice on that dimension

    Returns
    -------
    out : List[Tuple[int, np.ndarray]]
        A list of chunk/sub-index pairs corresponding to each output chunk
    """
    from .utils import asarray_safe

    if not is_arraylike(index):
        index = np.asanyarray(index)
    cum_chunks = cached_cumsum(chunks)

    cum_chunks = asarray_safe(cum_chunks, like=index)
    # this dispactches to the array library
    chunk_locations = np.searchsorted(cum_chunks, index, side="right")

    # but we need chunk_locations as python ints for getitem calls downstream
    chunk_locations = chunk_locations.tolist()
    where = np.where(np.diff(chunk_locations))[0] + 1

    extra = asarray_safe([0], like=where)
    c_loc = asarray_safe([len(chunk_locations)], like=where)
    where = np.concatenate([extra, where, c_loc])

    out = []
    for i in range(len(where) - 1):
        sub_index = index[where[i] : where[i + 1]]
        chunk = chunk_locations[where[i]]
        if chunk > 0:
            sub_index = sub_index - cum_chunks[chunk - 1]
        out.append((chunk, sub_index))

    return out


def take(outname, inname, chunks, index, itemsize, axis=0):
    """Index array with an iterable of index

    Handles a single index by a single list

    Mimics ``np.take``

    >>> chunks, dsk = take('y', 'x', [(20, 20, 20, 20)], [5, 1, 47, 3], 8, axis=0)
    >>> chunks
    ((2, 1, 1),)
    >>> dsk  # doctest: +SKIP
    {('y', 0): (getitem, (np.concatenate, [(getitem, ('x', 0), ([1, 3, 5],)),
                                           (getitem, ('x', 2), ([7],))],
                                          0),
                         (2, 0, 4, 1))}

    When list is sorted we retain original block structure

    >>> chunks, dsk = take('y', 'x', [(20, 20, 20, 20)], [1, 3, 5, 47], 8, axis=0)
    >>> chunks
    ((3, 1),)
    >>> dsk  # doctest: +SKIP
    {('y', 0): (getitem, ('x', 0), ([1, 3, 5],)),
     ('y', 2): (getitem, ('x', 2), ([7],))}

    When any indexed blocks would otherwise grow larger than
    dask.config.array.chunk-size, we might split them,
    depending on the value of ``dask.config.slicing.split-large-chunks``.

    >>> import dask
    >>> with dask.config.set({"array.slicing.split-large-chunks": True}):
    ...      chunks, dsk = take('y', 'x', [(1, 1, 1), (1000, 1000), (1000, 1000)],
    ...                        [0] + [1] * 6 + [2], axis=0, itemsize=8)
    >>> chunks
    ((1, 3, 3, 1), (1000, 1000), (1000, 1000))
    """
    from .core import PerformanceWarning

    plan = slicing_plan(chunks[axis], index)
    if len(plan) >= len(chunks[axis]) * 10:
        factor = math.ceil(len(plan) / len(chunks[axis]))

        warnings.warn(
            "Slicing with an out-of-order index is generating %d "
            "times more chunks" % factor,
            PerformanceWarning,
            stacklevel=6,
        )
    if not is_arraylike(index):
        index = np.asarray(index)

    # Check for chunks from the plan that would violate the user's
    # configured chunk size.
    nbytes = utils.parse_bytes(config.get("array.chunk-size"))
    other_chunks = [chunks[i] for i in range(len(chunks)) if i != axis]
    other_numel = np.prod([sum(x) for x in other_chunks])

    if math.isnan(other_numel):
        warnsize = maxsize = math.inf
    else:
        maxsize = math.ceil(nbytes / (other_numel * itemsize))
        warnsize = maxsize * 5

    split = config.get("array.slicing.split-large-chunks", None)

    # Warn only when the default is not specified.
    warned = split is not None

    for _, index_list in plan:
        if not warned and len(index_list) > warnsize:
            msg = (
                "Slicing is producing a large chunk. To accept the large\n"
                "chunk and silence this warning, set the option\n"
                "    >>> with dask.config.set(**{'array.slicing.split_large_chunks': False}):\n"
                "    ...     array[indexer]\n\n"
                "To avoid creating the large chunks, set the option\n"
                "    >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):\n"
                "    ...     array[indexer]"
            )
            warnings.warn(msg, PerformanceWarning, stacklevel=6)
            warned = True

    where_index = []
    index_lists = []
    for where_idx, index_list in plan:
        index_length = len(index_list)
        if split and index_length > maxsize:
            index_sublist = np.array_split(
                index_list, math.ceil(index_length / maxsize)
            )
            index_lists.extend(index_sublist)
            where_index.extend([where_idx] * len(index_sublist))
        else:
            if not is_arraylike(index_list):
                index_list = np.array(index_list)
            index_lists.append(index_list)
            where_index.append(where_idx)

    dims = [range(len(bd)) for bd in chunks]

    indims = list(dims)
    indims[axis] = list(range(len(where_index)))
    keys = list(product([outname], *indims))

    outdims = list(dims)
    outdims[axis] = where_index
    slices = [[colon] * len(bd) for bd in chunks]
    slices[axis] = index_lists
    slices = list(product(*slices))
    inkeys = list(product([inname], *outdims))
    values = [(getitem, inkey, slc) for inkey, slc in zip(inkeys, slices)]

    chunks2 = list(chunks)
    chunks2[axis] = tuple(map(len, index_lists))
    dsk = dict(zip(keys, values))

    return tuple(chunks2), dsk


def posify_index(shape, ind):
    """Flip negative indices around to positive ones

    >>> posify_index(10, 3)
    3
    >>> posify_index(10, -3)
    7
    >>> posify_index(10, [3, -3])
    array([3, 7])

    >>> posify_index((10, 20), (3, -3))
    (3, 17)
    >>> posify_index((10, 20), (3, [3, 4, -3]))  # doctest: +NORMALIZE_WHITESPACE
    (3, array([ 3,  4, 17]))
    """
    if isinstance(ind, tuple):
        return tuple(map(posify_index, shape, ind))
    if isinstance(ind, Integral):
        if ind < 0 and not math.isnan(shape):
            return ind + shape
        else:
            return ind
    if isinstance(ind, (np.ndarray, list)) and not math.isnan(shape):
        ind = np.asanyarray(ind)
        return np.where(ind < 0, ind + shape, ind)
    return ind


@memoize
def _expander(where):
    if not where:

        def expand(seq, val):
            return seq

        return expand
    else:
        decl = """def expand(seq, val):
            return ({left}) + tuple({right})
        """
        left = []
        j = 0
        for i in range(max(where) + 1):
            if i in where:
                left.append("val, ")
            else:
                left.append("seq[%d], " % j)
                j += 1
        right = "seq[%d:]" % j
        left = "".join(left)
        decl = decl.format(**locals())
        ns = {}
        exec(compile(decl, "<dynamic>", "exec"), ns, ns)
        return ns["expand"]


def expander(where):
    """Create a function to insert value at many locations in sequence.

    >>> expander([0, 2])(['a', 'b', 'c'], 'z')
    ('z', 'a', 'z', 'b', 'c')
    """
    return _expander(tuple(where))


def new_blockdim(dim_shape, lengths, index):
    """

    >>> new_blockdim(100, [20, 10, 20, 10, 40], slice(0, 90, 2))
    [10, 5, 10, 5, 15]

    >>> new_blockdim(100, [20, 10, 20, 10, 40], [5, 1, 30, 22])
    [4]

    >>> new_blockdim(100, [20, 10, 20, 10, 40], slice(90, 10, -2))
    [16, 5, 10, 5, 4]
    """
    if index == slice(None, None, None):
        return lengths
    if isinstance(index, list):
        return [len(index)]
    assert not isinstance(index, Integral)
    pairs = sorted(_slice_1d(dim_shape, lengths, index).items(), key=itemgetter(0))
    slices = [
        slice(0, lengths[i], 1) if slc == slice(None, None, None) else slc
        for i, slc in pairs
    ]
    if isinstance(index, slice) and index.step and index.step < 0:
        slices = slices[::-1]
    return [int(math.ceil((1.0 * slc.stop - slc.start) / slc.step)) for slc in slices]


def replace_ellipsis(n, index):
    """Replace ... with slices, :, : ,:

    >>> replace_ellipsis(4, (3, Ellipsis, 2))
    (3, slice(None, None, None), slice(None, None, None), 2)

    >>> replace_ellipsis(2, (Ellipsis, None))
    (slice(None, None, None), slice(None, None, None), None)
    """
    # Careful about using in or index because index may contain arrays
    isellipsis = [i for i, ind in enumerate(index) if ind is Ellipsis]
    if not isellipsis:
        return index
    else:
        loc = isellipsis[0]
    extra_dimensions = n - (len(index) - sum(i is None for i in index) - 1)
    return (
        index[:loc] + (slice(None, None, None),) * extra_dimensions + index[loc + 1 :]
    )


def normalize_slice(idx, dim):
    """Normalize slices to canonical form

    Parameters
    ----------
    idx: slice or other index
    dim: dimension length

    Examples
    --------
    >>> normalize_slice(slice(0, 10, 1), 10)
    slice(None, None, None)
    """

    if isinstance(idx, slice):
        if math.isnan(dim):
            return idx
        start, stop, step = idx.indices(dim)
        if step > 0:
            if start == 0:
                start = None
            if stop >= dim:
                stop = None
            if step == 1:
                step = None
            if stop is not None and start is not None and stop < start:
                stop = start
        elif step < 0:
            if start >= dim - 1:
                start = None
            if stop < 0:
                stop = None
        return slice(start, stop, step)
    return idx


def normalize_index(idx, shape):
    """Normalize slicing indexes

    1.  Replaces ellipses with many full slices
    2.  Adds full slices to end of index
    3.  Checks bounding conditions
    4.  Replace multidimensional numpy arrays with dask arrays
    5.  Replaces numpy arrays with lists
    6.  Posify's integers and lists
    7.  Normalizes slices to canonical form

    Examples
    --------
    >>> normalize_index(1, (10,))
    (1,)
    >>> normalize_index(-1, (10,))
    (9,)
    >>> normalize_index([-1], (10,))
    (array([9]),)
    >>> normalize_index(slice(-3, 10, 1), (10,))
    (slice(7, None, None),)
    >>> normalize_index((Ellipsis, None), (10,))
    (slice(None, None, None), None)
    >>> normalize_index(np.array([[True, False], [False, True], [True, True]]), (3, 2))
    (dask.array<array, shape=(3, 2), dtype=bool, chunksize=(3, 2), chunktype=numpy.ndarray>,)
    """
    from .core import Array, from_array

    if not isinstance(idx, tuple):
        idx = (idx,)

    # if a > 1D numpy.array is provided, cast it to a dask array
    if len(idx) > 0 and len(shape) > 1:
        i = idx[0]
        if is_arraylike(i) and not isinstance(i, Array) and i.shape == shape:
            idx = (from_array(i), *idx[1:])

    idx = replace_ellipsis(len(shape), idx)
    n_sliced_dims = 0
    for i in idx:
        if hasattr(i, "ndim") and i.ndim >= 1:
            n_sliced_dims += i.ndim
        elif i is None:
            continue
        else:
            n_sliced_dims += 1

    idx = idx + (slice(None),) * (len(shape) - n_sliced_dims)
    if len([i for i in idx if i is not None]) > len(shape):
        raise IndexError("Too many indices for array")

    none_shape = []
    i = 0
    for ind in idx:
        if ind is not None:
            none_shape.append(shape[i])
            i += 1
        else:
            none_shape.append(None)

    for i, d in zip(idx, none_shape):
        if d is not None:
            check_index(i, d)
    idx = tuple(map(sanitize_index, idx))
    idx = tuple(map(normalize_slice, idx, none_shape))
    idx = posify_index(none_shape, idx)
    return idx


def check_index(ind, dimension):
    """Check validity of index for a given dimension

    Examples
    --------
    >>> check_index(3, 5)
    >>> check_index(5, 5)
    Traceback (most recent call last):
    ...
    IndexError: Index is not smaller than dimension 5 >= 5

    >>> check_index(6, 5)
    Traceback (most recent call last):
    ...
    IndexError: Index is not smaller than dimension 6 >= 5

    >>> check_index(-1, 5)
    >>> check_index(-6, 5)
    Traceback (most recent call last):
    ...
    IndexError: Negative index is not greater than negative dimension -6 <= -5

    >>> check_index([1, 2], 5)
    >>> check_index([6, 3], 5)
    Traceback (most recent call last):
    ...
    IndexError: Index out of bounds 5

    >>> check_index(slice(0, 3), 5)

    >>> check_index([True], 1)
    >>> check_index([True, True], 3)
    Traceback (most recent call last):
    ...
    IndexError: Boolean array length 2 doesn't equal dimension 3
    >>> check_index([True, True, True], 1)
    Traceback (most recent call last):
    ...
    IndexError: Boolean array length 3 doesn't equal dimension 1
    """
    if isinstance(ind, list):
        ind = np.asanyarray(ind)

    # unknown dimension, assumed to be in bounds
    if np.isnan(dimension):
        return
    elif is_dask_collection(ind):
        return
    elif is_arraylike(ind):
        if ind.dtype == bool:
            if ind.size != dimension:
                raise IndexError(
                    "Boolean array length %s doesn't equal dimension %s"
                    % (ind.size, dimension)
                )
        elif (ind >= dimension).any() or (ind < -dimension).any():
            raise IndexError("Index out of bounds %s" % dimension)
    elif isinstance(ind, slice):
        return
    elif ind is None:
        return

    elif ind >= dimension:
        raise IndexError(
            "Index is not smaller than dimension %d >= %d" % (ind, dimension)
        )

    elif ind < -dimension:
        msg = "Negative index is not greater than negative dimension %d <= -%d"
        raise IndexError(msg % (ind, dimension))


def slice_with_int_dask_array(x, index):
    """Slice x with at most one 1D dask arrays of ints.

    This is a helper function of :meth:`Array.__getitem__`.

    Parameters
    ----------
    x: Array
    index: tuple with as many elements as x.ndim, among which there are
           one or more Array's with dtype=int

    Returns
    -------
    tuple of (sliced x, new index)

    where the new index is the same as the input, but with slice(None)
    replaced to the original slicer where a 1D filter has been applied and
    one less element where a zero-dimensional filter has been applied.
    """
    from .core import Array

    assert len(index) == x.ndim
    fancy_indexes = [
        isinstance(idx, (tuple, list))
        or (isinstance(idx, (np.ndarray, Array)) and idx.ndim > 0)
        for idx in index
    ]
    if sum(fancy_indexes) > 1:
        raise NotImplementedError("Don't yet support nd fancy indexing")

    out_index = []
    dropped_axis_cnt = 0
    for in_axis, idx in enumerate(index):
        out_axis = in_axis - dropped_axis_cnt
        if isinstance(idx, Array) and idx.dtype.kind in "iu":
            if idx.ndim == 0:
                idx = idx[np.newaxis]
                x = slice_with_int_dask_array_on_axis(x, idx, out_axis)
                x = x[tuple(0 if i == out_axis else slice(None) for i in range(x.ndim))]
                dropped_axis_cnt += 1
            elif idx.ndim == 1:
                x = slice_with_int_dask_array_on_axis(x, idx, out_axis)
                out_index.append(slice(None))
            else:
                raise NotImplementedError(
                    "Slicing with dask.array of ints only permitted when "
                    "the indexer has zero or one dimensions"
                )
        else:
            out_index.append(idx)
    return x, tuple(out_index)


def slice_with_int_dask_array_on_axis(x, idx, axis):
    """Slice a ND dask array with a 1D dask arrays of ints along the given
    axis.

    This is a helper function of :func:`slice_with_int_dask_array`.
    """
    from . import chunk
    from .core import Array, blockwise, from_array
    from .utils import asarray_safe

    assert 0 <= axis < x.ndim

    if np.isnan(x.chunks[axis]).any():
        raise NotImplementedError(
            "Slicing an array with unknown chunks with "
            "a dask.array of ints is not supported"
        )

    # Calculate the offset at which each chunk starts along axis
    # e.g. chunks=(..., (5, 3, 4), ...) -> offset=[0, 5, 8]
    offset = np.roll(np.cumsum(asarray_safe(x.chunks[axis], like=x._meta)), 1)
    offset[0] = 0
    offset = from_array(offset, chunks=1)
    # Tamper with the declared chunks of offset to make blockwise align it with
    # x[axis]
    offset = Array(
        offset.dask, offset.name, (x.chunks[axis],), offset.dtype, meta=x._meta
    )

    # Define axis labels for blockwise
    x_axes = tuple(range(x.ndim))
    idx_axes = (x.ndim,)  # arbitrary index not already in x_axes
    offset_axes = (axis,)
    p_axes = x_axes[: axis + 1] + idx_axes + x_axes[axis + 1 :]
    y_axes = x_axes[:axis] + idx_axes + x_axes[axis + 1 :]

    # Calculate the cartesian product of every chunk of x vs every chunk of idx
    p = blockwise(
        chunk.slice_with_int_dask_array,
        p_axes,
        x,
        x_axes,
        idx,
        idx_axes,
        offset,
        offset_axes,
        x_size=x.shape[axis],
        axis=axis,
        dtype=x.dtype,
        meta=x._meta,
    )

    # Aggregate on the chunks of x along axis
    y = blockwise(
        chunk.slice_with_int_dask_array_aggregate,
        y_axes,
        idx,
        idx_axes,
        p,
        p_axes,
        concatenate=True,
        x_chunks=x.chunks[axis],
        axis=axis,
        dtype=x.dtype,
        meta=x._meta,
    )
    return y


def slice_with_bool_dask_array(x, index):
    """Slice x with one or more dask arrays of bools

    This is a helper function of `Array.__getitem__`.

    Parameters
    ----------
    x: Array
    index: tuple with as many elements as x.ndim, among which there are
           one or more Array's with dtype=bool

    Returns
    -------
    tuple of (sliced x, new index)

    where the new index is the same as the input, but with slice(None)
    replaced to the original slicer when a filter has been applied.

    Note: The sliced x will have nan chunks on the sliced axes.
    """
    from .core import Array, blockwise, elemwise

    out_index = [
        slice(None) if isinstance(ind, Array) and ind.dtype == bool else ind
        for ind in index
    ]

    if len(index) == 1 and index[0].ndim == x.ndim:
        if not np.isnan(x.shape).any() and not np.isnan(index[0].shape).any():
            x = x.ravel()
            index = tuple(i.ravel() for i in index)
        elif x.ndim > 1:
            warnings.warn(
                "When slicing a Dask array of unknown chunks with a boolean mask "
                "Dask array, the output array may have a different ordering "
                "compared to the equivalent NumPy operation. This will raise an "
                "error in a future release of Dask.",
                stacklevel=3,
            )
        y = elemwise(getitem, x, *index, dtype=x.dtype)
        name = "getitem-" + tokenize(x, index)
        dsk = {(name, i): k for i, k in enumerate(core.flatten(y.__dask_keys__()))}
        chunks = ((np.nan,) * y.npartitions,)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[y])
        return Array(graph, name, chunks, x.dtype), out_index

    if any(
        isinstance(ind, Array) and ind.dtype == bool and ind.ndim != 1 for ind in index
    ):
        raise NotImplementedError(
            "Slicing with dask.array of bools only permitted when "
            "the indexer has only one dimension or when "
            "it has the same dimension as the sliced "
            "array"
        )
    indexes = [
        ind if isinstance(ind, Array) and ind.dtype == bool else slice(None)
        for ind in index
    ]

    arginds = []
    i = 0
    for ind in indexes:
        if isinstance(ind, Array) and ind.dtype == bool:
            new = (ind, tuple(range(i, i + ind.ndim)))
            i += x.ndim
        else:
            new = (slice(None), None)
            i += 1
        arginds.append(new)

    arginds = list(concat(arginds))

    out = blockwise(
        getitem_variadic,
        tuple(range(x.ndim)),
        x,
        tuple(range(x.ndim)),
        *arginds,
        dtype=x.dtype,
    )

    chunks = []
    for ind, chunk in zip(index, out.chunks):
        if isinstance(ind, Array) and ind.dtype == bool:
            chunks.append((np.nan,) * len(chunk))
        else:
            chunks.append(chunk)
    out._chunks = tuple(chunks)
    return out, tuple(out_index)


def getitem_variadic(x, *index):
    return x[index]


def make_block_sorted_slices(index, chunks):
    """Generate blockwise-sorted index pairs for shuffling an array.

    Parameters
    ----------
    index : ndarray
        An array of index positions.
    chunks : tuple
        Chunks from the original dask array

    Returns
    -------
    index2 : ndarray
        Same values as `index`, but each block has been sorted
    index3 : ndarray
        The location of the values of `index` in `index2`

    Examples
    --------
    >>> index = np.array([6, 0, 4, 2, 7, 1, 5, 3])
    >>> chunks = ((4, 4),)
    >>> a, b = make_block_sorted_slices(index, chunks)

    Notice that the first set of 4 items are sorted, and the
    second set of 4 items are sorted.

    >>> a
    array([0, 2, 4, 6, 1, 3, 5, 7])
    >>> b
    array([3, 0, 2, 1, 7, 4, 6, 5])
    """
    from .core import slices_from_chunks

    slices = slices_from_chunks(chunks)

    if len(slices[0]) > 1:
        slices = [slice_[0] for slice_ in slices]

    offsets = np.roll(np.cumsum(chunks[0]), 1)
    offsets[0] = 0

    index2 = np.empty_like(index)
    index3 = np.empty_like(index)

    for slice_, offset in zip(slices, offsets):
        a = index[slice_]
        b = np.sort(a)
        c = offset + np.argsort(b.take(np.argsort(a)))
        index2[slice_] = b
        index3[slice_] = c

    return index2, index3


def shuffle_slice(x, index):
    """A relatively efficient way to shuffle `x` according to `index`.

    Parameters
    ----------
    x : Array
    index : ndarray
        This should be an ndarray the same length as `x` containing
        each index position in ``range(0, len(x))``.

    Returns
    -------
    Array
    """
    from .core import PerformanceWarning

    chunks1 = chunks2 = x.chunks
    if x.ndim > 1:
        chunks1 = (chunks1[0],)
    index2, index3 = make_block_sorted_slices(index, chunks1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PerformanceWarning)
        return x[index2].rechunk(chunks2)[index3]


class _HashIdWrapper:
    """Hash and compare a wrapped object by identity instead of value"""

    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __eq__(self, other):
        if not isinstance(other, _HashIdWrapper):
            return NotImplemented
        return self.wrapped is other.wrapped

    def __ne__(self, other):
        if not isinstance(other, _HashIdWrapper):
            return NotImplemented
        return self.wrapped is not other.wrapped

    def __hash__(self):
        return id(self.wrapped)


@functools.lru_cache()
def _cumsum(seq, initial_zero):
    if isinstance(seq, _HashIdWrapper):
        seq = seq.wrapped
    if initial_zero:
        return tuple(accumulate(add, seq, 0))
    else:
        return tuple(accumulate(add, seq))


def cached_cumsum(seq, initial_zero=False):
    """Compute :meth:`toolz.accumulate` with caching.

    Caching is by the identify of `seq` rather than the value. It is thus
    important that `seq` is a tuple of immutable objects, and this function
    is intended for use where `seq` is a value that will persist (generally
    block sizes).

    Parameters
    ----------
    seq : tuple
        Values to cumulatively sum.
    initial_zero : bool, optional
        If true, the return value is prefixed with a zero.

    Returns
    -------
    tuple
    """
    if isinstance(seq, tuple):
        # Look up by identity first, to avoid a linear-time __hash__
        # if we've seen this tuple object before.
        result = _cumsum(_HashIdWrapper(seq), initial_zero)
    else:
        # Construct a temporary tuple, and look up by value.
        result = _cumsum(tuple(seq), initial_zero)
    return result

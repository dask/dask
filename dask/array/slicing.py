from __future__ import absolute_import, division, print_function

from itertools import product
from math import ceil
from numbers import Number
from operator import getitem, add, itemgetter

import numpy as np
from toolz import merge, accumulate, pluck, memoize

from ..base import tokenize
from ..compatibility import long


colon = slice(None, None, None)


def sanitize_index(ind):
    """ Sanitize the elements for indexing along one axis

    >>> sanitize_index([2, 3, 5])
    [2, 3, 5]
    >>> sanitize_index([True, False, True, False])
    [0, 2]
    >>> sanitize_index(np.array([1, 2, 3]))
    [1, 2, 3]
    >>> sanitize_index(np.array([False, True, True]))
    [1, 2]
    >>> type(sanitize_index(np.int32(0)))
    <type 'int'>
    >>> sanitize_index(1.0)
    1
    >>> sanitize_index(0.5)
    Traceback (most recent call last):
    ...
    IndexError: Bad index.  Must be integer-like: 0.5
    """
    if isinstance(ind, Number):
        ind2 = int(ind)
        if ind2 != ind:
            raise IndexError("Bad index.  Must be integer-like: %s" % ind)
        else:
            return ind2
    if hasattr(ind, 'tolist'):
        ind = ind.tolist()
    if isinstance(ind, list) and ind and isinstance(ind[0], bool):
        ind = [a for a, b in enumerate(ind) if b]
        return ind
    if isinstance(ind, list):
        return [sanitize_index(i) for i in ind]
    if isinstance(ind, slice):
        return slice(sanitize_index(ind.start),
                     sanitize_index(ind.stop),
                     sanitize_index(ind.step))
    if ind is None:
        return ind
    try:
        return sanitize_index(np.array(ind).tolist())
    except:
        raise TypeError("Invalid index type", type(ind), ind)


def slice_array(out_name, in_name, blockdims, index):
    """
    Master function for array slicing

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

    slice_with_newaxis - handle None/newaxis case
    slice_wrap_lists - handle fancy indexing with lists
    slice_slices_and_integers - handle everything else
    """
    index = replace_ellipsis(len(blockdims), index)
    index = tuple(map(sanitize_index, index))
    blockdims = tuple(map(tuple, blockdims))

    # x[:, :, :] - Punt and return old value
    if all(index == slice(None, None, None) for index in index):
        suffixes = product(*[range(len(bd)) for bd in blockdims])
        dsk = dict(((out_name,) + s, (in_name,) + s)
                   for s in suffixes)
        return dsk, blockdims

    # Add in missing colons at the end as needed.  x[5] -> x[5, :, :]
    missing = len(blockdims) - (len(index) - index.count(None))
    index += (slice(None, None, None),) * missing

    # Pass down to next function
    dsk_out, bd_out = slice_with_newaxes(out_name, in_name, blockdims, index)

    bd_out = tuple(map(tuple, bd_out))
    return dsk_out, bd_out


def slice_with_newaxes(out_name, in_name, blockdims, index):
    """
    Handle indexing with Nones

    Strips out Nones then hands off to slice_wrap_lists
    """
    # Strip Nones from index
    index2 = tuple([ind for ind in index if ind is not None])
    where_none = [i for i, ind in enumerate(index) if ind is None]
    where_none_orig = list(where_none)
    for i, x in enumerate(where_none):
        n = sum(isinstance(ind, int) for ind in index[:x])
        if n:
            where_none[i] -= n

    # Pass down and do work
    dsk, blockdims2 = slice_wrap_lists(out_name, in_name, blockdims, index2)

    if where_none:
        expand = expander(where_none)
        expand_orig = expander(where_none_orig)

        # Insert ",0" into the key:  ('x', 2, 3) -> ('x', 0, 2, 0, 3)
        dsk2 = {(out_name,) + expand(k[1:], 0):
                (v[:2] + (expand_orig(v[2], None),))
                for k, v in dsk.items()
                if k[0] == out_name}

        # Add back intermediate parts of the dask that weren't the output
        dsk3 = merge(dsk2, {k: v for k, v in dsk.items() if k[0] != out_name})

        # Insert (1,) into blockdims:  ((2, 2), (3, 3)) -> ((2, 2), (1,), (3, 3))
        blockdims3 = expand(blockdims2, (1,))

        return dsk3, blockdims3

    else:
        return dsk, blockdims2


def slice_wrap_lists(out_name, in_name, blockdims, index):
    """
    Fancy indexing along blocked array dasks

    Handles index of type list.  Calls slice_slices_and_integers for the rest

    See Also
    --------

    take - handle slicing with lists ("fancy" indexing)
    slice_slices_and_integers - handle slicing with slices and integers
    """
    shape = tuple(map(sum, blockdims))
    assert all(isinstance(i, (slice, list, int, long)) for i in index)
    if not len(blockdims) == len(index):
        raise IndexError("Too many indices for array")
    for bd, i in zip(blockdims, index):
        check_index(i, sum(bd))

    # Change indices like -1 to 9
    index2 = posify_index(shape, index)

    # Do we have more than one list in the index?
    where_list = [i for i, ind in enumerate(index) if isinstance(ind, list)]
    if len(where_list) > 1:
        raise NotImplementedError("Don't yet support nd fancy indexing")

    # No lists, hooray! just use slice_slices_and_integers
    if not where_list:
        return slice_slices_and_integers(out_name, in_name, blockdims, index2)

    # Replace all lists with full slices  [3, 1, 0] -> slice(None, None, None)
    index_without_list = tuple(slice(None, None, None)
                               if isinstance(i, list) else i
                               for i in index2)

    # lists and full slices.  Just use take
    if all(isinstance(i, list) or i == slice(None, None, None)
            for i in index2):
        axis = where_list[0]
        blockdims2, dsk3 = take(out_name, in_name, blockdims,
                                index2[where_list[0]], axis=axis)
    # Mixed case. Both slices/integers and lists. slice/integer then take
    else:
        # Do first pass without lists
        tmp = 'slice-' + tokenize((out_name, in_name, blockdims, index))
        dsk, blockdims2 = slice_slices_and_integers(tmp, in_name, blockdims, index_without_list)

        # After collapsing some axes due to int indices, adjust axis parameter
        axis = where_list[0]
        axis2 = axis - sum(1 for i, ind in enumerate(index2)
                           if i < axis and isinstance(ind, (int, long)))

        # Do work
        blockdims2, dsk2 = take(out_name, tmp, blockdims2, index2[axis],
                                axis=axis2)
        dsk3 = merge(dsk, dsk2)

    return dsk3, blockdims2


def slice_slices_and_integers(out_name, in_name, blockdims, index):
    """
    Dask array indexing with slices and integers

    See Also
    --------

    _slice_1d
    """
    shape = tuple(map(sum, blockdims))

    for dim, ind in zip(shape, index):
        if np.isnan(dim) and ind != slice(None, None, None):
            raise ValueError("Arrays chunk sizes are unknown: %s", shape)

    assert all(isinstance(ind, (slice, int, long)) for ind in index)
    assert len(index) == len(blockdims)

    # Get a list (for each dimension) of dicts{blocknum: slice()}
    block_slices = list(map(_slice_1d, shape, blockdims, index))
    sorted_block_slices = [sorted(i.items()) for i in block_slices]

    # (in_name, 1, 1, 2), (in_name, 1, 1, 4), (in_name, 2, 1, 2), ...
    in_names = list(product([in_name], *[pluck(0, s) for s in sorted_block_slices]))

    # (out_name, 0, 0, 0), (out_name, 0, 0, 1), (out_name, 0, 1, 0), ...
    out_names = list(product([out_name],
                             *[range(len(d))[::-1] if i.step and i.step < 0 else range(len(d))
                               for d, i in zip(block_slices, index)
                               if not isinstance(i, (int, long))]))

    all_slices = list(product(*[pluck(1, s) for s in sorted_block_slices]))

    dsk_out = {out_name: (getitem, in_name, slices)
               for out_name, in_name, slices
               in zip(out_names, in_names, all_slices)}

    new_blockdims = [new_blockdim(d, db, i)
                     for d, i, db in zip(shape, index, blockdims)
                     if not isinstance(i, (int, long))]

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

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(100, 0, -3))
    {0: slice(-2, -20, -3), 1: slice(-1, -21, -3), 2: slice(-3, -21, -3), 3: slice(-2, -21, -3), 4: slice(-1, -21, -3)}

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(100, 12, -3))
    {0: slice(-2, -8, -3), 1: slice(-1, -21, -3), 2: slice(-3, -21, -3), 3: slice(-2, -21, -3), 4: slice(-1, -21, -3)}

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(100, -12, -3))
    {4: slice(-1, -12, -3)}
    """
    if isinstance(index, (int, long)):
        i = 0
        ind = index
        lens = list(lengths)
        while ind >= lens[0]:
            i += 1
            ind -= lens.pop(0)
        return {i: ind}

    assert isinstance(index, slice)

    if index == colon:
        return {k: colon for k in range(len(lengths))}

    step = index.step or 1
    if step > 0:
        start = index.start or 0
        stop = index.stop if index.stop is not None else dim_shape
    else:
        start = index.start or dim_shape - 1
        start = dim_shape - 1 if start >= dim_shape else start
        stop = -(dim_shape + 1) if index.stop is None else index.stop

    # posify start and stop
    if start < 0:
        start += dim_shape
    if stop < 0:
        stop += dim_shape

    d = dict()
    if step > 0:
        for i, length in enumerate(lengths):
            if start < length and stop > 0:
                d[i] = slice(start, min(stop, length), step)
                start = (start - length) % step
            else:
                start = start - length
            stop -= length
    else:
        rstart = start  # running start
        chunk_boundaries = list(accumulate(add, lengths))
        for i, chunk_stop in reversed(list(enumerate(chunk_boundaries))):
            # create a chunk start and stop
            if i == 0:
                chunk_start = 0
            else:
                chunk_start = chunk_boundaries[i - 1]

            # if our slice is in this chunk
            if (chunk_start <= rstart < chunk_stop) and (rstart > stop):
                d[i] = slice(rstart - chunk_stop,
                             max(chunk_start - chunk_stop - 1,
                                 stop - chunk_stop),
                             step)

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
    [[1, 5, 9], [2, 19], [5]]
    """
    seq = np.array(seq)
    right = np.cumsum(sizes)
    locations = np.searchsorted(seq, right)
    locations = [0] + locations.tolist()
    left = [0] + right.tolist()
    return [(seq[locations[i]:locations[i + 1]] - left[i]).tolist()
            for i in range(len(locations) - 1)]


def issorted(seq):
    """ Is sequence sorted?

    >>> issorted([1, 2, 3])
    True
    >>> issorted([3, 1, 2])
    False
    """
    if not seq:
        return True
    x = seq[0]
    for elem in seq[1:]:
        if elem < x:
            return False
        x = elem
    return True


def take_sorted(outname, inname, blockdims, index, axis=0):
    """ Index array with sorted list index

    Forms a dask for the following case

        x[:, [1, 3, 5, 10], ...]

    where the index, ``[1, 3, 5, 10]`` is sorted in non-decreasing order.

    >>> blockdims, dsk = take('y', 'x', [(20, 20, 20, 20)], [1, 3, 5, 47], axis=0)
    >>> blockdims
    ((3, 1),)
    >>> dsk  # doctest: +SKIP
    {('y', 0): (getitem, ('x', 0), ([1, 3, 5],)),
     ('y', 1): (getitem, ('x', 2), ([7],))}

    See Also
    --------
    take - calls this function
    """
    sizes = blockdims[axis]  # the blocksizes on the axis that we care about

    index_lists = partition_by_size(sizes, sorted(index))
    where_index = [i for i, il in enumerate(index_lists) if il]
    index_lists = [il for il in index_lists if il]

    dims = [range(len(bd)) for bd in blockdims]

    indims = list(dims)
    indims[axis] = list(range(len(where_index)))
    keys = list(product([outname], *indims))

    outdims = list(dims)
    outdims[axis] = where_index
    slices = [[colon] * len(bd) for bd in blockdims]
    slices[axis] = index_lists
    slices = list(product(*slices))
    inkeys = list(product([inname], *outdims))
    values = [(getitem, inkey, slc) for inkey, slc in zip(inkeys, slices)]

    blockdims2 = list(blockdims)
    blockdims2[axis] = tuple(map(len, index_lists))

    return tuple(blockdims2), dict(zip(keys, values))


def take(outname, inname, blockdims, index, axis=0):
    """ Index array with an iterable of index

    Handles a single index by a single list

    Mimics ``np.take``

    >>> blockdims, dsk = take('y', 'x', [(20, 20, 20, 20)], [5, 1, 47, 3], axis=0)
    >>> blockdims
    ((4,),)
    >>> dsk  # doctest: +SKIP
    {('y', 0): (getitem, (np.concatenate, [(getitem, ('x', 0), ([1, 3, 5],)),
                                           (getitem, ('x', 2), ([7],))],
                                          0),
                         (2, 0, 4, 1))}

    When list is sorted we retain original block structure

    >>> blockdims, dsk = take('y', 'x', [(20, 20, 20, 20)], [1, 3, 5, 47], axis=0)
    >>> blockdims
    ((3, 1),)
    >>> dsk  # doctest: +SKIP
    {('y', 0): (getitem, ('x', 0), ([1, 3, 5],)),
     ('y', 2): (getitem, ('x', 2), ([7],))}
    """
    if issorted(index):
        return take_sorted(outname, inname, blockdims, index, axis)

    n = len(blockdims)
    sizes = blockdims[axis]  # the blocksizes on the axis that we care about

    index_lists = partition_by_size(sizes, sorted(index))

    dims = [[0] if axis == i else list(range(len(bd)))
            for i, bd in enumerate(blockdims)]
    keys = list(product([outname], *dims))

    rev_index = list(map(sorted(index).index, index))
    vals = [(getitem, (np.concatenate,
                       [(getitem, ((inname, ) + d[:axis] + (i, ) + d[axis + 1:]),
                         ((colon, ) * axis + (IL, ) + (colon, ) * (n - axis - 1)))
                        for i, IL in enumerate(index_lists) if IL], axis),
             ((colon, ) * axis + (rev_index, ) + (colon, ) * (n - axis - 1)))
            for d in product(*dims)]

    blockdims2 = list(blockdims)
    blockdims2[axis] = (len(index), )

    return tuple(blockdims2), dict(zip(keys, vals))


def posify_index(shape, ind):
    """ Flip negative indices around to positive ones

    >>> posify_index(10, 3)
    3
    >>> posify_index(10, -3)
    7
    >>> posify_index(10, [3, -3])
    [3, 7]

    >>> posify_index((10, 20), (3, -3))
    (3, 17)
    >>> posify_index((10, 20), (3, [3, 4, -3]))
    (3, [3, 4, 17])
    """
    if isinstance(ind, tuple):
        return tuple(map(posify_index, shape, ind))
    if isinstance(ind, (int, long)):
        if ind < 0:
            return ind + shape
        else:
            return ind
    if isinstance(ind, list):
        return [i + shape if i < 0 else i for i in ind]
    return ind


def insert_many(seq, where, val):
    """ Insert value at many locations in sequence

    >>> insert_many(['a', 'b', 'c'], [0, 2], 'z')
    ('z', 'a', 'z', 'b', 'c')
    """
    seq = list(seq)
    result = []
    for i in range(len(where) + len(seq)):
        if i in where:
            result.append(val)
        else:
            result.append(seq.pop(0))
    return tuple(result)


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
        return ns['expand']


def expander(where):
    """ An optimized version of insert_many() when *where*
    is known upfront and used many times.

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
    assert not isinstance(index, (int, long))
    pairs = sorted(_slice_1d(dim_shape, lengths, index).items(),
                   key=itemgetter(0))
    slices = [slice(0, lengths[i], 1) if slc == slice(None, None, None) else slc
              for i, slc in pairs]
    if isinstance(index, slice) and index.step and index.step < 0:
        slices = slices[::-1]
    return [int(ceil((1. * slc.stop - slc.start) / slc.step)) for slc in slices]


def replace_ellipsis(n, index):
    """ Replace ... with slices, :, : ,:

    >>> replace_ellipsis(4, (3, Ellipsis, 2))
    (3, slice(None, None, None), slice(None, None, None), 2)

    >>> replace_ellipsis(2, (Ellipsis, None))
    (slice(None, None, None), slice(None, None, None), None)
    """
    # Careful about using in or index because index may contain arrays
    isellipsis = [i for i, ind in enumerate(index) if ind is Ellipsis]
    extra_dimensions = n - (len(index) - sum(i is None for i in index) - 1)
    if not isellipsis:
        return index
    else:
        loc = isellipsis[0]
    return (index[:loc] + (slice(None, None, None),) * extra_dimensions +
            index[loc + 1:])


def check_index(ind, dimension):
    """ Check validity of index for a given dimension

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
    """
    if isinstance(ind, list):
        x = np.array(ind)
        if (x >= dimension).any() or (x <= -dimension).any():
            raise IndexError("Index out of bounds %s" % dimension)
    elif isinstance(ind, slice):
        return

    elif ind >= dimension:
        raise IndexError("Index is not smaller than dimension %d >= %d" %
                         (ind, dimension))

    elif ind < -dimension:
        msg = "Negative index is not greater than negative dimension %d <= -%d"
        raise IndexError(msg % (ind, dimension))

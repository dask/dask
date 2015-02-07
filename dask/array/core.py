from __future__ import absolute_import, division, print_function

from operator import add, getitem
from bisect import bisect
import operator
from math import ceil, floor
from itertools import product, count
from collections import Iterator
from functools import partial
from toolz.curried import (identity, pipe, partition, concat, unique, pluck,
        frequencies, join, first, memoize, map, groupby, valmap, accumulate,
        merge)
import numpy as np
from ..utils import deepmap
from ..async import inline_functions
from ..optimize import cull
from .. import threaded, core


names = ('x_%d' % i for i in count(1))


def getem(arr, blocksize, shape):
    """ Dask getting various chunks from an array-like

    >>> getem('X', blocksize=(2, 3), shape=(4, 6))  # doctest: +SKIP
    {('X', 0, 0): (getitem, 'X', (slice(0, 2), slice(0, 3))),
     ('X', 1, 0): (getitem, 'X', (slice(2, 4), slice(0, 3))),
     ('X', 1, 1): (getitem, 'X', (slice(2, 4), slice(3, 6))),
     ('X', 0, 1): (getitem, 'X', (slice(0, 2), slice(3, 6)))}
    """
    numblocks = tuple([int(ceil(n/k)) for n, k in zip(shape, blocksize)])
    return dict(
               ((arr,) + ijk,
               (getitem,
                 arr,
                 tuple(slice(i*d, (i+1)*d) for i, d in zip(ijk, blocksize))))
               for ijk in product(*map(range, numblocks)))


def dotmany(A, B, leftfunc=None, rightfunc=None, **kwargs):
    """ Dot product of many aligned chunks

    >>> x = np.array([[1, 2], [1, 2]])
    >>> y = np.array([[10, 20], [10, 20]])
    >>> dotmany([x, x, x], [y, y, y])
    array([[ 90, 180],
           [ 90, 180]])

    Optionally pass in functions to apply to the left and right chunks

    >>> dotmany([x, x, x], [y, y, y], rightfunc=np.transpose)
    array([[150, 150],
           [150, 150]])
    """
    if leftfunc:
        A = map(leftfunc, A)
    if rightfunc:
        B = map(rightfunc, B)
    return sum(map(partial(np.dot, **kwargs), A, B))


def lol_tuples(head, ind, values, dummies):
    """ List of list of tuple keys

    Parameters
    ----------

    head : tuple
        The known tuple so far
    ind : Iterable
        An iterable of indices not yet covered
    values : dict
        Known values for non-dummy indices
    dummies : dict
        Ranges of values for dummy indices

    Examples
    --------

    >>> lol_tuples(('x',), 'ij', {'i': 1, 'j': 0}, {})
    ('x', 1, 0)

    >>> lol_tuples(('x',), 'ij', {'i': 1}, {'j': range(3)})
    [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]

    >>> lol_tuples(('x',), 'ij', {'i': 1}, {'j': range(3)})
    [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]

    >>> lol_tuples(('x',), 'ijk', {'i': 1}, {'j': [0, 1, 2], 'k': [0, 1]}) # doctest: +NORMALIZE_WHITESPACE
    [[('x', 1, 0, 0), ('x', 1, 0, 1)],
     [('x', 1, 1, 0), ('x', 1, 1, 1)],
     [('x', 1, 2, 0), ('x', 1, 2, 1)]]
    """
    if not ind:
        return head
    if ind[0] not in dummies:
        return lol_tuples(head + (values[ind[0]],), ind[1:], values, dummies)
    else:
        return [lol_tuples(head + (v,), ind[1:], values, dummies)
                for v in dummies[ind[0]]]


def zero_broadcast_dimensions(lol, nblocks):
    """

    >>> lol = [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]
    >>> nblocks = (4, 1, 2)  # note singleton dimension in second place
    >>> lol = [[('x', 1, 0, 0), ('x', 1, 0, 1)],
    ...        [('x', 1, 1, 0), ('x', 1, 1, 1)],
    ...        [('x', 1, 2, 0), ('x', 1, 2, 1)]]

    >>> zero_broadcast_dimensions(lol, nblocks)  # doctest: +NORMALIZE_WHITESPACE
    [[('x', 1, 0, 0), ('x', 1, 0, 1)],
     [('x', 1, 0, 0), ('x', 1, 0, 1)],
     [('x', 1, 0, 0), ('x', 1, 0, 1)]]

    See Also
    --------

    lol_tuples
    """
    f = lambda t: (t[0],) + tuple(0 if d == 1 else i for i, d in zip(t[1:], nblocks))
    return deepmap(f, lol)


def broadcast_dimensions(argpairs, numblocks, sentinels=(1, (1,))):
    """ Find block dimensions from arguments

    Parameters
    ----------

    argpairs: iterable
        name, ijk index pairs
    numblocks: dict
        maps {name: number of blocks}
    sentinels: iterable (optional)
        values for singleton dimensions

    Examples
    --------

    >>> argpairs = [('x', 'ij'), ('y', 'ji')]
    >>> numblocks = {'x': (2, 3), 'y': (3, 2)}
    >>> broadcast_dimensions(argpairs, numblocks)
    {'i': 2, 'j': 3}

    Supports numpy broadcasting rules

    >>> argpairs = [('x', 'ij'), ('y', 'ij')]
    >>> numblocks = {'x': (2, 1), 'y': (1, 3)}
    >>> broadcast_dimensions(argpairs, numblocks)
    {'i': 2, 'j': 3}

    Works in other contexts too

    >>> argpairs = [('x', 'ij'), ('y', 'ij')]
    >>> d = {'x': ('Hello', 1), 'y': (1, (2, 3))}
    >>> broadcast_dimensions(argpairs, d)
    {'i': 'Hello', 'j': (2, 3)}
    """
    # List like [('i', 2), ('j', 1), ('i', 1), ('j', 2)]
    L = concat([zip(inds, dims)
                    for (x, inds), (x, dims)
                    in join(first, argpairs, first, numblocks.items())])
    g = groupby(0, L)
    g = dict((k, set([d for i, d in v])) for k, v in g.items())

    g2 = dict((k, v - set(sentinels) if len(v) > 1 else v) for k, v in g.items())

    if not set(map(len, g2.values())) == set([1]):
        raise ValueError("Shapes do not align %s" % g)

    return valmap(first, g2)


def top(func, output, out_indices, *arrind_pairs, **kwargs):
    """ Tensor operation

    Applies a function, ``func``, across blocks from many different input
    dasks.  We arrange the pattern with which those blocks interact with sets
    of matching indices.  E.g.

        top(func, 'z', 'i', 'x', 'i', 'y', 'i')

    yield an embarassingly parallel communication pattern and is read as

        z_i = func(x_i, y_i)

    More complex patterns may emerge, including multiple indices

        top(func, 'z', 'ij', 'x', 'ij', 'y', 'ji')

        $$ z_{ij} = func(x_{ij}, y_{ji}) $$

    Indices missing in the output but present in the inputs results in many
    inputs being sent to one function (see examples).

    Examples
    --------

    Simple embarassing map operation

    >>> inc = lambda x: x + 1
    >>> top(inc, 'z', 'ij', 'x', 'ij', numblocks={'x': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (inc, ('x', 0, 0)),
     ('z', 0, 1): (inc, ('x', 0, 1)),
     ('z', 1, 0): (inc, ('x', 1, 0)),
     ('z', 1, 1): (inc, ('x', 1, 1))}

    Simple operation on two datasets

    >>> add = lambda x, y: x + y
    >>> top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij', numblocks={'x': (2, 2),
    ...                                                      'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
     ('z', 1, 0): (add, ('x', 1, 0), ('y', 1, 0)),
     ('z', 1, 1): (add, ('x', 1, 1), ('y', 1, 1))}

    Operation that flips one of the datasets

    >>> addT = lambda x, y: x + y.T  # Transpose each chunk
    >>> #                                        z_ij ~ x_ij y_ji
    >>> #               ..         ..         .. notice swap
    >>> top(addT, 'z', 'ij', 'x', 'ij', 'y', 'ji', numblocks={'x': (2, 2),
    ...                                                       'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 1, 0)),
     ('z', 1, 0): (add, ('x', 1, 0), ('y', 0, 1)),
     ('z', 1, 1): (add, ('x', 1, 1), ('y', 1, 1))}

    Dot product with contraction over ``j`` index.  Yields list arguments

    >>> top(dotmany, 'z', 'ik', 'x', 'ij', 'y', 'jk', numblocks={'x': (2, 2),
    ...                                                          'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                            [('y', 0, 0), ('y', 1, 0)]),
     ('z', 0, 1): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                            [('y', 0, 1), ('y', 1, 1)]),
     ('z', 1, 0): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                            [('y', 0, 0), ('y', 1, 0)]),
     ('z', 1, 1): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                            [('y', 0, 1), ('y', 1, 1)])}

    Supports Broadcasting rules

    >>> top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij', numblocks={'x': (1, 2),
    ...                                                      'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
     ('z', 1, 0): (add, ('x', 0, 0), ('y', 1, 0)),
     ('z', 1, 1): (add, ('x', 0, 1), ('y', 1, 1))}
    """
    numblocks = kwargs['numblocks']
    argpairs = list(partition(2, arrind_pairs))

    assert set(numblocks) == set(pluck(0, argpairs))

    all_indices = pipe(argpairs, pluck(1), concat, set)
    dummy_indices = all_indices - set(out_indices)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    dims = broadcast_dimensions(argpairs, numblocks)

    # (0, 0), (0, 1), (0, 2), (1, 0), ...
    keytups = list(product(*[range(dims[i]) for i in out_indices]))
    # {i: 0, j: 0}, {i: 0, j: 1}, ...
    keydicts = [dict(zip(out_indices, tup)) for tup in keytups]

    # {j: [1, 2, 3], ...}  For j a dummy index of dimension 3
    dummies = dict((i, list(range(dims[i]))) for i in dummy_indices)

    # Create argument lists
    valtups = []
    for kd in keydicts:
        args = []
        for arg, ind in argpairs:
            tups = lol_tuples((arg,), ind, kd, dummies)
            tups2 = zero_broadcast_dimensions(tups, numblocks[arg])
            args.append(tups2)
        valtups.append(tuple(args))

    # Add heads to tuples
    keys = [(output,) + kt for kt in keytups]
    vals = [(func,) + vt for vt in valtups]

    return dict(zip(keys, vals))


def _concatenate2(arrays, axes=[]):
    """ Recursively Concatenate nested lists of arrays along axes

    Each entry in axes corresponds to each level of the nested list.  The
    length of axes should correspond to the level of nesting of arrays.

    >>> x = np.array([[1, 2], [3, 4]])
    >>> _concatenate2([x, x], axes=[0])
    array([[1, 2],
           [3, 4],
           [1, 2],
           [3, 4]])

    >>> _concatenate2([x, x], axes=[1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4]])

    >>> _concatenate2([[x, x], [x, x]], axes=[0, 1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4],
           [1, 2, 1, 2],
           [3, 4, 3, 4]])

    Supports Iterators
    >>> _concatenate2(iter([x, x]), axes=[1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4]])
    """
    if isinstance(arrays, Iterator):
        arrays = list(arrays)
    if len(axes) > 1:
        arrays = [_concatenate2(a, axes=axes[1:]) for a in arrays]
    return np.concatenate(arrays, axis=axes[0])
    if len(axes) == 1:
        return np.concatenate(arrays, axis=axes[0])
    else:
        return np.concatenate


def rec_concatenate(arrays, axis=0):
    """ Recursive np.concatenate

    >>> x = np.array([1, 2])
    >>> rec_concatenate([[x, x], [x, x], [x, x]])
    array([[1, 2, 1, 2],
           [1, 2, 1, 2],
           [1, 2, 1, 2]])
    """
    if isinstance(arrays, Iterator):
        arrays = list(arrays)
    if isinstance(arrays[0], Iterator):
        arrays = list(map(list, arrays))
    if not isinstance(arrays[0], np.ndarray):
        arrays = [rec_concatenate(a, axis=axis + 1) for a in arrays]
    if arrays[0].ndim <= axis:
        arrays = [a[None, ...] for a in arrays]
    return np.concatenate(arrays, axis=axis)


def new_blockdim(dim_shape, lengths, index):
    """

    >>> new_blockdim(100, [20, 10, 20, 10, 40], slice(0, 90, 2))
    [10, 5, 10, 5, 15]

    >>> new_blockdim(100, [20, 10, 20, 10, 40], [5, 1, 30, 22])
    [4]
    """
    if isinstance(index, list):
        return [len(index)]
    assert not isinstance(index, int)
    pairs = sorted(_slice_1d(dim_shape, lengths, index).items(), key=first)
    return [(slc.stop - slc.start) // slc.step for _, slc in pairs]


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

    100 length array cut into length 20 pieces, slice 0:35

    >>> _slice_1d(100, [20, 20, 20, 20, 20], slice(0, 35))
    {0: slice(0, 20, 1), 1: slice(0, 15, 1)}

    Support irregular blocks and various slices

    >>> _slice_1d(100, [20, 10, 10, 10, 25, 25], slice(10, 35))
    {0: slice(10, 20, 1), 1: slice(0, 10, 1), 2: slice(0, 5, 1)}

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
    if isinstance(index, int):
        i = 0
        ind = index
        lens = list(lengths)
        while ind >= lens[0]:
            i += 1
            ind -= lens.pop(0)
        return {i: ind}

    assert isinstance(index, slice)

    step = index.step or 1
    if step > 0:
        start = index.start or 0
        stop = index.stop or dim_shape
    else:
        start = index.start or dim_shape - 1
        start = dim_shape - 1 if start >= dim_shape else start
        stop = -(dim_shape + 1) if index.stop is None else index.stop

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
        stop -= dim_shape
        tail_index = list(accumulate(add, lengths))
        pos_step = abs(step) # 11%3==2, 11%-3==-1. Need positive step for %

        offset = 0
        for i, length in zip(range(len(lengths)-1, -1, -1), reversed(lengths)):
            if start + length >= tail_index[i] and stop < 0:
                d[i] = slice(start - tail_index[i],
                             max(stop, -length - 1), step)
                # The offset accumulates over time from the start point
                offset = (offset + pos_step - (length % pos_step)) % pos_step
                start = tail_index[i] - 1 - length - offset

            stop += length

    return d


def partition_by_size(sizes, seq):
    """

    >>> partition_by_size([10, 20, 10], [1, 5, 9, 12, 29, 35])
    [[1, 5, 9], [2, 19], [5]]
    """
    seq = list(seq)
    pretotal = 0
    total = 0
    i = 0
    result = list()
    for s in sizes:
        total += s
        L = list()
        while i < len(seq) and seq[i] < total:
            L.append(seq[i] - pretotal)
            i += 1
        result.append(L)
        pretotal += s
    return result


def take(outname, inname, blockdims, index, axis=0):
    """ Index array with an iterable of index

    Mimics ``np.take``

    >>> take('y', 'x', [(20, 20, 20, 20)], [5, 1, 47, 3], axis=0)  # doctest: +SKIP
    {('y', 0): (getitem, (np.concatenate, [(getitem, ('x', 0), ([1, 3, 5],)),
                                           (getitem, ('x', 2), ([7],))],
                                          0),
                         (2, 0, 4, 1))}
    """
    n = len(blockdims)
    sizes = blockdims[axis]  # the blocksizes on the axis that we care about

    index_lists = partition_by_size(sizes, sorted(index))

    dims = [[0] if axis == i else list(range(len(bd)))
                for i, bd in enumerate(blockdims)]
    keys = list(product([outname], *dims))

    colon = slice(None, None, None)

    rev_index = tuple(map(sorted(index).index, index))
    vals = [(getitem, (np.concatenate,
                  (list, [(getitem, ((inname,) + d[:axis] + (i,) + d[axis+1:]),
                                   ((colon,)*axis + (IL,) + (colon,)*(n-axis-1)))
                                for i, IL in enumerate(index_lists)
                                if IL]),
                        axis),
                     ((colon,)*axis + (rev_index,) + (colon,)*(n-axis-1)))
            for d in product(*dims)]

    return dict(zip(keys, vals))


def posify_index(shape, ind):
    """

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
    if isinstance(ind, int):
        if ind < 0:
            return ind + shape
        else:
            return ind
    if isinstance(ind, list):
        return [posify_index(shape, i) for i in ind]
    return ind


slice_names = ('slice-%d' % i for i in count(1))


def slice_array(out_name, in_name, blockdims, index):
    """
    Master function for array slicing

    This function makes a new dask that slices blocks along every
    dimension and aggregates (via cartesian product) each dimension's
    slices so that the resulting block slices give the same results
    as the original slice on the original structure

    Parameters
    ----------

    in_name - string
      This is the dask variable name that will be used as input
    out_name - string
      This is the dask variable output name
    blockshape - iterable of integers
    index - iterable of integres, slices, or lists

    Returns
    -------

    Dict where the keys are tuples of

        (out_name, dim_index[, dim_index[, ...]])

    and the values are

        (function, (in_name, dim_index, dim_index, ...),
                   (slice(...), [slice()[,...]])

    Also new blockdims

    Example
    -------

    >>> slice_array('y', 'x', (100,), [(20, 20, 20, 20, 20)], (slice(10, 35),))  #  doctest: +SKIP
    {('y', 0): (getitem, ('x', 0), (slice(10, 20),)),
     ('y', 1): (getitem, ('x', 1), (slice( 0, 15),))}
    """
    index = tuple(index)
    blockdims = tuple(map(tuple, blockdims))

    if all(index == slice(None, None, None) for index in index):
        return {out_name: in_name}, blockdims

    missing = len(blockdims) - len([ind for ind in index if ind is not None])
    index2 = index + (slice(None, None, None),) * missing
    dsk_out, bd_out = slice_with_newaxes(out_name, in_name, blockdims, index2)

    bd_out = tuple(map(tuple, bd_out))
    return dsk_out, bd_out


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


def slice_with_newaxes(out_name, in_name, blockdims, index):
    """
    Handle indexing with Nones

    Strips out Nones then hands off to slice_wrap_lists
    """
    assert all(isinstance(ind, (slice, int, list, type(None)))
               for ind in index)

    # Strip Nones from index
    index2 = tuple([ind for ind in index if ind is not None])
    where_none = [i for i, ind in enumerate(index) if ind is None]

    # Pass down and do work
    dsk, blockdims2 = slice_wrap_lists(out_name, in_name, blockdims, index2)

    # Insert ",0" into the key:  ('x', 2, 3) -> ('x', 0, 2, 0, 3)
    dsk2 = dict(((out_name,) + insert_many(k[1:], where_none, 0),
                 (v[:2] + (insert_many(v[2], where_none, None),)))
                for k, v in dsk.items()
                if k[0] == out_name)

    # Add back intermediate parts of the dask that weren't the output
    dsk3 = merge(dsk2, dict((k, v) for k, v in dsk.items() if k[0] != out_name))

    # Insert (1,) into blockdims:  ((2, 2), (3, 3)) -> ((2, 2), (1,), (3, 3))
    blockdims3 = insert_many(blockdims2, where_none, (1,))

    return dsk3, blockdims3


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
    assert all(isinstance(i, (slice, list, int)) for i in index)

    index2 = posify_index(shape, index)

    # Do we have more than one list in the index?
    where_list = [i for i, ind in enumerate(index) if isinstance(ind, list)]
    if len(where_list) > 1:
        raise NotImplementedError("Don't yet support nd fancy indexing")

    # Turn x[5:10] into x[5:10, :, :] as needed
    num_missing_dims = len(shape) - len([i for i in index2 if i is not None])
    index3 = index2 + (slice(None, None, None),) * num_missing_dims

    index_without_list = tuple(slice(None, None, None)
                                    if isinstance(i, list)
                                    else i
                                    for i in index3)

    # No lists, hooray! just use slice_slices_and_integers
    if index3 == index_without_list:
        return slice_slices_and_integers(out_name, in_name, blockdims, index3)

    # lists and full slice/:   Just use take
    if all(isinstance(i, list) or i == slice(None, None, None)
            for i in index3):
        axis = where_list[0]
        dsk3 = take(out_name, in_name, blockdims, index3[where_list[0]],
                    axis=axis)
        blockdims2 = blockdims
    else:  # Mixed case. Both slices/integers and lists. slice/integer then take
        tmp = next(slice_names)
        dsk, blockdims2 = slice_slices_and_integers(tmp, in_name, blockdims, index_without_list)

        # After collapsing some axes due to int indices, adjust axis parameter
        axis = where_list[0]
        axis2 = axis - sum(1 for i, ind in enumerate(index3)
                           if i < axis and isinstance(ind, int))

        # Do work
        dsk2 = take(out_name, tmp, blockdims2, index3[axis], axis=axis2)
        dsk3 = merge(dsk, dsk2)

    # Replace blockdims of list entries with single block
    index4 = [ind for ind in index3 if not isinstance(ind, int)]
    blockdims3 = tuple([bd if not isinstance(i, list) else (len(i),)
                        for i, bd in zip(index4, blockdims2)])

    return dsk3, blockdims3


def slice_slices_and_integers(out_name, in_name, blockdims, index):
    """
    Dask array indexing with slices and integers

    Assumes that index is only slice or int objects

    See fancy_slice for full docstring

    Returns
    -------

    New dask and new blockdims
    """
    shape = tuple(map(sum, blockdims))

    assert all(isinstance(ind, (slice, int)) for ind in index)
    assert len(index) == len(blockdims)

    # Get a list (for each dimension) of dicts{blocknum: slice()}
    block_slices = list(map(_slice_1d, shape, blockdims, index))

    # (in_name, 1, 1, 2), (in_name, 1, 1, 4), (in_name, 2, 1, 2), ...
    in_names = product([in_name], *[i.keys() for i in block_slices])

    # (out_name, 0, 0, 0), (out_name, 0, 0, 1), (out_name, 0, 1, 0), ...
    out_names = product([out_name],
                        *[range(len(d))
                            for d, i in zip(block_slices, index)
                            if not isinstance(i, int)])

    all_slices = list(product(*[i.values() for i in block_slices]))

    dsk_out = dict((out_name, (getitem, in_name, slices))
                   for out_name, in_name, slices
                   in zip(out_names, in_names, all_slices))

    new_blockdims = [new_blockdim(d, db, i)
                     for d, i, db in zip(shape, index, blockdims)
                     if not isinstance(i, int)]

    return dsk_out, new_blockdims


class Array(object):
    """ Array object holding a dask

    Parameters
    ----------

    dask : dict
        Task dependency graph
    name : string
        Name of array in dask
    shape : tuple of ints
        Shape of the entire array
    blockdims : iterable of tuples
        block sizes along each dimension
    """

    __slots__ = 'dask', 'name', 'shape', 'blockdims'

    def __init__(self, dask, name, shape, blockshape=None, blockdims=None):
        self.dask = dask
        self.name = name
        self.shape = shape
        if blockshape is not None:
            blockdims = tuple((bd,) * (d // bd) + ((d % bd,) if d % bd else ())
                              for d, bd in zip(shape, blockshape))
        self.blockdims = tuple(map(tuple, blockdims))

    @property
    def numblocks(self):
        return tuple(map(len, self.blockdims))

    def _get_block(self, *args):
        return core.get(self.dask, (self.name,) + args)

    @property
    def ndim(self):
        return len(self.shape)

    def keys(self, *args):
        if self.ndim == 0:
            return [(self.name,)]
        ind = len(args)
        if ind + 1 == self.ndim:
            return [(self.name,) + args + (i,)
                        for i in range(self.numblocks[ind])]
        else:
            return [self.keys(*(args + (i,)))
                        for i in range(self.numblocks[ind])]

    def __array__(self, dtype=None, **kwargs):
        from .into import into
        x = into(np.ndarray, self)
        if dtype and x.dtype != dtype:
            x = x.astype(dtype)
        return x


def atop(func, out, out_ind, *args):
    """ Array object version of dask.array.top """
    arginds = list(partition(2, args)) # [x, ij, y, jk] -> [(x, ij), (y, jk)]
    numblocks = dict([(a.name, a.numblocks) for a, ind in arginds])
    argindsstr = list(concat([(a.name, ind) for a, ind in arginds]))

    dsk = top(func, out, out_ind, *argindsstr, numblocks=numblocks)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    shapes = dict((a, a.shape) for a, _ in arginds)
    dims = broadcast_dimensions(arginds, shapes)
    shape = tuple(dims[i] for i in out_ind)

    blockdim_dict = dict((a, a.blockdims) for a, _ in arginds)
    blockdimss = broadcast_dimensions(arginds, blockdim_dict)
    blockdims = tuple(blockdimss[i] for i in out_ind)

    dsks = [a.dask for a, _ in arginds]
    return Array(merge(dsk, *dsks), out, shape, blockdims=blockdims)


def get(dsk, keys, get=core.get, **kwargs):
    """ Specialized get function

    1. Handle inlining
    2. Use custom score function
    """
    fast_functions=kwargs.get('fast_functions',
                             set([getitem, np.transpose]))
    dsk2 = cull(dsk, list(core.flatten(keys)))
    dsk3 = inline_functions(dsk2, fast_functions=fast_functions)
    return get(dsk3, keys, **kwargs)


stacked_names = ('stack-%d' % i for i in count(1))


def stack(seq, axis=0):
    """
    Stack arrays along a new axis

    Given a sequence of dask Arrays form a new dask Array by stacking them
    along a new dimension (axis=0 by default)

    Example
    -------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [da.into(da.Array, np.ones((4, 4)), blockshape=(2, 2))
    ...          for i in range(3)]

    >>> x = da.stack(data, axis=0)
    >>> x.shape
    (3, 4, 4)

    >>> da.stack(data, axis=1).shape
    (4, 3, 4)

    >>> da.stack(data, axis=-1).shape
    (4, 4, 3)

    Result is a new dask Array

    See Also:
        concatenate
    """
    n = len(seq)
    ndim = len(seq[0].shape)
    if axis < 0:
        axis = ndim + axis + 1
    if axis > ndim:
        raise ValueError("Axis must not be greater than number of dimensions"
                "\nData has %d dimensions, but got axis=%d" % (ndim, axis))

    assert len(set(a.blockdims for a in seq)) == 1  # same blockshape
    shape = seq[0].shape[:axis] + (len(seq),) + seq[0].shape[axis:]
    blockdims = (  seq[0].blockdims[:axis]
                + ((1,) * n,)
                + seq[0].blockdims[axis:])

    name = next(stacked_names)
    keys = list(product([name], *[range(len(bd)) for bd in blockdims]))

    names = [a.name for a in seq]
    values = [(names[key[axis+1]],) + key[1:axis + 1] + key[axis + 2:]
                for key in keys]

    dsk = dict(zip(keys, values))
    dsk2 = merge(dsk, *[a.dask for a in seq])
    return Array(dsk2, name, shape, blockdims=blockdims)


concatenate_names = ('concatenate-%d' % i for i in count(1))


def concatenate(seq, axis=0):
    """
    Concatenate arrays along an existing axis

    Given a sequence of dask Arrays form a new dask Array by stacking them
    along an existing dimension (axis=0 by default)

    Example
    -------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [da.into(da.Array, np.ones((4, 4)), blockshape=(2, 2))
    ...          for i in range(3)]

    >>> x = da.concatenate(data, axis=0)
    >>> x.shape
    (12, 4)

    >>> da.concatenate(data, axis=1).shape
    (4, 12)

    Result is a new dask Array

    See Also:
        stack
    """
    n = len(seq)
    ndim = len(seq[0].shape)
    if axis < 0:
        axis = ndim + axis
    if axis >= ndim:
        raise ValueError("Axis must be less than than number of dimensions"
                "\nData has %d dimensions, but got axis=%d" % (ndim, axis))

    bds = [a.blockdims for a in seq]

    if not all(len(set(bds[i][j] for i in range(n))) == 1
            for j in range(len(bds[0])) if j != axis):
        raise ValueError("Block shapes do not align")

    shape = (seq[0].shape[:axis]
            + (sum(a.shape[axis] for a in seq),)
            + seq[0].shape[axis + 1:])
    blockdims = (  seq[0].blockdims[:axis]
                + (sum([bd[axis] for bd in bds], ()),)
                + seq[0].blockdims[axis + 1:])

    name = next(concatenate_names)
    keys = list(product([name], *[range(len(bd)) for bd in blockdims]))

    cum_dims = [0] + list(accumulate(add, [len(a.blockdims[axis]) for a in seq]))
    names = [a.name for a in seq]
    values = [(names[bisect(cum_dims, key[axis + 1]) - 1],)
                + key[1:axis + 1]
                + (key[axis + 1] - cum_dims[bisect(cum_dims, key[axis+1]) - 1],)
                + key[axis + 2:]
                for key in keys]

    dsk = dict(zip(keys, values))
    dsk2 = merge(dsk, *[a.dask for a in seq])

    return Array(dsk2, name, shape, blockdims=blockdims)

import numpy as np
from math import ceil
import itertools
from collections import Iterator
from functools import partial
from toolz.curried import (identity, pipe, partition, concat, unique, pluck,
        frequencies, join, first, memoize, map)
import operator


def ndslice(x, blocksize, *args):
    """ Get a block from an nd-array

    >>> x = np.arange(24).reshape((4, 6))
    >>> ndslice(x, (2, 3), 0, 0)
    array([[0, 1, 2],
           [6, 7, 8]])

    >>> ndslice(x, (2, 3), 1, 0)
    array([[12, 13, 14],
           [18, 19, 20]])
    """
    return x.__getitem__(tuple([slice(i*n, (i+1)*n)
                            for i, n in zip(args, blocksize)]))


def getem(arr, blocksize, shape):
    """ Dask getting various chunks from an array-like

    >>> getem('X', blocksize=(2, 3), shape=(4, 6))  # doctest: +SKIP
    {('X', 0, 0): (ndslice, 'X', (2, 3), 0, 0),
     ('X', 1, 0): (ndslice, 'X', (2, 3), 1, 0),
     ('X', 1, 1): (ndslice, 'X', (2, 3), 1, 1),
     ('X', 0, 1): (ndslice, 'X', (2, 3), 0, 1)}
    """
    numblocks = tuple([int(ceil(n/k)) for n, k in zip(shape, blocksize)])
    return dict(((arr,) + tup, (ndslice, arr, blocksize) + tup)
            for tup in itertools.product(*map(range, numblocks)))


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


def top(func, output, out_indices, *arrind_pairs, **kwargs):
    """ Tensor operation

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
    """
    numblocks = kwargs['numblocks']
    argpairs = list(partition(2, arrind_pairs))

    assert set(numblocks) == set(pluck(0, argpairs))

    all_indices = pipe(argpairs, pluck(1), concat, set)
    dummy_indices = all_indices - set(out_indices)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    dims = dict(concat([zip(inds, dims)
                        for (x, inds), (x, dims)
                        in join(first, argpairs, first, numblocks.items())]))

    # (0, 0), (0, 1), (0, 2), (1, 0), ...
    keytups = list(itertools.product(*[range(dims[i]) for i in out_indices]))
    # {i: 0, j: 0}, {i: 0, j: 1}, ...
    keydicts = [dict(zip(out_indices, tup)) for tup in keytups]

    # {j: [1, 2, 3], ...}  For j a dummy index of dimension 3
    dummies = dict((i, list(range(dims[i]))) for i in dummy_indices)

    # Create argument lists
    valtups = []
    for kd in keydicts:
        args = []
        for arg, ind in argpairs:
            args.append(lol_tuples((arg,), ind, kd, dummies))
        valtups.append(tuple(args))

    # Add heads to tuples
    keys = [(output,) + kt for kt in keytups]
    vals = [(func,) + vt for vt in valtups]

    return dict(zip(keys, vals))


def concatenate2(arrays, axes=[]):
    """ Recursively Concatenate nested lists of arrays along axes

    Each entry in axes corresponds to each level of the nested list.  The
    length of axes should correspond to the level of nesting of arrays.

    >>> x = np.array([[1, 2], [3, 4]])
    >>> concatenate2([x, x], axes=[0])
    array([[1, 2],
           [3, 4],
           [1, 2],
           [3, 4]])

    >>> concatenate2([x, x], axes=[1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4]])

    >>> concatenate2([[x, x], [x, x]], axes=[0, 1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4],
           [1, 2, 1, 2],
           [3, 4, 3, 4]])

    Supports Iterators
    >>> concatenate2(iter([x, x]), axes=[1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4]])
    """
    if isinstance(arrays, Iterator):
        arrays = list(arrays)
    if len(axes) > 1:
        arrays = [concatenate2(a, axes=axes[1:]) for a in arrays]
    return np.concatenate(arrays, axis=axes[0])
    if len(axes) == 1:
        return np.concatenate(arrays, axis=axes[0])
    else:
        return np.concatenate


def concatenate(arrays, axis=0):
    """

    >>> x = np.array([1, 2])
    >>> concatenate([[x, x], [x, x], [x, x]])
    array([[1, 2, 1, 2],
           [1, 2, 1, 2],
           [1, 2, 1, 2]])
    """
    if isinstance(arrays, Iterator):
        arrays = list(arrays)
    if isinstance(arrays[0], Iterator):
        arrays = list(map(list, arrays))
    if not isinstance(arrays[0], np.ndarray):
        arrays = [concatenate(a, axis=axis + 1) for a in arrays]
    if arrays[0].ndim <= axis:
        arrays = [a[None, ...] for a in arrays]
    return np.concatenate(arrays, axis=axis)


def dask_1d_slice(out_name, in_name, slice_spec, shape, blockshape):
    """
    slice_spec - the full description of the python slice we want.
    shape - the complete shape of the data that in_name describes
    blockshape - the maximum size of each block

     - see if our slice can be met checking against the shape
       - one end goes past the shape? i.e. slice(0, 100) on a shape of (10,)
       - stride/step? On this first pass, I'm going to ignore the stride.
       - negative index?
    - Should the out_name index match the in_name index assuming that the first
        slice is not 0?
      i.e. should we return
        {(y,0):(get, (x,1), slice(3,5))}
      or
        {(y,1):(get, (x,1), slice(3,5))}

    - Can we always assume that block (x,1) is the second block? yes, per conversation with @mrocklin.
    - We also assume that the first slice is (out_name, 0), even if we
        slice from an inner block like (in_name, 3)
    - How do we handle a case where the slice size is greater than
        the block size?
                       
    """

    #Step 0: Setup all the variables we will need for the actual slicing
    #  calculations. This includes the proper starting and stopping
    #  indexes and step size. Remember that all of these can be negative,
    #  and all of them can be None
    start = slice_spec.start
    stop = slice_spec.stop
    step = slice_spec.step
    
    if start < 0:
        start = shape[0]+start
    if stop < 0:
        stop = shape[0]+stop

    if start is None:
        start = 0
    if stop is None:
        stop = shape[0]
    if step is None:
        step = 1

    if start >= shape[0] or start == stop:
        return {}
        
    if stop > shape[0]:
        stop = shape[0]
                        

    #Step 1: All of our input variables should now be setup correctly,
    #  so figure out where to start and stop based on the blocksize
    #  of the input data
    leftmost_block = start/blockshape[0]
    #We subtract 1 from stop so that if the rightmost index
    #falls on a block boundary, we won't include a slice in the next block
    #  i.e. slice(0,20) on blockshape(20,) should give us
    #  (x,0), slice(0,20)
    #  NOT
    #  ((x,0), slice(0,20)), ((x,1), slice(0,0))
    rightmost_block = (stop-1)/blockshape[0]
    leftmost_block_start_index = start % blockshape[0]
    rightmost_block_stop_index = stop - (rightmost_block*blockshape[0])
    num_blocks = rightmost_block - leftmost_block

    
        
    dask = {}
    #There are three main cases to account for when doing the slicing
    #  within a dask graph
    #1. slicing within a single block
    #2. slicing with only adjacent blocks, i.e. blocks 1,2
    #3. slicing with multiple contiguous blocks, i.e. block 1,2,3,4
    if leftmost_block == rightmost_block:    
        dask[(out_name, 0)] = (operator.getitem, (in_name, leftmost_block), slice(leftmost_block_start_index, rightmost_block_stop_index))
    else:
        dask[(out_name, 0)] = (operator.getitem, (in_name, leftmost_block), slice(leftmost_block_start_index, blockshape[0]))
        dask[(out_name, num_blocks)] = (operator.getitem, (in_name, rightmost_block), slice(0, rightmost_block_stop_index))
            
    for block_num in range(1, num_blocks):
        dask[(out_name, block_num)] = (operator.getitem, (in_name, leftmost_block+block_num), slice(0, blockshape[0]))

    return dask



def _dask_slice(in_name, out_name, leftmost_blocks, rightmost_blocks,
                leftmost_block_indexes, rightmost_block_indexes,
                blockshape):
    """
    Variables
    ----
    leftmost_blocks - list of block indexes
    rightmost_blocks - list of block indexes
    

    Assumptions
    ----
    1. All of the block and slice indexes are already correct.
    2. Right rightmost indexes are exclusive like in normal python slicing. i.e. left_index=1, right_index=4 is logically equivalent to list[1:4]
    """
    ndims = len(leftmost_blocks)
    assert len(leftmost_blocks) == len(rightmost_blocks) == \
      len(leftmost_block_indexes) == len(rightmost_block_indexes)

    #per_dim_count holds the number of blocks that each dimension
    #  will need to slice. This ends up being a cartesian product
    per_dim_count = []
    for dim in range(ndims):
        per_dim_count.append(rightmost_blocks[dim] - leftmost_blocks[dim])

    #{('y', 0, 0, 0):None, ('y', 0, 0, 1):None, ('y', 1, 0, 0):None, ('y', 1, 0, 1):None}
    dask = {key: None for key in
            itertools.product(*[out_name]+[range(i) for i in per_dim_count])
            }
              
    for key in dask:
        #our output keys start at (0,0,0)
        #our input blocks start wherever leftmost_blocks says
        #So, we need to use the output keys as offsets into leftmost_blocks
        #That is why we add offsets[idx] and leftmost_blocks[idx]
        offsets = key[1:]
        in_blocks = [offsets[idx] + leftmost_blocks[idx] for idx in range(ndims)]
        final_slice = [operator.getitem, tuple([in_name] + in_blocks)]
        #At this point, all we have to do is construct the slice() commands
        #
        dim_slices = []
        for dim in range(ndims):
            lblock = leftmost_blocks[dim]
            rblock = rightmost_blocks[dim]
            lindex = leftmost_block_indexes[dim]
            rindex = rightmost_block_indexes[dim]

            #if we make it here, each dimension must have at least 1 member

            #Remember that the rightmost blocks and indexes are exclusive
            # (i.e. only block-1 and index-1 are actually included in the output)
            if lblock == rblock-1:
                dim_slices.append(slice(lindex, rindex))
            elif offsets[dim] == 0:
                dim_slices.append(slice(lindex, blockshape[dim]))
            elif offsets[dim]+lblock == rblock-1:
                dim_slices.append(slice(0, rindex))
            else:
                dim_slices.append(slice(0, blockshape[dim]))

        final_slice.append(tuple(dim_slices))
        dask[key] = tuple(final_slice)


    return dask


        

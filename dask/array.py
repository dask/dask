import numpy as np
from math import ceil
from itertools import product
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
            for tup in product(*map(range, numblocks)))


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


def _slice_1d(dim_shape, blocksize, index):
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
    a dictionary where the keys are the integer indexes of the blocks that
      should be sliced and the values are the slices

    Tests
    -----
    >>> _slice_1d(100, 20, slice(0, 35))
    {0: slice(0, 20, None), 1: slice(0, 15, None)}
    >>> _slice_1d(100, 20, slice(10,15))
    {0: slice(10, 15, None)}
    >>> _slice_1d(100, 20, slice(40, 100))
    {2: slice(0, 20, None), 3: slice(0, 20, None), 4: slice(0, 20, None)}
    >>> _slice_1d(100, 20, slice(0, 100, 40)) #step > blocksize
    {0: slice(0, 20, 40), 2: (0, 20, 40), 4: (0, 20, 40)}
    >>> _slice_1d(100, 20, 5)
    {0: 5}
    >>> _slice_1d(100, 20, 57)
    {2: 17}
    """
    #integer division often won't tell us how many blocks
    #  we have.
    num_blocks = int(ceil(float(dim_shape)/blocksize))
    
    if index == Ellipsis or index == slice(None, None, None):
        return {i: index for i in range(num_blocks)}

    if isinstance(index, int):
        #integer math here is ok.
        if abs(index) >= dim_shape:
            raise IndexError("index %s is out of bounds for shape %s" % (
                index, dim_shape))
        return {index/blocksize : index % blocksize}

    if isinstance(index, slice):
        #start, stop, and step == None are valid for a slice, 
        #  but not for our calculations.
        start = index.start or 0
        stop = index.stop or dim_shape
        step = index.step or 1
        start_block = start/blocksize
        stop_block = int(ceil(stop/blocksize))+1

        res = [_block_slice_step_start(blocksize, block, start, stop, step) \
               for block in range(0, stop_block)]
                    
        #res[block] will be None if the block doesn't contribute anything
        return {block: res[block] for block in range(start_block, stop_block) \
                if res[block] is not None}


def _block_slice_step_start(blocksize, blocknum, start, stop, step):
    """Return the slice for a single block

    This function determines the relative slice in a single block
    given the absolute positions of start, stop, and step. Each block
    element has an absolute index and a relative index. The absolute
    index is the element's position in the larger/conjoined structure.
    The relative index is the element's position in the block.

    Parameters
    ----------
    blocksize : integer
      Denotes the size of the current block
    blocknum : integer
      Denotes the index of the block (block indexing starts from 0)
      Example:
        - blocksize = 25
        - blocknum = 0
        This implies that this block (0) holds block indexes [0:25]
        and absolute indexes [0:25]

        - blocksize = 25
        - blocknum = 3
        This implies that this block (3) holds block indexes [0:25]
        and absolute indexes [75:100]
        
    start : integer
      Denotes the absolute starting index (inclusive) of the slice
      start is NOT the start within this block
      start IS the start index for the entire (aka non-blocked) slice
    stop : integer
      Denotes the absolute stopping index (exclusive) of the slice
      stop is NOT the stop index within this block
      stop IS the stop index for the entire slice
    step : An integer or None
      Denotes the step size of the slice


    Returns
    -------
    - None if start falls after the highest absolute index in the block
    - None if stop falls before the lowest absolute index in the block
    - None if the step size is so large that the current block won't
      contribute any elements
    - An integer index if the step size is large enough that the current
      block only contributes a single element (in some cases)
    - a slice() containing the correct start and stop indexes into the
      block plus the step if any
    
    Notes
    -----
    - if step > blocksize, we are doing basic indexing into the blocks
      and may end up skipping a block
    - negative stepping is UNHANDLED as of 2015-01-16

    Tests
    -----
    >>> _block_slice_step_start(20, 0, 10, 20, None)
    slice(10, 20, None)
    >>> #The starting index falls outside the block, so it returns None
    >>> _block_slice_step_start(20, 0, 20, 50, None)
    >>> #The starting index falls in block 0, so this block's slice
    >>> #starts at index 0
    >>> _block_slice_step_start(20, 1, 10, 50, None)
    slice(0, 20, None)
    >>> #The stopping index falls before block 1 so it returns None
    >>> _block_slice_step_start(20, 1, 10, 20, None)
    >>> _block_slice_step_start(20, 0, 3, 100, 25)
    slice(3, 20, 25)
    >>> _block_slice_step_start(20, 1, 3, 100, 25)
    slice(8, 20, 25)
    >>> #Because we start at index 10, the next block's slice starts at
    >>> #index 2 (aka absolute index 22 == 10 + 12)
    >>> _block_slice_step_start(20, 1, 10, 40, 3)
    slice(2, 20, 3)
        
    """
    if start >= blocksize*(blocknum+1):
        #The starting index falls after the end of the given block
        return None

    if stop <= blocksize*(blocknum):
        #the stopping index falls before the start of the given block
        return None

    #if the starting index happens to be in the current block,
    #  the slice we return will need to use that index
    #  for the slice.start index
    if _index_in_block(start, blocknum, blocksize):
        block_start_index = start % blocksize
    else:
        block_start_index = _first_step_in_block(step, start,
                                                 blocksize, blocknum)
        #If the block_start_index is less than 0,
        #  that means that this block
        #  doesn't have any elements selected
        if block_start_index < 0:
            return None
        
    if block_start_index >= blocksize:
        #The starting index into the block falls outside the
        #  current block's end
        return None
    else:
        if _index_in_block(stop, blocknum, blocksize):
            #If the stopping index falls inside this block, our
            #  returned slice should reflect that stoppage
            return slice(block_start_index, stop % blocksize, step)
        else:
            #otherwise, the slice should go to the end of the block
            return slice(block_start_index, blocksize, step)


def _first_step_in_block(step, start, blocksize, blocknum):
    """Returns the blockindex of the first contributing element

    This function determines where in the given block the first
    contributing element is. If the block doesn't contribute
    an element, -1 is returned  
    """
    if start >= blocksize*(blocknum+1):
        return -1
    return step - 1 - ((blocknum*blocksize) - start - 1) % step


def _index_in_block(index, blocknum, blocksize):
    """
    Parameters
    ----------
    index : integer
      The absolute index into the iterable
    blocknum : integer
      Denotes which block we are examining
    blocksize: integer
      Denotes how large each block is (how many elements each block has)

    Returns
    -------
    A boolean telling if index falls in the block given by blocknum

    >>> _index_in_block(0, 0, 100)
    True
    >>> _index_in_block(0, 1, 100)
    False
    >>> _index_in_block(25, 0, 25)
    False
    >>> _index_in_block(25, 1, 25)
    True
    """
    return blocksize*blocknum <= index < blocksize*(blocknum+1)


def dask_slice(out_name, in_name, shape, blockshape, indexes,
               getitem_func=operator.getitem):
    """Returns a new dask containing the slices for all n-dimensions

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
    shape - iterable of integers (only tested with tuples of integers)
      The shape of the 
    blockshape - iterable of integers
    indexes - iterable of indexes, ellipses, or slices
    getitem_func - the function that will be used to get the
      items from the block. This is required by dask.
    
    Returns
    -------
    a dict where the keys are tuples of
    (out_name, dim_index[, dim_index[, ...]])
    and the values are 
    (function, (in_name, dim_index[, dim_index[, ...]]),
               (slice(),[slice()[,...]])
        
    Tests
    -----
    See tests/test_slicing.py
    """

    #Quick Optimization
    #If we are only given full slices, simply return the input variable
    #i.e. input_data[:,:,:] becomes
    #(slice(None,None,None), slice(None,None,None),
    # slice(None,None,None))
    #In this case, we shouldn't do any slicing.
    empty = slice(None, None, None)
    if all(index == empty for index in indexes):
        return {out_name: in_name}
    
    #Get a list (for each dimension) of dicts{blocknum: slice()}
    #  for each dimension
    block_slices = list(map(_slice_1d, shape, blockshape, indexes))

    #out_names has the cartesion product of output block index locations
    #i.e. (out_name, 0, 0, 0), (out_name, 0, 0, 1), (out_name, 0, 1, 0)
    out_names = product([out_name],
                        *[range(len(d)) for d in block_slices])

    #in_names holds the cartesion product of input block index locations
    #i.e. (in_name, 1, 1, 2), (in_name, 1, 1, 4), (in_name, 2, 1, 2)
    in_names = product([in_name], *[i.keys() for i in block_slices])

    #all_slices holds the slices needed to generate
    #(out_name, 0, 0, 0) from (in_name, 1, 1, 2)
    #There should be 1 slice per dimension index
    all_slices = product(*[i.values() for i in block_slices])

    final_out = {out_name:(getitem_func, in_name, slices) for
                 out_name, in_name, slices in zip(out_names, in_names,
                                                  all_slices)}
    
    return final_out


from __future__ import absolute_import, division, print_function
""" 
The reblock module defines:
    intersect_blockdims: a function for 
        converting blockdims to new dimensions
    reblock: a function to convert the blocks 
        of an existing dask array to new blockdims or blockshape
"""
from itertools import count, product, chain
from operator import getitem, add
import numpy as np
from toolz import merge, accumulate
from dask.array.core import rec_concatenate, Array
from dask.array.core import blockdims_from_blockshape

reblock_names  = ('reblock-%d' % i for i in count(1))
def cumdims_label(blockdims, const):
    """ cumdims_label(blockdims, const)
    Interal utility for cumulative sum with label.
    >>> cumdims_label(((5,3,3),(2,2,1)),'n')
        [(('n', 0), ('n', 5), ('n', 8), ('n', 11)),
         (('n', 0), ('n', 2), ('n', 4), ('n', 5))]
    """
    return [tuple(zip((const,) * (1+ len(bds)), list(accumulate(add, (0,) + bds)))) for bds in blockdims ]


def _breakpoints(cumold, cumnew):
    """
    >>> new = cumdims_label(((5,3,3),(2,2,1)),'n')
    >>> old = cumdims_label(((2,2,1),(5,)),'o')
    >>> _breakpoints(c[1],old[1])
    (('n', 0), ('o', 0), ('n', 2), ('n', 4), ('n', 5), ('o', 5))
    >>> _breakpoints(c[0],old[0])
        (('n', 0),('o', 0),('o', 2),('o', 4),('n', 5),('o', 5),('n', 8),('n', 11))
    """
    return tuple(sorted(tuple(cumold) + tuple(cumnew), key=lambda x:x[1]))
    

def _intersect_1d(breaks):
    """Internal utility to intersect blockdims for 1 d
    after preprocessing.

    >>> new = cumdims_label(((5,3,3),(2,2,1)),'n')
    >>> old = cumdims_label(((2,2,1),(5,)),'o')
    >>> _intersect_1d(_breakpoints(old[0],new[0])
        (((0, slice(0, 2, None)), (1, slice(0, 2, None)), (2, slice(0, 1, None))),
         ((2, slice(0, 3, None)),),
         ((2, slice(3, 6, None)),))
    >>> _intersect_1d(_breakpoints(old[1],new[1])

        (((0, slice(0, 2, None)),),
         ((0, slice(2, 4, None)),),
         ((0, slice(4, 5, None)),))

    Parameters:
        breaks: list of tuples
            Each tuple is ('o', 8) or ('n', 8)
            These are pairs of 'o' old or new 'n'
            indicator with a corresponding cumulative sum.
        summ: int
            The shape[dimension_being_passed]

    Uses 'o' and 'n' to make new tuples of slices for 
    the new block crosswalk to old blocks.
    """
    start = 0
    last_end = 0
    old_idx = 0
    lastbi = ('n',0)
    ret = [[]]
    for idx in range(1, len(breaks)):
        bi = breaks[idx]
        lastbi = breaks[idx -1]
        if 'n' in lastbi[0] and bi[1]:
            ret.append([])
        if 'o' in lastbi[0]:
            start = 0
        else:
            start = last_end
        end = bi[1] - lastbi[1] + start
        last_end = end
        if bi[1] == lastbi[1]:
            continue
        ret[-1].append((old_idx, slice(start, end)))
        if bi[0] == 'o':
            old_idx += 1
            start = 0
    return tuple(map(tuple, filter(None, ret)))
    
def intersect_blockdims(old_blockdims=None, 
                        new_blockdims=None,
                        shape=None,
                        old_blockshape=None,
                        new_blockshape=None):
    """ 
    Make dask.array slices as intersection of old and new blockdims.

    >>> intersect_blockdims(((4,4),(2,)), ((8,), (1,1)))
        (((((0, slice(0, 4, None)), (1, slice(0, 4, None))),),
          (((0, slice(0, 1, None)),), ((0, slice(1, 2, None)),))),
         ((((0, slice(0, 4, None)), (0, slice(0, 1, None))),
           ((1, slice(0, 4, None)), (0, slice(0, 1, None)))),
          (((0, slice(0, 4, None)), (0, slice(1, 2, None))),
           ((1, slice(0, 4, None)), (0, slice(1, 2, None))))))
    
    Parameters:

    old_blockdims : iterable of tuples
        block sizes along each dimension (convert from old_blockdims)
    new_blockdims: iterable of tuples
        block sizes along each dimension (converts to new_blockdims) 
    shape : tuple of ints
        Shape of the entire array (not needed if using blockdims)
    old_blockshape: size of each old block as tuple 
        (converts from this old_blockshape)
    new_blockshape: size of each new block as tuple 
        (converts to this old_blockshape)

    Note: shape is only required when using old_blockshape or new_blockshape.
    """
    
    if not old_blockdims:
        old_blockdims = blockdims_from_blockshape(shape, old_blockshape)
    if not new_blockdims:
        new_blockdims = blockdims_from_blockshape(shape, new_blockshape)    
    global zipped, old_to_new, cross1,cross
    cmo = cumdims_label(old_blockdims,'o')
    cmn = cumdims_label(new_blockdims,'n')
    sums = [sum(o) for o in old_blockdims]
    sums2 = [sum(n) for n in old_blockdims]
    if not sums == sums2:
        raise ValueError('Cannot change dimensions from to %r' % sums2)
    zipped = zip(old_blockdims,new_blockdims)
    old_to_new =  tuple(_intersect_1d(_breakpoints(cm[0],cm[1])) for cm in zip(cmo, cmn))
    cross1 = tuple(product(*old_to_new))
    cross = tuple(chain(tuple(product(*cr)) for cr in cross1))
    return cross


def reblock(x, blockdims=None, blockshape=None ):
    """
    Convert blocks in dask array x for new blockdims.

    reblock(x, blockdims=None, blockshape=None )
    
    >>> import dask.array as da 
    >>> old_blockdims = ((2,3,2),)*4
    >>> new = ((2,4,1), (4, 2, 1), (4, 3), (7,))
    >>> a = np.random.uniform(0, 1, 7**4).reshape((7,) * 4)
    >>> x = da.from_array(a, blockdims=old)
    >>> x.blockdims
    ((2,4,1), (4, 2, 1), (4, 3), (7,))

    Parameters:

    x:   dask array
    blockdims:  the new block dimensions to create
    blockshape: the new blockshape to create

    Provide one of blockdims or blockshape.
    """
    
    if not blockdims:
        blockdims = blockdims_from_blockshape(x.shape, blockshape)
    crossed = intersect_blockdims(x.blockdims, blockdims)
    x2 = dict()
    temp_name = next(reblock_names)
    new_index = tuple(product(*(tuple(range(len(n))) for n in blockdims)))
    for flat_idx, cross1 in enumerate(crossed):
        new_idx = new_index[flat_idx]
        key = (temp_name,) + new_idx
        cr2 = iter(cross1)
        old_blocks = tuple(tuple(ind  for ind,_ in cr) for cr in cross1)
        subdims = tuple(len(set(ss[i] for ss in old_blocks)) for i in range(x.ndim))
        rec_cat_arg =np.empty(subdims).tolist()
        inds_in_block = product(*(range(s) for s in subdims))
        for old_block in old_blocks:
            ind_slics = next(cr2)
            old_inds = tuple(tuple(s[0] for s in ind_slics) for i in range(x.ndim))
            # list of nd slices
            slic = tuple(tuple(s[1] for s in ind_slics)  for i in range(x.ndim))
            ind_in_blk = next(inds_in_block)
            temp = rec_cat_arg
            for i in range(x.ndim -1):  
                temp = getitem(temp, ind_in_blk[i])
            for ind, slc in zip(old_inds, slic):
                temp[ind_in_blk[-1]] = (getitem, (x.name,) + ind, slc)
        x2[key] = (rec_concatenate, rec_cat_arg)
    x2 = merge(x.dask, x2)
    return Array(x2, temp_name, blockdims=blockdims)

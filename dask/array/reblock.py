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
from toolz import merge
from dask.array.core import rec_concatenate, Array
from dask.array.core import blockdims_from_blockshape

reblock_names  = ('reblock-%d' % i for i in count(1))

def intersect_blockdims_1d(old, new):
	summ = sum(old)
	if summ != sum(new):
		raise ValueError('Cannot change size from %r to %r'%(summ, sum(new)))
	od = enumerate(old)
	nw = iter(new)
	ret = []
	sum_old = 0
	sum_new = 0
	while True:
		while sum_new <= sum_old:
			try:
				n = next(nw)
				if not ret or ret[-1]:
					ret.append([])
			except StopIteration:
				break
			sum_new += n
			if sum_new - n < sum_old and sum_new >= sum_old:
				start = o -leftover
				endd = o
				adding = (cnt, slice(start, endd))
				ret[-1].append(adding)
		leftover = 0
		while sum_old < sum_new:
			try:
				cnt, o = next(od)
			except StopIteration:
				break
			sum_old += o
			if sum_old > sum_new and sum_old - o < sum_new:
				leftover =  (sum_old - sum_new)
				endd = o - leftover
				start = 0
			elif sum_old <= sum_new:
				start = leftover
				endd = o
			elif sum_old < sum_new:
				start = 0
				endd = o
			else:
				continue
			adding = (cnt, slice(start, endd))
			ret[-1].append(adding)
		if sum_old == summ and sum_new == summ:
			break
	return tuple(map(tuple, ret))


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
	zipped = zip(old_blockdims,new_blockdims)
	old_to_new =  tuple(intersect_blockdims_1d(o, n) for o,n in zipped)
	cross = tuple(product(*old_to_new))
	cross = tuple(chain(tuple(product(*cr)) for cr in cross))
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
	xshape = tuple(map(sum, x.blockdims))
	if not blockdims:
		blockdims = blockdims_from_blockshape(xshape, blockshape)
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
	return Array(x2, temp_name, blockdims = blockdims)

from __future__ import absolute_import, division, print_function
from itertools import count, product, chain, accumulate
from operator import getitem, add, mul, ifloordiv
import numpy as np
from toolz import merge, reduce
from dask.utils import ignoring
from dask.array.core import rec_concatenate, Array

reblock_names  = ('reblock-%d' % i for i in count(1))

def intersect_blockdims_1d(old, new):
	summ = sum(old)
	if summ != sum(new):
		raise ValueError('sum(old) != sum(new)')
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

def intersect_blockdims(old, new):
	old_to_new =  tuple(intersect_blockdims_1d(o, n) for o,n in zip(old,new))
	cross = tuple(product(*old_to_new))
	cross = tuple(chain(tuple(product(*cr)) for cr in cross))
	return (old_to_new, cross)


def reblock(dsk, blockdims):
	"""
	Convert blocks in dsk for new blockdims.
	"""
	(old_to_new, crossed) = intersect_blockdims(dsk.blockdims, blockdims)
	dsk2 = dict()
	temp_name = next(reblock_names)
	new_index = tuple(product(*(tuple(range(len(n))) for n in blockdims)))
	for flat_idx, cross1 in enumerate(crossed):
		new_idx = new_index[flat_idx]
		key = (temp_name,) + new_idx
		cr2 = iter(cross1)
		old_blocks = tuple(tuple(ind  for ind,_ in cr) for cr in cross1)
		subdims = tuple(len(set(ss[i] for ss in old_blocks)) for i in range(dsk.ndim))
		# rec_cat_arg following is to allocate an ndarray of lists
		# as a template for the argument to rec_concatenate
		rec_cat_arg =np.empty(subdims).tolist()
		inds_in_block = product(*(range(s) for s in subdims))
		for old_block in old_blocks:
			ind_slics = next(cr2)
			old_inds = tuple(tuple(s[0] for s in ind_slics) for i in range(dsk.ndim))
			# list of nd slices
			slic = tuple(tuple(s[1] for s in ind_slics)  for i in range(dsk.ndim))
			ind_in_blk = next(inds_in_block)
			temp = rec_cat_arg
			for i in range(dsk.ndim -1):  
				# getitem up to the inner most list in ND array of lists
				temp = getitem(temp, ind_in_blk[i])
			for ind, slc in zip(old_inds, slic):
				# set item there with rec cat slices
				temp[ind_in_blk[-1]] = (getitem, (dsk.name,) + ind, slc)
		dsk2[key] = (rec_concatenate, rec_cat_arg)
	dsk2 = merge(dsk.dask, dsk2)
	return Array(dsk2, temp_name, blockdims = blockdims)

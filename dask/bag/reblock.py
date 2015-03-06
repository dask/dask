<<<<<<< HEAD
import unittest
import numpy as np
from dask.utils import ignoring
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
		print('so,sn', sum_old, sum_new, ret)	
		if sum_old == summ and sum_new == summ:
			break
	return tuple(map(tuple, ret))

class TestIntersectBlockDims(unittest.TestCase):
	#TODO obviously move this...
	def test_1(self):
		""" Example from Matthew's docs"""
		old=(10, 10, 10, 10, 10)
		new = (25, 5, 20)
		answer = (((0, slice(0, 10, None)), 
					(1, slice(0, 10, None)), 
					(2, slice(0, 5, None))), 
					((2, slice(5, 10, None)),),
					 ((3, slice(0, 10, None)), 
					 	(4, slice(0, 10, None))))
		got = intersect_blockdims(old, new)
		self.assertEqual(answer, got)
	def test_2(self):
		
		old = (20, 20, 20, 20, 20)
		new = (58, 4, 20, 18)
		answer = (((0, slice(0, 20, None)), (1, slice(0, 20, None)), (2, slice(0, 18, None))),
			 ((2, slice(18, 20, None)), (3, slice(0, 2, None))),
			 ((3, slice(2, 20, None)), (4, slice(0, 2, None))),
			 ((4, slice(2, 20, None)),))
		got = intersect_blockdims(old, new)
		self.assertEqual(answer, got)
def intersect_blockdims(old, new):
	return tuple(intersect_blockdims_1d(o, n) for o,n in zip(old,new))




if __name__ == "__main__":
	unittest.main()
=======
from __future__ import absolute_import, division, print_function
import itertools
from operator import add
import numpy as np
from dask.utils import ignoring
from toolz import accumulate
def cumdims(blockdims):
    return [list(accumulate(add, (0,) + bds)) for bds in blockdims]

def intersect_blockdims(old, new):
    oc, nc = map(cumdims, (old, new))
    new_cum = tuple(tuple(zip((0,) * len(nci),  nci,(0,) + nw,  tuple(range(len(nw))))) for nci, nw in zip(nc, new))
    old_cum = tuple(tuple(zip((1,) * len(oci),  oci,(0,) + od, (None,) + tuple(range(len(od))))) for oci, od in zip(oc, old))
    breaks = tuple(sorted(oci + nci, key = lambda x: (x[1], x[0])) for oci, nci in zip(old_cum, new_cum))
   
    old_inds = [ [0] + [b[idx][1] - b[idx-1][1] for idx in range(1,len(b))] for b in breaks]
    inds = tuple(tuple( b + (o,) for b, o in zip(breaks[idx],old_inds[idx])) for idx in range(len(breaks)))
    final = []
    # clean up all these naming messes
    for ind in inds:
        old_idx = 0
        idx_new_old2 = 0
        ret = [[]]
        for (is_old, cumm, old_chunk, idx_new_old, length) in ind:
            if idx_new_old2 != idx_new_old:
                old_idx = 0
            if is_old:
                idx_new_old2 = idx_new_old
            if length:
                ret[-1].append((idx_new_old2, slice(old_idx, old_idx + length)))
            old_idx = old_idx + length
            if not is_old:
                ret.append([])
        final.append(tuple(filter(None, map(tuple, ret))))
    return tuple(final)
    
def reblock(dsk, old_to_new, block_id):
    getting = dict()
    dsk2 = dict()
    layer1 = map(itertools.product, itertools.product(*old_to_new))
    # do more stuff here...
    return layer1
if __name__ == "__main__":
    import pprint
    old = ((20,20,20),(10,)*2)
    new = ((15,)*2, (15,5))
    print('With old = ', old, 'new = ', new,'intersect_blockdims = ')
    i = intersect_blockdims(old, new)
    pprint.pprint(i)
    
>>>>>>> reblock

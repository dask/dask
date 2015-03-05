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

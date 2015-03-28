""" 
test_binary_dask_op.py 

These tests assert that dask arrays of the same shape
can be added, subtracted, etc, even if the dask arrays
have different blockdims.
"""
from itertools import product
import numpy as np
from dask.array.reblock import intersect_blockdims, reblock
from dask.array.reblock import cumdims_label, _breakpoints, _intersect_1d
import dask.array as da
from dask.array.core import blockdims_from_blockshape
n = np.random.uniform(0,1,100).reshape((10,10))
n2 = np.random.uniform(0,1,100).reshape((10,10))
blockdimsx = ((5,5),(3,7))
blockdimsy = ((2,8),(1,9))
x = da.from_array(n, blockdims=blockdimsx)
y = da.from_array(n2, blockdims=blockdimsy)
    
def test_add():
    z = x + y
    assert np.all(z.compute() == x.compute() + y.compute())
    z = 2 * x + 3 * y
    assert np.all(z.compute() ==  x.compute() * 2 + y.compute() * 3)

def test_sub():
    z = x - y
    assert np.all(z.compute() == x.compute() - y.compute())


def test_mul():
    z = x * y
    assert np.all(z.compute() == x.compute() * y.compute())


def test_div():
    z = x / y
    assert np.all(z.compute() == x.compute() / y.compute())


def test_rdiv():
    z = da.from_array(n, blockdims=blockdimsx)
    z /= da.from_array(n2, blockdims=blockdimsy)
    assert np.all(z.compute() == n/n2)


def test_rmul():
    z = da.from_array(n, blockdims=blockdimsx)
    z *= da.from_array(n2, blockdims=blockdimsy)
    assert np.all(z.compute() == n * n2)


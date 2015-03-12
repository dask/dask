import numpy as np
from dask.array.reblock import intersect_blockdims, reblock
import dask.array as da
from dask.array.core import blockdims_from_blockshape

def test_intersect_1():
    """ Convert 1 D blockdims"""
    old=((10, 10, 10, 10, 10),)
    new = ((25, 5, 20), )
    answer = ((((0, slice(0, 10, None)),),
              ((1, slice(0, 10, None)),),
              ((2, slice(0, 5, None)),)),
             (((2, slice(5, 10, None)),),),
             (((3, slice(0, 10, None)),), ((4, slice(0, 10, None)),)))
    cross = intersect_blockdims(old_blockdims=old, new_blockdims=new)
    assert answer == cross


def test_intersect_2():
    """ Convert 1 D blockdims"""
    old = ((20, 20, 20, 20, 20), )
    new = ((58, 4, 20, 18),)
    answer = ((((0, slice(0, 20, None)),),
            ((1, slice(0, 20, None)),),
            ((2, slice(0, 18, None)),)),
           (((2, slice(18, 20, None)),), ((3, slice(0, 2, None)),)),
           (((3, slice(2, 20, None)),), ((4, slice(0, 2, None)),)),
           (((4, slice(2, 20, None)),),))
    cross = intersect_blockdims(old_blockdims=old, new_blockdims=new)
    assert answer == cross


def test_reblock_1d():
    """Try reblocking a random 1d matrix"""
    a = np.random.uniform(0,1,300)
    x = da.from_array(a, blockdims = ((100,)*3,))
    new = ((50,)*6,)
    x2 =reblock(x, blockdims=new)
    assert x2.blockdims == new
    assert np.all(x2.compute() == a)


def test_reblock_2d():
    """Try reblocking a random 2d matrix"""
    a = np.random.uniform(0,1,300).reshape((10,30))
    x = da.from_array(a, blockdims = ((1,2,3,4),(5,)*6))
    new = ((5,5), (15,)*2)
    x2 =reblock(x, blockdims=new)
    assert x2.blockdims == new
    assert np.all(x2.compute() == a)


def test_reblock_4d():
    """Try reblocking a random 4d matrix"""
    old = ((5,5),)*4
    a = np.random.uniform(0,1,10000).reshape((10,) * 4)
    x = da.from_array(a, blockdims = old)
    new = ((10,),)* 4
    x2 =reblock(x, blockdims = new)
    assert x2.blockdims == new
    assert np.all(x2.compute() == a)


def test_reblock_expand():
    a = np.random.uniform(0,1,100).reshape((10, 10))
    x = da.from_array(a, blockshape=(5, 5))
    y = x.reblock(blockdims=((3, 3, 3, 1), (3, 3, 3, 1)))
    assert np.all(y.compute() == a)


def test_reblock_method():
    """ Test reblocking can be done as a method of dask array."""
    old = ((9,3),)*3
    new = ((3,6,3),)*3
    a = np.random.uniform(0,1,10000).reshape((10,) * 4)
    x = da.from_array(a, blockdims=old)
    x2 = x.reblock(blockdims=new)
    assert x2.blockdims == new
    assert np.all(x2.compute() == a)


def test_reblock_blockshape():
    new_shape, new_blockshape = (10, 10), (4, 3)
    new_blockdims = blockdims_from_blockshape(new_shape, new_blockshape)
    old_blockdims = ((4, 4, 2), (3, 3, 3, 1))
    a = np.random.uniform(0,1,100).reshape((10, 10))
    x = da.from_array(a, blockdims=old_blockdims)
    check1 = reblock(x, blockdims=new_blockdims)
    check2 = reblock(x, blockshape=new_blockshape)
    assert check1.blockdims == new_blockdims
    assert check2.blockdims == new_blockdims
    assert np.all(check2.compute() == a)
    assert np.all(check1.compute() == a)


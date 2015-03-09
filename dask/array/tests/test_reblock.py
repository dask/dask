import numpy as np
from dask.array.reblock import intersect_blockdims, reblock
import dask.array as da

def test_intersect_1():
    """ Example from Matthew's docs"""
    old=((10, 10, 10, 10, 10),)
    new = ((25, 5, 20), )
    answer = ((((0, slice(0, 10, None)),
                (1, slice(0, 10, None)),
                (2, slice(0, 5, None))),
               ((2, slice(5, 10, None)),),
               ((3, slice(0, 10, None)),
                (4, slice(0, 10, None)))),)
    got, cross = intersect_blockdims(old, new)
    assert answer == got


def test_intersect_2():

    old = ((20, 20, 20, 20, 20), )
    new = ((58, 4, 20, 18),)
    answer1, answer2 = (((((0, slice(0, 20, None)), (1, slice(0, 20, None)), 
                        (2, slice(0, 18, None))),
           ((2, slice(18, 20, None)), (3, slice(0, 2, None))),
           ((3, slice(2, 20, None)), (4, slice(0, 2, None))),
           ((4, slice(2, 20, None)),)),),
         ((((0, slice(0, 20, None)),),
           ((1, slice(0, 20, None)),),
           ((2, slice(0, 18, None)),)),
          (((2, slice(18, 20, None)),), ((3, slice(0, 2, None)),)),
          (((3, slice(2, 20, None)),), ((4, slice(0, 2, None)),)),
          (((4, slice(2, 20, None)),),)))
    single, cross = intersect_blockdims(old, new)
    assert answer1 == single
    assert answer2 == cross
def test_reblock_2d():
    a = np.random.uniform(0,1,300).reshape((10,30))
    dsk = da.from_array(a, blockdims = ((1,2,3,4),(5,)*6))
    new = ((5,5), (15,)*2)
    dsk2 =reblock(dsk, new)
    assert dsk2.blockdims == new
    assert np.all(dsk2.compute() == a)
def test_reblock_4d():
    old = ((5,5),)*4
    a = np.random.uniform(0,1,10000).reshape((10,) * 4)
    dsk = da.from_array(a, blockdims = old  )
    new = ((10,),)* 4
    dsk2 =reblock(dsk, new)
    assert dsk2.blockdims == new
    assert np.all(dsk2.compute() == a)
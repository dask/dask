from itertools import product
import numpy as np
from dask.array.reblock import intersect_blockdims, reblock
from dask.array.reblock import cumdims_label, _breakpoints, _intersect_1d
import dask.array as da
from dask.array.core import blockdims_from_blockshape


def test_reblock_internals_1():
    """ Test the cumdims_label and _breakpoints and
    _intersect_1d internal funcs to reblock."""
    new = cumdims_label(((1,1,2),(1,5,1)),'n')
    old = cumdims_label(((4, ),(1,) * 5),'o')
    breaks = tuple(_breakpoints(o, n) for o, n in zip(old, new))
    answer = (('o', 0), ('n', 0), ('n', 1), ('n', 2), ('o', 4), ('n', 4))
    assert breaks[0] == answer
    answer2 = (('o', 0),
                 ('n', 0),
                 ('o', 1),
                 ('n', 1),
                 ('o', 2),
                 ('o', 3),
                 ('o', 4),
                 ('o', 5),
                 ('n', 6),
                 ('n', 7))
    assert breaks[1] == answer2
    i1d = [_intersect_1d(b) for b in breaks]
    answer3 = (((0, slice(0, 1, None)),),
             ((0, slice(1, 2, None)),),
             ((0, slice(2, 4, None)),))
    assert i1d[0] == answer3
    answer4 = (((0, slice(0, 1, None)),),
             ((1, slice(0, 1, None)),
              (2, slice(0, 1, None)),
              (3, slice(0, 1, None)),
              (4, slice(0, 1, None)),
              (5, slice(0, 1, None))),
             ((5, slice(1, 2, None)),))
    assert i1d[1] == answer4


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


def test_reblock_expand2():
    (a, b) = (3, 2)
    orig = np.random.uniform(0, 1, a ** b).reshape((a,) * b)
    for off, off2 in product(range(1, a - 1), range(1, a - 1)):
        old = ((a - off, off) ,)* b
        x = da.from_array(orig, blockdims=old)
        new = ((a - off2, off2) ,)* b
        assert np.all(x.reblock(blockdims=new).compute() == orig)
        if a - off - off2 > 0:
            new = ((off, a - off2 - off, off2) ,)* b
            y = x.reblock(blockdims=new).compute()
            assert np.all(y == orig)


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
    """ Test that blockshape can be used."""
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




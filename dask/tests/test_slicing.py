#!/usr/bin/env py.test

import dask
from dask.array import (_block_slice_step_start, _index_in_block,
                        dask_slice, _slice_1d, _first_step_in_block)
import operator
import numpy

def test_block_slice_step_start():
    arr25 = numpy.arange(25)
    arr27 = numpy.arange(27)

    assert all(arr25[::2] == arr25[_block_slice_step_start(25, 0, 0, 25, 2)])
    assert all(arr25[1::2] == arr25[_block_slice_step_start(25, 1, 0, 50, 2)])
    assert all(arr25[::2] == arr25[_block_slice_step_start(25, 2, 0, 100, 2)])
    assert all(arr25[1::2] == arr25[_block_slice_step_start(25, 3, 0, 150, 2)])

    assert all(arr25[::3] == arr25[_block_slice_step_start(25, 0, 0, 25, 3)])
    assert all(arr25[::10] == arr25[_block_slice_step_start(25, 0, 0, 25, 10)])
    assert all(arr25[::20] == arr25[_block_slice_step_start(25, 0, 0, 25, 20)])
    assert all(arr25[::32] == arr25[_block_slice_step_start(25, 0, 0, 25, 32)])
    

def test_slice_1d():
    expected = {0: slice(10, 25, 1), 1: slice(0, 25, 1), 2: slice(0, 1, 1)}
    result = _slice_1d(100, 25, slice(10,51,None))

    assert expected == result

    expected = {0: slice(None, None, None), 1: slice(None, None, None),
                2: slice(None, None, None), 3: slice(None, None, None)}
    result = _slice_1d(100,25,slice(None,None,None))

    assert expected == result


def test_dask_slice_1d():
    #x[24::2]
    expected = {('y', 0): (operator.getitem, ('x', 0), (slice(24, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 3): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
    result = dask_slice('y', 'x', [100], [25], [slice(24,None,2)])

    assert expected == result

    #x[26::2]
    expected = {('y', 0): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
        
    result = dask_slice('y', 'x', [100], [25], [slice(26,None,2)])    
    assert expected == result

    #x[24::2]
    expected = {('y', 0): (operator.getitem, ('x', 0), (slice(24, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 3): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
    result = dask_slice('y', 'x', (100,), (25,), (slice(24,None,2),))

    assert expected == result

    #x[26::2]
    expected = {('y', 0): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
        
    result = dask_slice('y', 'x', (100,), (25,), (slice(26,None,2),))    
    assert expected == result


def test_dask_slice_2d():
    #2d slices: x[13::2,10::1]
    expected = {('y', 0, 0): (operator.getitem,
                               ('x', 0, 0),
                               (slice(13, 20, 2), slice(10, 20, 1))),
                 ('y', 0, 1): (operator.getitem,
                               ('x', 0, 1),
                               (slice(13, 20, 2), slice(0, 20, 1))),
                 ('y', 0, 2): (operator.getitem,
                               ('x', 0, 2),
                               (slice(13, 20, 2), slice(0, 5, 1)))}
        
    result = dask_slice('y', 'x', [20, 45], [25, 20],
                        [slice(13,None,2), slice(10, None, 1)])

    assert expected == result

    #2d slices with one dimension: x[5,10::1]
    expected = {('y', 0, 0): (operator.getitem,
                               ('x', 0, 0),
                               (5, slice(10, 20, 1))),
                 ('y', 0, 1): (operator.getitem,
                               ('x', 0, 1),
                               (5, slice(0, 20, 1))),
                 ('y', 0, 2): (operator.getitem,
                               ('x', 0, 2),
                               (5, slice(0, 5, 1)))}
        
    result = dask_slice('y', 'x', [20, 45], [25, 20],
                        [5, slice(10, None, 1)])

    assert expected == result
    

def test_first_step_in_block():
    #None of these tests have a start index in the block
    #  because this function does not account for that.
    #That case is handled by _block_slice_step_start in
    #  the actual code
    assert _first_step_in_block(2, 10, 25, 1) == 1
    assert _first_step_in_block(2, 10, 25, 2) == 0
    assert _first_step_in_block(2, 10, 25, 3) == 1
    assert _first_step_in_block(2, 10, 25, 4) == 0

    assert _first_step_in_block(1, 0, 20, 1) == 0
    assert _first_step_in_block(1, 0, 20, 2) == 0
    assert _first_step_in_block(1, 0, 20, 3) == 0
    assert _first_step_in_block(1, 0, 20, 4) == 0

    #Step greater than block size
    assert _first_step_in_block(37, 10, 15, 1) == 32
    assert _first_step_in_block(29, 10, 15, 2) == 9
    assert _first_step_in_block(29, 20, 15, 3) == 4
    assert _first_step_in_block(29, 37, 15, 4) == 6

    #Start index > blocknum*blocksize
    assert _first_step_in_block(13, 25, 15, 0) == -1
    

def test_slice_optimizations():
    #bar[:]
    expected = {'foo':'bar'}
    result = dask_slice('foo', 'bar', (100,), (13,), (slice(None,None,None),))
    assert expected == result

    #bar[:,:,:]
    expected = {'foo':'bar'}
    result = dask_slice('foo', 'bar', (100,1000,10000), (13,0,331),
                        (slice(None,None,None),slice(None,None,None),
                         slice(None,None,None)))
    assert expected == result


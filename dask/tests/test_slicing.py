#!/usr/bin/env py.test

import dask
from dask.array import _block_slice_step_start, _index_in_block, dask_slice, _slice_1d
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
    expected = {0: slice(10, 25, None), 1: slice(0, 25, None), 2: slice(0, 1, None)}
    result = _slice_1d(100, 25, slice(10,51,None))

    assert expected == result

    expected = {0: slice(None, None, None), 1: slice(None, None, None), 2: slice(None, None, None), 3: slice(None, None, None)}
    result = _slice_1d(100,25,slice(None,None,None))

    assert expected == result


def test_dask_slice_1d():
    expected = {('y', 0): (operator.getitem, ('x', 0), (slice(24, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 3): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
    result = dask_slice('x', 'y', [100], [25], [slice(24,None,2)])

    assert expected == result

    expected = {('y', 0): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
        
    result = dask_slice('x', 'y', [100], [25], [slice(26,None,2)])    
    assert expected == result

def test_dask_slice_2d():
    expected = {('y', 0, 0): (operator.getitem,
                               ('x', 0, 0),
                               (slice(13, 20, 2), slice(10, 20, None))),
                 ('y', 0, 1): (operator.getitem,
                               ('x', 0, 1),
                               (slice(13, 20, 2), slice(0, 20, None))),
                 ('y', 0, 2): (operator.getitem,
                               ('x', 0, 2),
                               (slice(13, 20, 2), slice(0, 5, None)))}
        
    result = dask_slice('x', 'y', [20, 45], [25, 20], [slice(13,None,2), slice(10, None, None)])

    assert expected == result

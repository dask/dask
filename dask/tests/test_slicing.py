#!/usr/bin/env py.test

import dask
from dask.array import dask_1d_slice, _dask_slice
import operator

def test_1d_left_slicing():
    expected = {('y', 0): (operator.getitem, ('x',0), (slice(5,20))),
                ('y', 1): (operator.getitem, ('x',1), (slice(0,3)))}
    result = dask_1d_slice('y', 'x', slice(5,23), (100,), (20,))
    assert expected == result

def test_1d_inner_slicing():
    expected = {('y', 0): (operator.getitem, ('x',0), (slice(3,19)))}
    result = dask_1d_slice('y', 'x', slice(3,19), (100,), (20,))
    assert expected == result

def test_1d_full_inner_slicing():
    expected = {('y', 0): (operator.getitem, ('x',2), (slice(5,20))),
                ('y', 1): (operator.getitem, ('x',3), (slice(0,20))),
                ('y', 2): (operator.getitem, ('x',4), (slice(0,12)))}
    result = dask_1d_slice('y', 'x', slice(45,92), (100,), (20,))
    assert expected == result
    
        
def test_1d_right_border_slicing():
    #The first test slices up to the right-most border
    #  of the array
    expected = {('y', 0): (operator.getitem, ('x',2), (slice(5,20))),
                ('y', 1): (operator.getitem, ('x',3), (slice(0,20))),
                ('y', 2): (operator.getitem, ('x',4), (slice(0,20)))}
    result = dask_1d_slice('y', 'x', slice(45,100), (100,), (20,))
    assert expected == result

    #The first test slices past the right-most border
    #  of the array
    expected = {('y', 0): (operator.getitem, ('x',2), (slice(5,20))),
                ('y', 1): (operator.getitem, ('x',3), (slice(0,20))),
                ('y', 2): (operator.getitem, ('x',4), (slice(0,20)))}
    result = dask_1d_slice('y', 'x', slice(45,200), (100,), (20,))
    assert expected == result
    

def test_1d_negative_slicing():
    expected = {('y', 0): (operator.getitem, ('x',2), (slice(15,20))),
                ('y', 1): (operator.getitem, ('x',3), (slice(0,20))),
                ('y', 2): (operator.getitem, ('x',4), (slice(0,19)))}
    result = dask_1d_slice('y', 'x', slice(-45,-1), (100,), (20,))
    assert expected == result

def test_1d_bad_slices():
    expected = {}
    result = dask_1d_slice('y', 'x', slice(100,200), (100,), (20,))
    assert expected == result

    

def test_priv_dask_slice_1d():
    expected = {('y', 0):(operator.getitem, ('x', 0), (slice(5,20),)),
                ('y', 1):(operator.getitem, ('x', 1), (slice(0,10),)),
                }
    result = _dask_slice('x', 'y', [0], [2], [5], [10], [20])

    assert expected == result
    

def test_priv_dask_slice_2d():
    expected = {('y', 0, 0):(operator.getitem, ('x', 1, 0),
                             (slice(5,20), slice(10,22))),
                ('y', 1, 0):(operator.getitem, ('x', 2, 0),
                             (slice(0,10), slice(10,22))),
                }
    result = _dask_slice('x', 'y', [1, 0], [3, 1], [5, 10], [10, 22], [20, 30])

    assert expected == result
    
    expected = {('y', 0, 0):(operator.getitem, ('x', 1, 0),
                             (slice(5,20), slice(10,30))),
                ('y', 0, 1):(operator.getitem, ('x', 1, 1),
                             (slice(5,20), slice(0,15))),

                ('y', 1, 0):(operator.getitem, ('x', 2, 0),
                             (slice(0,20), slice(10,30))),
                ('y', 1, 1):(operator.getitem, ('x', 2, 1),
                             (slice(0,20), slice(0,15))),

                ('y', 2, 0):(operator.getitem, ('x', 3, 0),
                             (slice(0,10), slice(10,30))),
                ('y', 2, 1):(operator.getitem, ('x', 3, 1),
                             (slice(0,10), slice(0,15)))
                }

    result = _dask_slice('x', 'y', [1, 0], [4, 2], [5, 10], [10, 15], [20, 30])

    assert expected == result
    

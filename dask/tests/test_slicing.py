#!/usr/bin/env py.test

import dask
from dask.array import dask_slice, _slice_1d
import operator
import numpy


def test_slice_1d():
    expected = {0: slice(10, 25, 1), 1: slice(0, 25, 1), 2: slice(0, 1, 1)}
    result = _slice_1d(100, [25]*4, slice(10,51,None))

    assert expected == result

    expected = {0: slice(None, None, None), 1: slice(None, None, None),
                2: slice(None, None, None), 3: slice(None, None, None)}
    result = _slice_1d(100, [25]*4 ,slice(None, None, None))

    assert expected == result


def test_dask_slice_1d():
    #x[24::2]
    expected = {('y', 0): (operator.getitem, ('x', 0), (slice(24, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 3): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
    result = dask_slice('y', 'x', [100], [[25]*4], [slice(24,None,2)])

    assert expected == result

    #x[26::2]
    expected = {('y', 0): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}

    result = dask_slice('y', 'x', [100], [[25]*4], [slice(26,None,2)])
    assert expected == result

    #x[24::2]
    expected = {('y', 0): (operator.getitem, ('x', 0), (slice(24, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 3): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}
    result = dask_slice('y', 'x', (100,), [(25,)*4], (slice(24,None,2),))

    assert expected == result

    #x[26::2]
    expected = {('y', 0): (operator.getitem, ('x', 1), (slice(1, 25, 2),)),
                ('y', 1): (operator.getitem, ('x', 2), (slice(0, 25, 2),)),
                ('y', 2): (operator.getitem, ('x', 3), (slice(1, 25, 2),))}

    result = dask_slice('y', 'x', (100,), [(25,)*4], (slice(26,None,2),))
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

    result = dask_slice('y', 'x', (20, 45), [[20], [20, 20, 5]],
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

    result = dask_slice('y', 'x', (20, 45), ([20], [20, 20, 5]),
                        [5, slice(10, None, 1)])

    assert expected == result


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


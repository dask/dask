import dask
from dask.array import dask_slice, _slice_1d
import operator
import numpy


def test_slice_1d():
    expected = {0: slice(10, 25, 1), 1: slice(0, 25, 1), 2: slice(0, 1, 1)}
    result = _slice_1d(100, [25]*4, slice(10, 51, None))
    assert expected == result

    #x[100:12:-3]
    expected = {0: slice(-2, -8, -3),
                1: slice(-1, -21, -3),
                2: slice(-3, -21, -3),
                3: slice(-2, -21, -3),
                4: slice(-1, -21, -3)}
    result = _slice_1d(100, [20]*5, slice(100, 12, -3))
    assert expected == result

    #x[102::-3]
    expected = {0: slice(-2, -21, -3),
                1: slice(-1, -21, -3),
                2: slice(-3, -21, -3),
                3: slice(-2, -21, -3),
                4: slice(-1, -21, -3)}
    result = _slice_1d(100, [20]*5, slice(102, None, -3))
    assert expected == result

    #x[::-4]
    expected = {0: slice(-1, -21, -4),
                1: slice(-1, -21, -4),
                2: slice(-1, -21, -4),
                3: slice(-1, -21, -4),
                4: slice(-1, -21, -4)}
    result = _slice_1d(100, [20]*5, slice(None, None, -4))
    assert expected == result

    #x[::-7]
    expected = {0: slice(-5, -21, -7),
                1: slice(-4, -21, -7),
                2: slice(-3, -21, -7),
                3: slice(-2, -21, -7),
                4: slice(-1, -21, -7)}
    result = _slice_1d(100, [20]*5, slice(None, None, -7))
    assert expected == result

    #x=range(115)
    #x[::-7]
    expected = {0: slice(-7, -24, -7),
                1: slice(-2, -24, -7),
                2: slice(-4, -24, -7),
                3: slice(-6, -24, -7),
                4: slice(-1, -24, -7)}
    result = _slice_1d(115, [23]*5, slice(None, None, -7))
    assert expected == result

    #x[79::-3]
    expected = {0: slice(-1, -21, -3),
                1: slice(-3, -21, -3),
                2: slice(-2, -21, -3),
                3: slice(-1, -21, -3)}
    result = _slice_1d(100, [20]*5, slice(79, None, -3))
    assert expected == result

    #x[-1:-8:-1]
    expected = {4: slice(-1, -8, -1)}
    result = _slice_1d(100, [20, 20, 20, 20, 20], slice(-1, 92, -1))
    assert expected == result

    #x[20:0:-1]
    expected = {0: slice(-1, -20, -1),
                1: slice(-20, -21, -1)}
    result = _slice_1d(100, [20, 20, 20, 20, 20], slice(20, 0, -1))
    assert expected == result

    #x=range(99)
    expected = {0: slice(-3, -21, -3),
                1: slice(-2, -21, -3),
                2: slice(-1, -21, -3),
                3: slice(-2, -20, -3),
                4: slice(-1, -21, -3)}
    #This array has non-uniformly sized blocks
    result = _slice_1d(99, [20, 20, 20, 19, 20], slice(100, None, -3))
    assert expected == result

    #x=range(104)
    #x[::-3]
    expected = {0: slice(-1, -21, -3),
                1: slice(-3, -24, -3),
                2: slice(-3, -28, -3),
                3: slice(-1, -14, -3),
                4: slice(-1, -22, -3)}
    #This array has non-uniformly sized blocks
    result = _slice_1d(104, [20, 23, 27, 13, 21], slice(None, None, -3))
    assert expected == result

    #x=range(104)
    #x[:27:-3]
    expected = {1: slice(-3, -16, -3),
                2: slice(-3, -28, -3),
                3: slice(-1, -14, -3),
                4: slice(-1, -22, -3)}
    #This array has non-uniformly sized blocks
    result = _slice_1d(104, [20, 23, 27, 13, 21], slice(None, 27, -3))
    assert expected == result

    #x=range(104)
    #x[100:27:-3]
    expected = {1: slice(-3, -16, -3),
                2: slice(-3, -28, -3),
                3: slice(-1, -14, -3),
                4: slice(-4, -22, -3)}
    #This array has non-uniformly sized blocks
    result = _slice_1d(104, [20, 23, 27, 13, 21], slice(100, 27, -3))
    assert expected == result


def test_slice_singleton_value_on_boundary():
    assert _slice_1d(15, [5, 5, 5], 10) == {2: 0}
    assert _slice_1d(30, (5, 5, 5, 5, 5, 5), 10) == {2: 0}


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
    expected = {('y', 0): (operator.getitem,
                               ('x', 0, 0),
                               (5, slice(10, 20, 1))),
                 ('y', 1): (operator.getitem,
                               ('x', 0, 1),
                               (5, slice(0, 20, 1))),
                 ('y', 2): (operator.getitem,
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


def test_slicing_with_singleton_indices():
    result = dask_slice('y', 'x', (10, 10), ([5, 5], [5, 5]),
                        (slice(0, 5), 8))

    expected = {('y', 0): (operator.getitem, ('x', 0, 1), (slice(0, 5, 1), 3))}

    assert expected == result

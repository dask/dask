import pytest
pytest.importorskip('numpy')

from itertools import product
import numpy as np
from dask.array.rechunk import intersect_chunks, rechunk, normalize_chunks
from dask.array.rechunk import cumdims_label, _breakpoints, _intersect_1d
import dask.array as da
from dask.utils import raises


def test_rechunk_internals_1():
    """ Test the cumdims_label and _breakpoints and
    _intersect_1d internal funcs to rechunk."""
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
    """ Convert 1 D chunks"""
    old=((10, 10, 10, 10, 10),)
    new = ((25, 5, 20), )
    answer = ((((0, slice(0, 10, None)),),
              ((1, slice(0, 10, None)),),
              ((2, slice(0, 5, None)),)),
             (((2, slice(5, 10, None)),),),
             (((3, slice(0, 10, None)),), ((4, slice(0, 10, None)),)))
    cross = intersect_chunks(old_chunks=old, new_chunks=new)
    assert answer == cross


def test_intersect_2():
    """ Convert 1 D chunks"""
    old = ((20, 20, 20, 20, 20), )
    new = ((58, 4, 20, 18),)
    answer = ((((0, slice(0, 20, None)),),
            ((1, slice(0, 20, None)),),
            ((2, slice(0, 18, None)),)),
           (((2, slice(18, 20, None)),), ((3, slice(0, 2, None)),)),
           (((3, slice(2, 20, None)),), ((4, slice(0, 2, None)),)),
           (((4, slice(2, 20, None)),),))
    cross = intersect_chunks(old_chunks=old, new_chunks=new)
    assert answer == cross


def test_rechunk_1d():
    """Try rechunking a random 1d matrix"""
    a = np.random.uniform(0,1,300)
    x = da.from_array(a, chunks = ((100,)*3,))
    new = ((50,)*6,)
    x2 =rechunk(x, chunks=new)
    assert x2.chunks == new
    assert np.all(x2.compute() == a)


def test_rechunk_2d():
    """Try rechunking a random 2d matrix"""
    a = np.random.uniform(0,1,300).reshape((10,30))
    x = da.from_array(a, chunks = ((1,2,3,4),(5,)*6))
    new = ((5,5), (15,)*2)
    x2 =rechunk(x, chunks=new)
    assert x2.chunks == new
    assert np.all(x2.compute() == a)


def test_rechunk_4d():
    """Try rechunking a random 4d matrix"""
    old = ((5,5),)*4
    a = np.random.uniform(0,1,10000).reshape((10,) * 4)
    x = da.from_array(a, chunks = old)
    new = ((10,),)* 4
    x2 =rechunk(x, chunks = new)
    assert x2.chunks == new
    assert np.all(x2.compute() == a)


def test_rechunk_expand():
    a = np.random.uniform(0,1,100).reshape((10, 10))
    x = da.from_array(a, chunks=(5, 5))
    y = x.rechunk(chunks=((3, 3, 3, 1), (3, 3, 3, 1)))
    assert np.all(y.compute() == a)


def test_rechunk_expand2():
    (a, b) = (3, 2)
    orig = np.random.uniform(0, 1, a ** b).reshape((a,) * b)
    for off, off2 in product(range(1, a - 1), range(1, a - 1)):
        old = ((a - off, off) ,)* b
        x = da.from_array(orig, chunks=old)
        new = ((a - off2, off2) ,)* b
        assert np.all(x.rechunk(chunks=new).compute() == orig)
        if a - off - off2 > 0:
            new = ((off, a - off2 - off, off2) ,)* b
            y = x.rechunk(chunks=new).compute()
            assert np.all(y == orig)


def test_rechunk_method():
    """ Test rechunking can be done as a method of dask array."""
    old = ((5, 2, 3),) * 4
    new = ((3, 3, 3, 1),) * 4
    a = np.random.uniform(0, 1, 10000).reshape((10,) * 4)
    x = da.from_array(a, chunks=old)
    x2 = x.rechunk(chunks=new)
    assert x2.chunks == new
    assert np.all(x2.compute() == a)


def test_rechunk_blockshape():
    """ Test that blockshape can be used."""
    new_shape, new_chunks = (10, 10), (4, 3)
    new_blockdims = normalize_chunks(new_chunks, new_shape)
    old_chunks = ((4, 4, 2), (3, 3, 3, 1))
    a = np.random.uniform(0,1,100).reshape((10, 10))
    x = da.from_array(a, chunks=old_chunks)
    check1 = rechunk(x, chunks=new_chunks)
    assert check1.chunks == new_blockdims
    assert np.all(check1.compute() == a)


def test_dtype():
    x = da.ones(5, chunks=(2,))
    assert x.rechunk(chunks=(1,))._dtype == x._dtype


def test_rechunk_with_dict():
    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(chunks={0: 12})
    assert y.chunks == ((12, 12), (8, 8, 8))

    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(chunks={0: (12, 12)})
    assert y.chunks == ((12, 12), (8, 8, 8))


def test_rechunk_with_empty_input():
    x = da.ones((24, 24), chunks=(4, 8))
    assert x.rechunk(chunks={}).chunks == x.chunks
    assert raises(ValueError, lambda: x.rechunk(chunks=()))


def test_rechunk_with_null_dimensions():
    x = da.from_array(np.ones((24, 24)), chunks=(4, 8))
    assert (x.rechunk(chunks=(None, 4)).chunks ==
            da.ones((24, 24), chunks=(4, 4)).chunks)


def test_rechunk_with_integer():
    x = da.from_array(np.arange(5), chunks=4)
    y = x.rechunk(3)
    assert y.chunks == ((3, 2),)
    assert (x.compute() == y.compute()).all()


def test_rechunk_0d():
    a = np.array(42)
    x = da.from_array(a, chunks=())
    y = x.rechunk(())
    assert y.chunks == ()
    assert y.compute() == a


def test_rechunk_same():
    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(x.chunks)
    assert x is y


def test_rechunk_intermediates():
    x = da.random.normal(10, 0.1, (10, 10), chunks=(10, 1))
    y = x.rechunk((1, 10))
    assert len(y.dask) > 30

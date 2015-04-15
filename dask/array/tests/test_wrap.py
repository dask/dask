from dask.array.wrap import ones
import dask.array as da
import numpy as np
import dask

def test_ones():
    a = ones((10, 10), dtype='i4', blockshape=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), 'i4')).all()

def test_size_as_list():
    a = ones([10, 10], dtype='i4', blockshape=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), dtype='i4')).all()

def test_singleton_size():
    a = ones(10, dtype='i4', blockshape=(4,))
    x = np.array(a)
    assert (x == np.ones(10, dtype='i4')).all()

def test_kwargs():
    a = ones(10, dtype='i4', blockshape=(4,))
    x = np.array(a)
    assert (x == np.ones(10, dtype='i4')).all()

def test_full():
    a = da.full((3, 3), 100, blockshape=(2, 2), dtype='i8')

    assert (a.compute() == 100).all()
    assert a._dtype == a.compute(get=dask.get).dtype == 'i8'

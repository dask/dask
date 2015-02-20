from dask.array.wrap import ones
import numpy as np

def test_ones():
    a = ones((10, 10), 'i4', blockshape=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), 'i4')).all()

def test_size_as_list():
    a = ones([10, 10], 'i4', blockshape=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), 'i4')).all()

def test_singleton_size():
    a = ones(10, 'i4', blockshape=(4,))
    x = np.array(a)
    assert (x == np.ones(10, 'i4')).all()

def test_kwargs():
    a = ones(10, dtype='i4', blockshape=(4,))
    x = np.array(a)
    assert (x == np.ones(10, 'i4')).all()

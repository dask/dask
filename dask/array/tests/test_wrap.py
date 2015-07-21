import pytest
pytest.importorskip('numpy')

from dask.array.wrap import ones
import dask.array as da
import numpy as np
import dask

def test_ones():
    a = ones((10, 10), dtype='i4', chunks=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), 'i4')).all()

def test_histogram():
    # Test for normal, flattened input
    n = 100
    v = da.random.random(n, chunks=10)
    
    bins = np.arange(0, 1.01, 0.01)
    (a1, b1) = da.histogram(v, bins=bins)
    (a2, b2) = np.histogram(v, bins=bins)
    
    def cmp_hist(a1, a2):
        # Check length is the same
        assert len(a1) == len(a2)
        assert len(b1) == len(b2)
        # Compare the arrays produced by dask and numpy
        assert (np.asarray(a1) == a2).all()
        assert (b1 == b2).all()
    
    # Check if the sum of the bins equals the number of samples
    assert a2.sum(axis=0) == n
    assert a1.sum(axis=0) == n
    cmp_hist(a1, a2)
    
    # Other input
    (a1, b1) = da.histogram(v, bins=10, range=(0, 1))
    (a2, b2) = np.histogram(v, bins=10, range=(0, 1))
    cmp_hist(a1, a2)
    
    # Check if return type is same as hist
    bins = np.arange(0, 11, 1, dtype='i4')
    (a1, b1) = da.histogram(v * 10, bins=bins)
    (a2, b2) = np.histogram(v * 10, bins=bins)
    cmp_hist(a1, a2)
    assert a1.dtype == a2.dtype
    
    # Check for extra args and shapes
    data = [(v, bins, ones(n, chunks=v.chunks) * 5),
            (da.random.random((50, 50), chunks=10), bins, ones((50, 50), chunks=10) * 5)]
    
    for v, bins, w in data:
        # density
        (a1, b1) = da.histogram(v, bins=bins, normed=True)
        (a2, b2) = np.histogram(v, bins=bins, normed=True)
        cmp_hist(a1, a2)
        
        # normed
        (a1, b1) = da.histogram(v, bins=bins, density=True)
        (a2, b2) = np.histogram(v, bins=bins, density=True)
        cmp_hist(a1, a2)
        
        # weights
        (a1, b1) = da.histogram(v, bins=bins, weights=w)
        (a2, b2) = np.histogram(v, bins=bins, weights=w)
        cmp_hist(a1, a2)
    
        (a1, b1) = da.histogram(v, bins=bins, weights=w, density=True)
        (a2, b2) = da.histogram(v, bins=bins, weights=w, density=True)
        cmp_hist(a1, a2)

def test_size_as_list():
    a = ones([10, 10], dtype='i4', chunks=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), dtype='i4')).all()

def test_singleton_size():
    a = ones(10, dtype='i4', chunks=(4,))
    x = np.array(a)
    assert (x == np.ones(10, dtype='i4')).all()

def test_kwargs():
    a = ones(10, dtype='i4', chunks=(4,))
    x = np.array(a)
    assert (x == np.ones(10, dtype='i4')).all()

def test_full():
    a = da.full((3, 3), 100, chunks=(2, 2), dtype='i8')

    assert (a.compute() == 100).all()
    assert a._dtype == a.compute(get=dask.get).dtype == 'i8'

def test_can_make_really_big_array_of_ones():
    a = ones((1000000, 1000000), chunks=(100000, 100000))
    a = ones(shape=(1000000, 1000000), chunks=(100000, 100000))

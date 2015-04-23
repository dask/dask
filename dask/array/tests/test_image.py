import numpy as np
from numpy.testing import assert_array_almost_equal

import pytest

import dask.array as da

filters = pytest.importorskip('scipy.ndimage.filters')

from scipy.ndimage.filters import (gaussian_filter, gaussian_filter1d,
                                   correlate1d, convolve1d)


# make data
a = np.random.random((20, 20))
d = da.from_array(a, chunks=(10, 10))


def test_gaussian_filter():
    # apply filter
    sigma = 1
    res = da.image.filter_(gaussian_filter, d, sigma=sigma)
    exp = gaussian_filter(a, sigma=sigma)

    assert_array_almost_equal(np.array(res), exp)


def test_gaussian_filter1d():
    # apply filter
    sigma = 1
    res = da.image.filter_(gaussian_filter1d, d, sigma=sigma)
    exp = gaussian_filter1d(a, sigma=sigma)

    assert_array_almost_equal(np.array(res), exp)

def test_correlate1d():
    weights = list(range(5))
    res = da.image.filter_(correlate1d, d, weights=weights)
    exp = correlate1d(a, weights=weights)

    assert_array_almost_equal(np.array(res), exp)

def test_convolve1d():
    weights = list(range(5))
    res = da.image.filter_(convolve1d, d, weights=weights)
    exp = convolve1d(a, weights=weights)

    assert_array_almost_equal(np.array(res), exp)

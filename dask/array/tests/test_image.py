import numpy as np
from numpy.testing import assert_array_almost_equal

import pytest

import dask.array as da
from dask.array.image import filter_

filters = pytest.importorskip('scipy.ndimage.filters')

from scipy.ndimage.filters import (gaussian_filter)

def test_filters():
    # make data
    a = np.random.random((10, 10))
    d = da.from_array(a, chunks=(2, 5))

    # apply filter
    sigma = 2
    res = filter_(gaussian_filter, d, sigma=sigma)
    exp = gaussian_filter(a, sigma=sigma)

    assert_array_almost_equal(np.array(res), exp)

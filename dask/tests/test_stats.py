import pytest

import numpy as np
import dask.array as da
import dask.stats
import scipy.stats
from dask.array.utils import allclose


@pytest.mark.parametrize('kind', [
    'chisquare', 'power_divergence',
])
def test_one(kind):
    a = np.random.random(size=30,)
    a_ = da.from_array(a, 3)

    dask_test = getattr(dask.stats, kind)
    scipy_test = getattr(scipy.stats, kind)

    result = dask_test(a_)
    expected = scipy_test(a)
    assert allclose(dask.compute(*result), expected)
    # fails occasionally. shouldn't this be exact?
    # assert dask.compute(*result) == expected

@pytest.mark.parametrize('kind', [
    'ttest_ind', 'ttest_1samp', 'chisquare', 'power_divergence'
])
def test_two(kind):
    a = np.random.random(size=30,)
    b = np.random.random(size=30,)
    a_ = da.from_array(a, 3)
    b_ = da.from_array(b, 3)

    dask_test = getattr(dask.stats, kind)
    scipy_test = getattr(scipy.stats, kind)

    result = dask_test(a_, b_)
    expected = scipy_test(a, b)
    assert allclose(dask.compute(*result), expected)
    # fails occasionally. shouldn't this be exact?
    # assert dask.compute(*result) == expected

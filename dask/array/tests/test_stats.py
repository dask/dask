import pytest

import numpy as np
import dask.array as da
from dask.array.utils import assert_eq
from dask.delayed import Delayed
import dask.array.stats
import scipy.stats
from dask.array.utils import allclose


@pytest.mark.parametrize('kind, kwargs', [
    ('skew', {}),
    ('skew', {'bias': True}),
    ('kurtosis', {}),
    ('kurtosis', {'bias': True}),
    ('kurtosis', {'fisher': False}),
])
def test_measures(kind, kwargs):
    x = np.random.random(size=(30, 2))
    y = da.from_array(x, 3)
    dfunc = getattr(dask.array.stats, kind)
    sfunc = getattr(scipy.stats, kind)

    expected = sfunc(x, **kwargs)
    result = dfunc(y, **kwargs)
    assert_eq(result, expected)
    assert isinstance(result, da.Array)


@pytest.mark.parametrize('kind', [
    'chisquare', 'power_divergence', 'normaltest', 'skewtest', 'kurtosistest',
])
def test_one(kind):
    a = np.random.random(size=30,)
    a_ = da.from_array(a, 3)

    dask_test = getattr(dask.array.stats, kind)
    scipy_test = getattr(scipy.stats, kind)

    result = dask_test(a_)
    expected = scipy_test(a)

    assert isinstance(result, Delayed)
    assert allclose(result.compute(), expected)


@pytest.mark.parametrize('kind, kwargs', [
    ('ttest_ind', {}),
    ('ttest_ind', {'equal_var': False}),
    ('ttest_1samp', {}),
    ('ttest_rel', {}),
    ('chisquare', {}),
    ('power_divergence', {}),
    ('power_divergence', {'lambda_': 0}),
    ('power_divergence', {'lambda_': -1}),
    ('power_divergence', {'lambda_': 'neyman'}),
])
def test_two(kind, kwargs):
    a = np.random.random(size=30,)
    b = np.random.random(size=30,)
    a_ = da.from_array(a, 3)
    b_ = da.from_array(b, 3)

    dask_test = getattr(dask.array.stats, kind)
    scipy_test = getattr(scipy.stats, kind)

    result = dask_test(a_, b_, **kwargs)
    expected = scipy_test(a, b, **kwargs)

    assert isinstance(result, Delayed)
    assert allclose(result.compute(), expected)
    # fails occasionally. shouldn't this be exact?
    # assert dask.compute(*result) == expected


@pytest.mark.parametrize('k', range(5))
def test_moments(k):
    x = np.random.random(size=(30, 2))
    y = da.from_array(x, 3)

    expected = scipy.stats.moment(x, k)
    result = dask.array.stats.moment(y, k)
    assert_eq(result, expected)


def test_anova():
    np_args = [i * np.random.random(size=(30,)) for i in range(4)]
    da_args = [da.from_array(x, chunks=10) for x in np_args]

    result = dask.array.stats.f_oneway(*da_args)
    expected = scipy.stats.f_oneway(*np_args)

    assert allclose(result.compute(), expected)

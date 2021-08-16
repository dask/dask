import pytest

pytestmark = pytest.mark.gpu

import dask.array as da
from dask.array.utils import assert_eq

cupy = pytest.importorskip("cupy")


def test_random_all():
    def rnd_test(func, *args, **kwargs):
        a = func(*args, **kwargs)
        assert type(a._meta) == cupy.ndarray
        assert_eq(a, a)  # Check that _meta and computed arrays match types

    rs = da.random.RandomState(RandomState=cupy.random.RandomState)

    rnd_test(rs.beta, 1, 2, size=5, chunks=3)
    rnd_test(rs.binomial, 10, 0.5, size=5, chunks=3)
    rnd_test(rs.chisquare, 1, size=5, chunks=3)
    rnd_test(rs.exponential, 1, size=5, chunks=3)
    rnd_test(rs.f, 1, 2, size=5, chunks=3)
    rnd_test(rs.gamma, 5, 1, size=5, chunks=3)
    rnd_test(rs.geometric, 1, size=5, chunks=3)
    rnd_test(rs.gumbel, 1, size=5, chunks=3)
    rnd_test(rs.hypergeometric, 1, 2, 3, size=5, chunks=3)
    rnd_test(rs.laplace, size=5, chunks=3)
    rnd_test(rs.logistic, size=5, chunks=3)
    rnd_test(rs.lognormal, size=5, chunks=3)
    rnd_test(rs.logseries, 0.5, size=5, chunks=3)
    # No RandomState for multinomial in CuPy
    # rnd_test(rs.multinomial, 20, [1 / 6.] * 6, size=5, chunks=3)
    rnd_test(rs.negative_binomial, 5, 0.5, size=5, chunks=3)
    rnd_test(rs.noncentral_chisquare, 2, 2, size=5, chunks=3)

    rnd_test(rs.noncentral_f, 2, 2, 3, size=5, chunks=3)
    rnd_test(rs.normal, 2, 2, size=5, chunks=3)
    rnd_test(rs.pareto, 1, size=5, chunks=3)
    rnd_test(rs.poisson, size=5, chunks=3)

    rnd_test(rs.power, 1, size=5, chunks=3)
    rnd_test(rs.rayleigh, size=5, chunks=3)
    rnd_test(rs.random_sample, size=5, chunks=3)

    rnd_test(rs.triangular, 1, 2, 3, size=5, chunks=3)
    rnd_test(rs.uniform, size=5, chunks=3)
    rnd_test(rs.vonmises, 2, 3, size=5, chunks=3)
    rnd_test(rs.wald, 1, 2, size=5, chunks=3)

    rnd_test(rs.weibull, 2, size=5, chunks=3)
    rnd_test(rs.zipf, 2, size=5, chunks=3)

    rnd_test(rs.standard_cauchy, size=5, chunks=3)
    rnd_test(rs.standard_exponential, size=5, chunks=3)
    rnd_test(rs.standard_gamma, 2, size=5, chunks=3)
    rnd_test(rs.standard_normal, size=5, chunks=3)
    rnd_test(rs.standard_t, 2, size=5, chunks=3)


@pytest.mark.parametrize("shape", [(2, 3), (2, 3, 4), (2, 3, 4, 2)])
def test_random_shapes(shape):
    rs = da.random.RandomState(RandomState=cupy.random.RandomState)

    x = rs.poisson(size=shape, chunks=3)
    assert type(x._meta) == cupy.ndarray
    assert_eq(x, x)  # Check that _meta and computed arrays match types
    assert x._meta.shape == (0,) * len(shape)
    assert x.shape == shape

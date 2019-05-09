# Tests in this file are potentially good candidates for future regression tests
# to be merged into their own repositories. Therefore, imports are part of each
# test, making it immediately clear of their requirements, and usually the last
# import identifies the actual project being tested. Tests are in no particular
# order.
#
# Passing tests are added at the bottom after the "PASSING" comment line

import numpy as np
import cupy
import dask.array as da
from cupy import asnumpy
from numpy.testing import assert_array_equal, assert_array_almost_equal
from dask.array.utils import assert_eq


def test_dask_mean_nep18():
    x = cupy.random.random((1000, 1000))

    d = da.from_array(x, chunks=(100, 100), asarray=False)

    assert_array_equal(asnumpy(x.mean()), asnumpy(d.mean().compute()))
    assert_array_equal(asnumpy(np.cov(x)), asnumpy(da.cov(d).compute()))


def test_cupy_conj_nep18():
    x = cupy.random.random(5000000)

    d = da.from_array(x, chunks=(1000000), asarray=False)
    assert_array_equal(asnumpy(np.conj(x)), asnumpy(da.conj(d).compute()))


def test_dask_glm_algorithms_nep18():
    import dask_glm.algorithms

    X = np.random.random((1000, 10))
    dX = da.from_array(X, chunks=(100, 10), asarray=False)
    y = np.ones(1000)
    dy = da.from_array(y, chunks=(100,), asarray=False)

    X_cupy = cupy.random.random((1000, 10))
    dX_cupy = da.from_array(X_cupy, chunks=(100, 10), asarray=False)
    y_cupy = cupy.ones(1000)
    dy_cupy = da.from_array(y_cupy, chunks=(100,), asarray=False)

    result = dask_glm.algorithms.admm(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.admm(dX_cupy, dy_cupy, max_iter=5)
    assert_array_equal(asnumpy(result_cupy), asnumpy(result))

    result = dask_glm.algorithms.proximal_grad(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.proximal_grad(dX_cupy, dy_cupy, max_iter=5)
    assert_array_equal(asnumpy(result_cupy), asnumpy(result))

    result = dask_glm.algorithms.newton(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.newton(dX_cupy, dy_cupy, max_iter=5)
    assert_array_equal(asnumpy(result_cupy), asnumpy(result))

    result = dask_glm.algorithms.lbfgs(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.lbfgs(dX_cupy, dy_cupy, max_iter=5)
    assert_array_equal(asnumpy(result_cupy), asnumpy(result))

    result = dask_glm.algorithms.gradient_descent(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.gradient_descent(dX_cupy, dy_cupy, max_iter=5)
    assert_array_equal(asnumpy(result_cupy), asnumpy(result))


def test_cupy_isclose_nep18():
    x = cupy.random.random((5000, 1000))
    assert_array_equal(np.isclose(x, x), asnumpy(cupy.isclose(x, x)))


def test_cupy_types_nep18():
    x = cupy.random.random((5000, 1000))
    assert cupy.can_cast(x, np.float16, 'safe')
    assert cupy.common_type(x, x) == cupy.float64


def test_dask_diag_nep18():
    x = cupy.random.random(5000)

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_array_almost_equal(asnumpy(np.diag(x)), asnumpy(da.diag(d).compute()))


def test_dask_bincount_nep18():
    x = cupy.random.randint(0, 100, size=(5000))

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_array_equal(asnumpy(np.bincount(x, minlength=10)), asnumpy(da.bincount(d, minlength=10).compute()))
    assert_array_equal(asnumpy(np.histogram(x, bins=10)), asnumpy(da.histogram(d, bins=10, range=[0, 99])))


def test_dask_roundings_nep18():
    x = (cupy.random.random(5000) + cupy.random.randint(-1, 1, 5000)) * 10.

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_array_equal(asnumpy(np.around(x, decimals=2)), asnumpy(da.around(d, decimals=2).compute()))
    assert_array_equal(asnumpy(np.ceil(x)), asnumpy(da.ceil(d).compute()))
    assert_array_equal(asnumpy(np.fix(x)), asnumpy(da.fix(d).compute()))
    assert_array_equal(asnumpy(np.floor(x)), asnumpy(da.floor(d).compute()))
    assert_array_equal(asnumpy(np.round(x)), asnumpy(da.round(d).compute()))


def test_dask_flatnonzero_nep18():
    x = cupy.random.random(5000) * cupy.random.randint(-1, 1, 5000)

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_array_equal(asnumpy(np.flatnonzero(x)), asnumpy(da.flatnonzero(d).compute()))


########################### PASSING ###########################


def test_dask_sum_nep18():
    x = cupy.random.random((1000, 1000))

    d = da.from_array(x, chunks=(100, 100), asarray=False)

    assert_eq(np.sum(x), np.sum(d))
    assert_eq(x.sum(), d.sum())


def test_dask_bitwise_nep18():
    x = cupy.random.randint(0, 100, size=(5000, 1000))

    d = da.from_array(x, chunks=(1000, 1000), asarray=False)

    assert_eq(np.bitwise_and(x, x), np.bitwise_and(d, d))
    assert_eq(np.bitwise_not(x), np.bitwise_not(d))
    assert_eq(np.bitwise_or(x, x), np.bitwise_or(d, d))
    assert_eq(np.bitwise_xor(x, x), np.bitwise_xor(d, d))


def test_dask_concatenate_nep18():
    x = cupy.random.random(5000)
    y = cupy.random.random(5000)
    z = cupy.asnumpy(y)

    d = da.from_array((x, z), chunks=(1000), asarray=False)

    #assert_eq(np.concatenate((x, y)), da.concatenate(d).compute())
    assert_array_equal(asnumpy(np.concatenate((x,y))), asnumpy(da.concatenate(d).compute()))


def test_dask_copysign_nep18():
    N = 5000000
    C = 1000000

    x = cupy.random.random(N)
    y = cupy.random.randint(-1, 1, N)

    dx = da.from_array(x, chunks=(C), asarray=False)
    dy = da.from_array(y, chunks=(C), asarray=False)

    assert_eq(np.copysign(x, y), np.copysign(dx, dy))


def test_dask_count_nonzero_nep18():
    x = cupy.random.randint(0, 100, size=(5000))

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_eq(np.count_nonzero(x), np.count_nonzero(d))


def test_dask_deg_nep18():
    x = cupy.random.random(5000)

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_eq(np.deg2rad(x), np.deg2rad(d))
    assert_eq(np.degrees(x), np.degrees(d))


def test_dask_trig_nep18():
    N = 5000
    C = 1000

    x = cupy.random.random(N)

    d = da.from_array(x, chunks=(C), asarray=False)

    fn_list = ['cos', 'sin', 'tan']

    for fn in fn_list:
        assert_eq(getattr(np, fn)(x), getattr(np, fn)(d))
        assert_eq(getattr(np, fn + 'h')(x), getattr(np, fn + 'h')(d))
        assert_eq(getattr(np, 'arc' + fn)(x), getattr(np, 'arc' + fn)(d))
        #assert_eq(getattr(np, 'arc' + fn + 'h')(x), getattr(np, 'arc' + fn + 'h')(d))
        assert_array_equal(asnumpy(getattr(np, 'arc' + fn + 'h')(x)),
                           asnumpy(getattr(np, 'arc' + fn + 'h')(d).compute()))

    assert_eq(asnumpy(getattr(np, 'arctan2')(x, x)), asnumpy(getattr(da, 'arctan2')(d, d).compute()))


def test_dask_dot_nep18():
    x = cupy.random.random(5000000)

    d = da.from_array(x, chunks=(1000000), asarray=False)

    assert_eq(np.dot(x, x), np.dot(d, d))


def test_dask_div_nep18():
    x = cupy.random.random(5000)

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_eq(np.divide(x, x), np.divide(d, d))


def test_dask_exp_nep18():
    x = cupy.random.random(5000)

    d = da.from_array(x, chunks=(1000), asarray=False)

    assert_eq(np.exp(x), np.exp(d))
    assert_eq(np.expm1(x), np.expm1(d))


def test_dask_comparison_nep18():
    x = cupy.random.random(5000)
    y = cupy.random.random(5000)

    dx = da.from_array(x, chunks=(1000), asarray=False)
    dy = da.from_array(y, chunks=(1000), asarray=False)

    assert_eq(np.fmax(x, y), np.fmax(dx, dy))
    assert_eq(np.fmin(x, y), np.fmin(dx, dy))
    assert_eq(np.fmod(x, y), np.fmod(dx, dy))


def test_dask_flip_nep18():
    x = cupy.random.random((50, 50))

    d = da.from_array(x, chunks=(10, 10), asarray=False)

    assert_eq(np.flip(x, axis=0), np.flip(d, axis=0))
    assert_eq(np.flip(x, axis=1), np.flip(d, axis=1))
    assert_eq(np.fliplr(x), np.fliplr(d))
    assert_eq(np.flipud(x), np.flipud(d))


def test_dask_fp_construct_nep18():
    x = cupy.random.random(5000)
    x1, x2 = np.frexp(x)
    xr = np.ldexp(x1, x2)

    d = da.from_array(x, chunks=(1000), asarray=False)
    d1, d2 = da.frexp(d)
    dr = da.ldexp(d1, d2).compute()
    d1 = d1.compute()
    d2 = d2.compute()

    assert_eq(x1, d1)
    assert_eq(x2, d2)
    assert_eq(xr, dr)


def test_dask_svd_nep18():
    x = cupy.random.random((5000, 1000))
    # Dask doesn't support full_matrices
    # https://github.com/dask/dask/issues/3576
    u_cupy, s_cupy, v_cupy = np.linalg.svd(x, full_matrices=False)

    d = da.from_array(x, chunks=(1000, 1000), asarray=False)
    u, s, v = np.linalg.svd(d)

    assert_eq(s_cupy, s)
    assert_eq(u_cupy.shape, u.shape)
    assert_eq(v_cupy.shape, v.shape)


def test_dask_qr_nep18():
    import numpy as np
    import cupy
    import dask.array as da
    from dask.array.utils import assert_eq

    x = cupy.random.random((5000, 1000))

    d = da.from_array(x, chunks=(1000, 1000), asarray=False)

    q, r = da.linalg.qr(d)
    q_cupy, r_cupy = np.linalg.qr(x)
    assert_eq(x, np.dot(q_cupy, r_cupy))
    assert_eq(d, np.dot(q, r))

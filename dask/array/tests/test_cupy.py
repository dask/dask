import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq, IS_NEP18_ACTIVE, AxisError

missing_arrfunc_cond = not IS_NEP18_ACTIVE
missing_arrfunc_reason = "NEP-18 support is not available in NumPy"

cupy = pytest.importorskip('cupy')


functions = [
    lambda x: x,
    lambda x: da.expm1(x),
    lambda x: 2 * x,
    lambda x: x / 2,
    lambda x: x**2,
    lambda x: x + x,
    lambda x: x * x,
    lambda x: x[0],
    lambda x: x[:, 1],
    lambda x: x[:1, None, 1:3],
    lambda x: x.T,
    lambda x: da.transpose(x, (1, 2, 0)),
    lambda x: x.sum(),
    pytest.param(lambda x: x.dot(np.arange(x.shape[-1])),
                 marks=pytest.mark.xfail(reason='cupy.dot(numpy) fails')),
    pytest.param(lambda x: x.dot(np.eye(x.shape[-1])),
                 marks=pytest.mark.xfail(reason='cupy.dot(numpy) fails')),
    pytest.param(lambda x: da.tensordot(x, np.ones(x.shape[:2]), axes=[(0, 1), (0, 1)]),
                 marks=pytest.mark.xfail(reason='cupy.dot(numpy) fails')),
    lambda x: x.sum(axis=0),
    lambda x: x.max(axis=0),
    lambda x: x.sum(axis=(1, 2)),
    lambda x: x.astype(np.complex128),
    lambda x: x.map_blocks(lambda x: x * 2),
    pytest.param(lambda x: x.round(1),
                 marks=pytest.mark.xfail(reason="cupy doesn't support round")),
    lambda x: x.reshape((x.shape[0] * x.shape[1], x.shape[2])),
    # Rechunking here is required, see https://github.com/dask/dask/issues/2561
    lambda x: (x.rechunk(x.shape)).reshape((x.shape[1], x.shape[0], x.shape[2])),
    lambda x: x.reshape((x.shape[0], x.shape[1], x.shape[2] / 2, x.shape[2] / 2)),
    lambda x: abs(x),
    lambda x: x > 0.5,
    lambda x: x.rechunk((4, 4, 4)),
    lambda x: x.rechunk((2, 2, 1)),
    pytest.param(lambda x: da.einsum("ijk,ijk", x, x),
                 marks=pytest.mark.xfail(
                     reason='depends on resolution of https://github.com/numpy/numpy/issues/12974'))
]


@pytest.mark.parametrize('func', functions)
def test_basic(func):
    c = cupy.random.random((2, 3, 4))
    n = c.get()
    dc = da.from_array(c, chunks=(1, 2, 2), asarray=False)
    dn = da.from_array(n, chunks=(1, 2, 2))

    ddc = func(dc)
    ddn = func(dn)

    assert_eq(ddc, ddn)

    if ddc.shape:
        result = ddc.compute(scheduler='single-threaded')
        assert isinstance(result, cupy.ndarray)


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
def test_diag():
    v = cupy.arange(11)
    assert_eq(da.diag(v), cupy.diag(v))

    v = v + v + 3
    darr = da.diag(v)
    cupyarr = cupy.diag(v)
    assert_eq(darr, cupyarr)

    x = cupy.arange(64).reshape((8, 8))
    assert_eq(da.diag(x), cupy.diag(x))


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
def test_diagonal():
    v = cupy.arange(11)
    with pytest.raises(ValueError):
        da.diagonal(v)

    v = cupy.arange(4).reshape((2, 2))
    with pytest.raises(ValueError):
        da.diagonal(v, axis1=0, axis2=0)

    with pytest.raises(AxisError):
        da.diagonal(v, axis1=-4)

    with pytest.raises(AxisError):
        da.diagonal(v, axis2=-4)

    v = cupy.arange(4 * 5 * 6).reshape((4, 5, 6))
    v = da.from_array(v, chunks=2, asarray=False)
    assert_eq(da.diagonal(v), np.diagonal(v))
    # Empty diagonal.
    assert_eq(da.diagonal(v, offset=10), np.diagonal(v, offset=10))
    assert_eq(da.diagonal(v, offset=-10), np.diagonal(v, offset=-10))
    assert isinstance(da.diagonal(v).compute(), cupy.core.core.ndarray)

    with pytest.raises(ValueError):
        da.diagonal(v, axis1=-2)

    # Negative axis.
    assert_eq(da.diagonal(v, axis1=-1), np.diagonal(v, axis1=-1))
    assert_eq(da.diagonal(v, offset=1, axis1=-1), np.diagonal(v, offset=1, axis1=-1))

    # Heterogenous chunks.
    v = cupy.arange(2 * 3 * 4 * 5 * 6).reshape((2, 3, 4, 5, 6))
    v = da.from_array(v, chunks=(1, (1, 2), (1, 2, 1), (2, 1, 2), (5, 1)), asarray=False)

    assert_eq(da.diagonal(v), np.diagonal(v))
    assert_eq(da.diagonal(v, offset=2, axis1=3, axis2=1),
              np.diagonal(v, offset=2, axis1=3, axis2=1))

    assert_eq(da.diagonal(v, offset=-2, axis1=3, axis2=1),
              np.diagonal(v, offset=-2, axis1=3, axis2=1))

    assert_eq(da.diagonal(v, offset=-2, axis1=3, axis2=4),
              np.diagonal(v, offset=-2, axis1=3, axis2=4))

    assert_eq(da.diagonal(v, 1), np.diagonal(v, 1))
    assert_eq(da.diagonal(v, -1), np.diagonal(v, -1))
    # Positional arguments
    assert_eq(da.diagonal(v, 1, 2, 1), np.diagonal(v, 1, 2, 1))


@pytest.mark.xfail(reason="no shape argument support *_like functions on CuPy yet")
@pytest.mark.skipif(np.__version__ < '1.17', reason='no shape argument for *_like functions')
@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
def test_tril_triu():
    A = cupy.random.randn(20, 20)
    for chk in [5, 4]:
        dA = da.from_array(A, (chk, chk), asarray=False)

        assert np.allclose(da.triu(dA).compute(), np.triu(A))
        assert np.allclose(da.tril(dA).compute(), np.tril(A))

        for k in [-25, -20, -19, -15, -14, -9, -8, -6, -5, -1,
                  1, 4, 5, 6, 8, 10, 11, 15, 16, 19, 20, 21]:
            assert np.allclose(da.triu(dA, k).compute(), np.triu(A, k))
            assert np.allclose(da.tril(dA, k).compute(), np.tril(A, k))


@pytest.mark.xfail(reason="no shape argument support *_like functions on CuPy yet")
@pytest.mark.skipif(np.__version__ < '1.17', reason='no shape argument for *_like functions')
@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
def test_tril_triu_non_square_arrays():
    A = cupy.random.randint(0, 11, (30, 35))
    dA = da.from_array(A, chunks=(5, 5), asarray=False)
    assert_eq(da.triu(dA), np.triu(A))
    assert_eq(da.tril(dA), np.tril(A))

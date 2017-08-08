import pytest
pytest.importorskip('numpy')

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq


def test_linspace():
    darr = da.linspace(6, 49, chunks=5)
    nparr = np.linspace(6, 49)
    assert_eq(darr, nparr)

    darr = da.linspace(1.4, 4.9, chunks=5, num=13)
    nparr = np.linspace(1.4, 4.9, num=13)
    assert_eq(darr, nparr)

    darr = da.linspace(6, 49, chunks=5, dtype=float)
    nparr = np.linspace(6, 49, dtype=float)
    assert_eq(darr, nparr)

    darr = da.linspace(1.4, 4.9, chunks=5, num=13, dtype=int)
    nparr = np.linspace(1.4, 4.9, num=13, dtype=int)
    assert_eq(darr, nparr)
    assert (sorted(da.linspace(1.4, 4.9, chunks=5, num=13).dask) ==
            sorted(da.linspace(1.4, 4.9, chunks=5, num=13).dask))
    assert (sorted(da.linspace(6, 49, chunks=5, dtype=float).dask) ==
            sorted(da.linspace(6, 49, chunks=5, dtype=float).dask))


def test_arange():
    darr = da.arange(77, chunks=13)
    nparr = np.arange(77)
    assert_eq(darr, nparr)

    darr = da.arange(2, 13, chunks=5)
    nparr = np.arange(2, 13)
    assert_eq(darr, nparr)

    darr = da.arange(4, 21, 9, chunks=13)
    nparr = np.arange(4, 21, 9)
    assert_eq(darr, nparr)

    # negative steps
    darr = da.arange(53, 5, -3, chunks=5)
    nparr = np.arange(53, 5, -3)
    assert_eq(darr, nparr)

    darr = da.arange(77, chunks=13, dtype=float)
    nparr = np.arange(77, dtype=float)
    assert_eq(darr, nparr)

    darr = da.arange(2, 13, chunks=5, dtype=int)
    nparr = np.arange(2, 13, dtype=int)
    assert_eq(darr, nparr)
    assert (sorted(da.arange(2, 13, chunks=5).dask) ==
            sorted(da.arange(2, 13, chunks=5).dask))
    assert (sorted(da.arange(77, chunks=13, dtype=float).dask) ==
            sorted(da.arange(77, chunks=13, dtype=float).dask))

    # 0 size output
    darr = da.arange(0, 1, -0.5, chunks=20)
    nparr = np.arange(0, 1, -0.5)
    assert_eq(darr, nparr)

    darr = da.arange(0, -1, 0.5, chunks=20)
    nparr = np.arange(0, -1, 0.5)
    assert_eq(darr, nparr)


def test_arange_has_dtype():
    assert da.arange(5, chunks=2).dtype == np.arange(5).dtype


@pytest.mark.xfail(reason="Casting floats to ints is not supported since edge"
                          "behavior is not specified or guaranteed by NumPy.")
def test_arange_cast_float_int_step():
    darr = da.arange(3.3, -9.1, -.25, chunks=3, dtype='i8')
    nparr = np.arange(3.3, -9.1, -.25, dtype='i8')
    assert_eq(darr, nparr)


def test_arange_float_step():
    darr = da.arange(2., 13., .3, chunks=4)
    nparr = np.arange(2., 13., .3)
    assert_eq(darr, nparr)

    darr = da.arange(7.7, 1.5, -.8, chunks=3)
    nparr = np.arange(7.7, 1.5, -.8)
    assert_eq(darr, nparr)

    darr = da.arange(0, 1, 0.01, chunks=20)
    nparr = np.arange(0, 1, 0.01)
    assert_eq(darr, nparr)

    darr = da.arange(0, 1, 0.03, chunks=20)
    nparr = np.arange(0, 1, 0.03)
    assert_eq(darr, nparr)


def test_indices_no_chunks():
    with pytest.raises(ValueError):
        da.indices((1,))


def test_indices_wrong_chunks():
    with pytest.raises(ValueError):
        da.indices((1,), chunks=tuple())


def test_empty_indicies():
    darr = da.indices(tuple(), chunks=tuple())
    nparr = np.indices(tuple())
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)

    darr = da.indices(tuple(), float, chunks=tuple())
    nparr = np.indices(tuple(), float)
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)

    darr = da.indices((0,), float, chunks=(1,))
    nparr = np.indices((0,), float)
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)

    darr = da.indices((0, 1, 2), float, chunks=(1, 1, 2))
    nparr = np.indices((0, 1, 2), float)
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)


def test_indicies():
    darr = da.indices((1,), chunks=(1,))
    nparr = np.indices((1,))
    assert_eq(darr, nparr)

    darr = da.indices((1,), float, chunks=(1,))
    nparr = np.indices((1,), float)
    assert_eq(darr, nparr)

    darr = da.indices((2, 1), chunks=(2, 1))
    nparr = np.indices((2, 1))
    assert_eq(darr, nparr)

    darr = da.indices((2, 3), chunks=(1, 2))
    nparr = np.indices((2, 3))
    assert_eq(darr, nparr)


def test_tril_triu():
    A = np.random.randn(20, 20)
    for chk in [5, 4]:
        dA = da.from_array(A, (chk, chk))

        assert np.allclose(da.triu(dA).compute(), np.triu(A))
        assert np.allclose(da.tril(dA).compute(), np.tril(A))

        for k in [-25, -20, -19, -15, -14, -9, -8, -6, -5, -1,
                  1, 4, 5, 6, 8, 10, 11, 15, 16, 19, 20, 21]:
            assert np.allclose(da.triu(dA, k).compute(), np.triu(A, k))
            assert np.allclose(da.tril(dA, k).compute(), np.tril(A, k))


def test_tril_triu_errors():
    A = np.random.randint(0, 11, (10, 10, 10))
    dA = da.from_array(A, chunks=(5, 5, 5))
    pytest.raises(ValueError, lambda: da.triu(dA))

    A = np.random.randint(0, 11, (30, 35))
    dA = da.from_array(A, chunks=(5, 5))
    pytest.raises(NotImplementedError, lambda: da.triu(dA))


def test_eye():
    assert_eq(da.eye(9, chunks=3), np.eye(9))
    assert_eq(da.eye(10, chunks=3), np.eye(10))
    assert_eq(da.eye(9, chunks=3, M=11), np.eye(9, M=11))
    assert_eq(da.eye(11, chunks=3, M=9), np.eye(11, M=9))
    assert_eq(da.eye(7, chunks=3, M=11), np.eye(7, M=11))
    assert_eq(da.eye(11, chunks=3, M=7), np.eye(11, M=7))
    assert_eq(da.eye(9, chunks=3, k=2), np.eye(9, k=2))
    assert_eq(da.eye(9, chunks=3, k=-2), np.eye(9, k=-2))
    assert_eq(da.eye(7, chunks=3, M=11, k=5), np.eye(7, M=11, k=5))
    assert_eq(da.eye(11, chunks=3, M=7, k=-6), np.eye(11, M=7, k=-6))
    assert_eq(da.eye(6, chunks=3, M=9, k=7), np.eye(6, M=9, k=7))
    assert_eq(da.eye(12, chunks=3, M=6, k=-3), np.eye(12, M=6, k=-3))

    assert_eq(da.eye(9, chunks=3, dtype=int), np.eye(9, dtype=int))
    assert_eq(da.eye(10, chunks=3, dtype=int), np.eye(10, dtype=int))


def test_diag():
    v = np.arange(11)
    assert_eq(da.diag(v), np.diag(v))

    v = da.arange(11, chunks=3)
    darr = da.diag(v)
    nparr = np.diag(v)
    assert_eq(darr, nparr)
    assert sorted(da.diag(v).dask) == sorted(da.diag(v).dask)

    v = v + v + 3
    darr = da.diag(v)
    nparr = np.diag(v)
    assert_eq(darr, nparr)

    v = da.arange(11, chunks=11)
    darr = da.diag(v)
    nparr = np.diag(v)
    assert_eq(darr, nparr)
    assert sorted(da.diag(v).dask) == sorted(da.diag(v).dask)

    x = np.arange(64).reshape((8, 8))
    assert_eq(da.diag(x), np.diag(x))

    d = da.from_array(x, chunks=(4, 4))
    assert_eq(da.diag(d), np.diag(x))

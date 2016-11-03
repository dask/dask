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


def test_arange_has_dtype():
    assert da.arange(5, chunks=2).dtype == np.arange(5).dtype


def test_arange_working_float_step():
    """Sometimes floating point step arguments work, but this could be platform
    dependent.
    """
    darr = da.arange(3.3, -9.1, -.25, chunks=3)
    nparr = np.arange(3.3, -9.1, -.25)
    assert_eq(darr, nparr)


@pytest.mark.xfail(reason="Casting floats to ints is not supported since edge"
                          "behavior is not specified or guaranteed by NumPy.")
def test_arange_cast_float_int_step():
    darr = da.arange(3.3, -9.1, -.25, chunks=3, dtype='i8')
    nparr = np.arange(3.3, -9.1, -.25, dtype='i8')
    assert_eq(darr, nparr)


@pytest.mark.xfail(reason="arange with a floating point step value can fail"
                          "due to numerical instability.")
def test_arange_float_step():
    darr = da.arange(2., 13., .3, chunks=4)
    nparr = np.arange(2., 13., .3)
    assert_eq(darr, nparr)

    darr = da.arange(7.7, 1.5, -.8, chunks=3)
    nparr = np.arange(7.7, 1.5, -.8)
    assert_eq(darr, nparr)

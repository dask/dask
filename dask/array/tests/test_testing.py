from __future__ import annotations

import sys

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq


@pytest.mark.skipif(bool(sys.flags.optimize), reason="Assertions disabled.")
def test_assert_eq_checks_scalars():
    # https://github.com/dask/dask/issues/2680
    with pytest.raises(AssertionError):
        assert_eq(np.array(0), np.array(1))

    a = da.from_array(np.array([0]), 1)[0]
    b = np.array([1])[0]
    with pytest.raises(AssertionError):
        assert_eq(a, b)


def test_assert_eq_array():
    x = np.arange(6).reshape((2, 3))
    d = da.from_array(x, chunks=(1, 3))
    assert assert_eq(d, x)
    assert assert_eq(x, x)
    assert assert_eq(d, d)


@pytest.mark.skipif(bool(sys.flags.optimize), reason="Assertions disabled.")
def test_assert_eq_checks_shape():
    with pytest.raises(AssertionError):
        assert_eq(np.array([1, 2]), np.array([1, 2, 3]))


@pytest.mark.skipif(bool(sys.flags.optimize), reason="Assertions disabled.")
def test_assert_eq_check_dtype():
    a = np.ones(3, dtype="i4")
    b = np.ones(3, dtype="f8")
    with pytest.raises(AssertionError):
        assert_eq(a, b)
    # values are equal, so disabling the dtype check should pass
    assert assert_eq(a, b, check_dtype=False)


@pytest.mark.skipif(bool(sys.flags.optimize), reason="Assertions disabled.")
def test_assert_eq_equal_nan():
    a = np.array([1.0, np.nan])
    b = np.array([1.0, np.nan])
    # NaNs compare equal by default
    assert assert_eq(a, b)
    with pytest.raises(AssertionError):
        assert_eq(a, b, equal_nan=False)

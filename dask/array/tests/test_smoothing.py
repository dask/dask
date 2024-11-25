from __future__ import annotations

import pytest

import dask.array as da
from dask.array.smoothing import moving_average


def test_moving_average():
    data = da.from_array([1, 2, 3, 4, 5], chunks=2)
    result = moving_average(data, window_size=3).compute()
    expected = [2.0, 3.0, 4.0]
    assert all(abs(r - e) < 1e-6 for r, e in zip(result, expected))

    data = da.from_array([1, 2, 3, 4, 5], chunks=2)
    result = moving_average(data, window_size=1).compute()
    expected = [1, 2, 3, 4, 5]
    assert all(abs(r - e) < 1e-6 for r, e in zip(result, expected))

    data = da.from_array([1, 2, 3, 4, 5], chunks=2)
    result = moving_average(data, window_size=5).compute()
    expected = [3.0]
    assert all(abs(r - e) < 1e-6 for r, e in zip(result, expected))

    data = da.from_array([1, 2, 3], chunks=2)
    with pytest.raises(
        ValueError, match="Window size cannot be larger than the data size"
    ):
        moving_average(data, window_size=4)


def test_invalid_window_size():
    data = da.from_array([1, 2, 3], chunks=3)
    with pytest.raises(ValueError):
        moving_average(data, window_size=0)


def test_window_size_type():
    data = da.from_array([1, 2, 3], chunks=3)
    with pytest.raises(TypeError):
        moving_average(data, window_size=1.5)

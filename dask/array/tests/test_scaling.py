import pytest
import dask.array as da
from dask.array.scaling import min_max_scale
from dask.array.scaling import l2_normalize

def test_min_max_scale():
    data = da.from_array([1, 2, 3, 4, 5], chunks=2)
    result = min_max_scale(data).compute()
    expected = [0.0, 0.25, 0.5, 0.75, 1.0]
    assert all(abs(r - e) < 1e-6 for r, e in zip(result, expected))

def test_min_max_scale_constant_values():
    data = da.from_array([5, 5, 5], chunks=2)
    with pytest.raises(ValueError, match="Cannot scale data with constant values."):
        min_max_scale(data).compute()

def test_l2_normalize():
    data = da.from_array([[1, 2], [3, 4]], chunks=2)
    result = l2_normalize(data, axis=1).compute()
    expected = [[0.447214, 0.894427], [0.6, 0.8]]
    for r_row, e_row in zip(result, expected):
        assert all(abs(r - e) < 1e-6 for r, e in zip(r_row, e_row))

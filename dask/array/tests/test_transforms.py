import pytest
import dask.array as da
from dask.array.transforms import log_transform, binarize

def test_log_transform():
    data = da.from_array([1, 10, 100, 1000], chunks=2)
    result = log_transform(data).compute()
    expected = [0.0, 2.302585, 4.60517, 6.907755]
    assert all(abs(r - e) < 1e-6 for r, e in zip(result, expected))

def test_log_transform_invalid_values():
    data = da.from_array([-1, 0, 1], chunks=2)
    with pytest.raises(ValueError, match="Log transformation is only defined for positive values."):
        log_transform(data).compute()
    
def test_binarize():
    data = da.from_array([-1, 0, 1, 2, 3], chunks=2)
    result = binarize(data, threshold=1).compute()
    expected = [0, 0, 0, 1, 1]
    assert all(r == e for r, e in zip(result, expected))

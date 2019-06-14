import dask.array as da
import numpy as np
import pytest

from dask.array.utils import normalize_meta


@pytest.mark.parametrize("func", [
    np.asarray,
    da.asarray,
])
def test_normalize_meta(func):
    x = np.ones((1, 2, 3), dtype='float32')
    x = func(x)

    assert normalize_meta(x).shape == (0, 0, 0)
    assert normalize_meta(x).dtype == 'float32'
    assert type(normalize_meta(x)) is type(x)

    assert normalize_meta(x, ndim=2).shape == (0, 0)
    assert normalize_meta(x, ndim=4).shape == (0, 0, 0, 0)
    assert normalize_meta(x, dtype="float64").dtype == "float64"

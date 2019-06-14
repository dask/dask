import dask.array as da
import numpy as np
import pytest

from dask.array.utils import meta_from_array

funcs = [np.asarray]
try:
    import sparse
    funcs.append(sparse.COO.from_numpy)
except ImportError:
    pass


@pytest.mark.parametrize("func", funcs)
def test_meta_from_array(func):
    x = np.ones((1, 2, 3), dtype='float32')
    x = func(x)

    assert meta_from_array(x).shape == (0, 0, 0)
    assert meta_from_array(x).dtype == 'float32'
    assert type(meta_from_array(x)) is type(x)

    assert meta_from_array(x, ndim=2).shape == (0, 0)
    assert meta_from_array(x, ndim=4).shape == (0, 0, 0, 0)
    assert meta_from_array(x, dtype="float64").dtype == "float64"

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import meta_from_array

asarrays = [np.asarray]

try:
    import sparse
    asarrays.append(sparse.COO.from_numpy)
except ImportError:
    pass

try:
    import cupy
    asarrays.append(cupy.asarray)
except ImportError:
    pass


@pytest.mark.parametrize("asarray", asarrays)
def test_meta_from_array(asarray):
    x = np.array(1)
    assert meta_from_array(x, ndim=1).shape == (0,)

    x = np.ones((1, 2, 3), dtype='float32')
    x = asarray(x)

    assert meta_from_array(x).shape == (0, 0, 0)
    assert meta_from_array(x).dtype == 'float32'
    assert type(meta_from_array(x)) is type(x)

    assert meta_from_array(x, ndim=2).shape == (0, 0)
    assert meta_from_array(x, ndim=4).shape == (0, 0, 0, 0)
    assert meta_from_array(x, dtype="float64").dtype == "float64"

    x = da.ones((1,))
    assert isinstance(meta_from_array(x), np.ndarray)

    assert meta_from_array(123) == 123
    assert meta_from_array('foo') == 'foo'
    assert meta_from_array(np.dtype('float32')) == np.dtype('float32')

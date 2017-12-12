from distutils.version import LooseVersion

import pytest
import numpy as np

import dask.array as da
from dask.array.numpy_compat import _make_sliced_dtype
from dask.array.utils import assert_eq

NP_LE_114 = LooseVersion(np.__version__) <= LooseVersion("1.14")

skip_if_np_ge_114 = pytest.mark.skipif(NP_LE_114,
                                       reason="NumPy is older than '1.14'.")
skip_if_np_lt_114 = pytest.mark.skipif(not NP_LE_114,
                                       reason="NumPy is at least '1.14'.")


@pytest.fixture(params=[
    [('A', ('f4', (3, 2))), ('B', ('f4', 3)), ('C', ('f8', 3))],
    [('A', ('i4', (3, 2))), ('B', ('f4', 3)), ('C', ('S4', 3))],
])
def dtype(request):
    return np.dtype(request.param)


@pytest.fixture(params=[
    ['A'],
    ['A', 'B'],
    ['A', 'B', 'C'],
])
def index(request):
    return request.param


@skip_if_np_ge_114
def test_basic():
    # sanity check
    dtype = [('a', 'f8'), ('b', 'f8'), ('c', 'f8')]
    x = np.ones((5, 3), dtype=dtype)
    dx = da.ones((5, 3), dtype=dtype, chunks=3)
    result = dx[['a', 'b']]
    expected = x[['a', 'b']]
    assert_eq(result, expected)

    expected_dtype = np.dtype({'names': ['a', 'b'],
                               'formats': ['<f8', '<f8'],
                               'offsets': [0, 8], 'itemsize': 24})
    assert result.dtype == expected_dtype


@skip_if_np_lt_114
def test_basic_old():
    dtype = [('a', 'f8'), ('b', 'f8'), ('c', 'f8')]
    x = np.ones((5, 3), dtype=dtype)
    dx = da.ones((5, 3), dtype=dtype, chunks=3)
    result = dx[['a', 'b']]
    expected = x[['a', 'b']]
    assert_eq(result, expected)

    assert result.dtype == dtype[:2]


def test_slice_dtype(dtype, index):
    result = _make_sliced_dtype(dtype, index)
    expected = np.ones((5, len(dtype)), dtype=dtype)[index].dtype
    assert result == expected

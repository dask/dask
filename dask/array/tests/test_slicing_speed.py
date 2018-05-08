import pytest
np = pytest.importorskip('numpy')

from dask.array.slicing import _slice_1d


def test_slice_1d_speed_slice_head():
    dims = 100000
    chunks = [1] * dims
    _slice_1d(dims, chunks, slice(10, 51, None))


def test_slice_1d_speed_slice_tail():
    dims = 100000
    chunks = [1] * dims
    _slice_1d(dims, chunks, slice(dims - 40, dims - 10, None))


def test_slice_1d_speed_slice_mid():
    dims = 100000
    chunks = [1] * dims
    _slice_1d(dims, chunks, slice(dims // 2, dims // 2 + 10, None))


def test_slice_1d_speed_int_tail():
    dims = 100000
    chunks = [1] * dims
    _slice_1d(dims, chunks, dims - 10)


def test_slice_1d_speed_int_mid():
    dims = 100000
    chunks = [1] * dims
    _slice_1d(dims, chunks, dims // 2)


def test_slice_1d_speed_int_head():
    dims = 100000
    chunks = [1] * dims
    _slice_1d(dims, chunks, 10)

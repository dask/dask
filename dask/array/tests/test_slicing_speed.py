import itertools
from operator import getitem

import pytest
from toolz import merge

np = pytest.importorskip('numpy')

import dask
import dask.array as da
from dask.array.slicing import (_sanitize_index_element, _slice_1d,
                                new_blockdim, sanitize_index, slice_array,
                                take, normalize_index)
from dask.array.utils import assert_eq, same_keys
def test_slice_1d_speed_slice_head():
    dims = 100000
    chunks = [1] * dims
    r1 = _slice_1d(dims, chunks, slice(10, 51, None))

def test_slice_1d_speed_slice_tail():
    dims = 100000
    chunks = [1] * dims
    r1 = _slice_1d(dims, chunks, slice(dims-40, dims-10, None))

def test_slice_1d_speed_slice_mid():
    dims = 100000
    chunks = [1] * dims
    r1 = _slice_1d(dims, chunks, slice(dims // 2, dims // 2 +10, None))

def test_slice_1d_speed_int_tail():
    dims = 100000
    chunks = [1] * dims
    r1 = _slice_1d(dims, chunks, dims-10)

def test_slice_1d_speed_int_mid():
    dims = 100000
    chunks = [1] * dims
    r1 = _slice_1d(dims, chunks, dims // 2)

def test_slice_1d_speed_int_head():
    dims = 100000
    chunks = [1] * dims
    r1 = _slice_1d(dims, chunks, 10)

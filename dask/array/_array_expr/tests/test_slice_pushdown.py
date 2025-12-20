"""Tests for slice pushdown into IO expressions."""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array._array_expr._io import FromArray
from dask.array._array_expr._slicing import SliceSlicesIntegers
from dask.array.utils import assert_eq


# Parametrized correctness tests: (array_shape, chunks, slice_tuple)
SLICE_CASES = [
    # Basic slices
    ((10, 10), (2, 2), (slice(0, 2), slice(0, 2))),  # corner
    ((10, 10), (2, 2), (slice(0, 4), slice(0, 4))),  # 2x2 chunks
    ((10, 10), (5, 5), (slice(0, 5), slice(0, 5))),  # chunk boundary
    ((10, 10), (5, 5), (slice(2, 7), slice(3, 8))),  # mid-chunk
    ((10, 10), (5, 5), (slice(None), slice(None))),  # full slice
    # Edge cases
    ((10, 10), (2, 2), (slice(5, 5), slice(None))),  # empty
    ((10, 10), (2, 2), (slice(3, 4), slice(None))),  # single row
    ((10, 10), (2, 2), (slice(-4, -1), slice(-3, None))),  # negative
    ((10, 10, 10), (3, 3, 3), (slice(1, 4), slice(2, 5), slice(3, 6))),  # 3D
    # Adversarial
    ((10, 10), (10, 10), (slice(2, 5), slice(3, 7))),  # single chunk source
    ((10, 10), (3, 4), (slice(1, 8), slice(2, 9))),  # uneven chunks
    ((10, 10), (3, 3), (slice(9, None), slice(9, None))),  # last chunk
    ((10, 10, 10), (3, 3, 3), (slice(2, 5),)),  # partial dims
]


@pytest.mark.parametrize("shape,chunks,slc", SLICE_CASES)
def test_slice_correctness(shape, chunks, slc):
    """Sliced dask array matches sliced numpy array."""
    arr = np.arange(np.prod(shape)).reshape(shape)
    x = da.from_array(arr, chunks=chunks)
    assert_eq(x[slc], arr[slc])


# Task count tests: (array_shape, chunks, slice_tuple, expected_tasks)
TASK_COUNT_CASES = [
    ((10, 10), (2, 2), (slice(0, 2), slice(0, 2)), 1),  # 1 chunk
    ((10, 10), (2, 2), (slice(0, 4), slice(0, 4)), 4),  # 2x2 chunks
    ((10, 10), (5, 5), (slice(0, 5), slice(0, 5)), 1),  # boundary
    ((10, 10), (5, 5), (slice(2, 7), slice(3, 8)), 4),  # spans 2x2
    ((20, 20), (5, 5), (slice(0, 3), slice(None)), 4),  # 1x4 row
    ((20, 20), (5, 5), (slice(None), slice(0, 3)), 4),  # 4x1 col
    ((20, 20), (5, 5), (slice(0, 12), slice(0, 12)), 9),  # 3x3
    ((100, 100), (10, 10), (slice(0, 5), slice(0, 5)), 1),  # small from large
    ((10, 10, 10), (5, 5, 5), (slice(0, 3), slice(0, 3), slice(0, 3)), 1),  # 3D corner
]


@pytest.mark.parametrize("shape,chunks,slc,expected", TASK_COUNT_CASES)
def test_task_count(shape, chunks, slc, expected):
    """After optimization, task count equals chunks touched."""
    arr = np.arange(np.prod(shape)).reshape(shape)
    x = da.from_array(arr, chunks=chunks)
    y = x[slc].optimize()
    assert len(y.__dask_graph__()) == expected


def test_slice_optimize_slice():
    """Slice, optimize, slice again works correctly."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(2, 2))

    y = x[0:6, 0:6].optimize()
    assert len(y.__dask_graph__()) == 9  # 3x3 chunks

    z = y[0:2, 0:2].optimize()
    assert len(z.__dask_graph__()) == 1  # 1 chunk

    assert_eq(z, arr[0:6, 0:6][0:2, 0:2])


def test_slice_through_elemwise():
    """Slice pushes through elemwise into IO."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(2, 2))
    y = ((x + 1) * 2)[0:2, 0:2].optimize()
    assert len(y.__dask_graph__()) <= 2
    assert_eq(y, ((arr + 1) * 2)[0:2, 0:2])


def test_nested_slices():
    """Nested slices fuse."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(2, 2))
    y = x[1:8, 2:9][1:4, 1:4]
    assert_eq(y, arr[1:8, 2:9][1:4, 1:4])


def test_expression_structure():
    """Verify expression types before/after optimization."""
    x = da.from_array(np.arange(100).reshape(10, 10), chunks=(2, 2))
    y = x[0:2, 0:2]

    assert isinstance(y.expr, SliceSlicesIntegers)
    assert isinstance(y.optimize().expr, FromArray)


def test_steps_and_reverse():
    """Slices with steps still compute correctly."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(2, 2))

    assert_eq(x[::2, ::2], arr[::2, ::2])
    assert_eq(x[::-1, ::-1], arr[::-1, ::-1])
    assert_eq(x[::5, ::5], arr[::5, ::5])


def test_non_pushdown_cases():
    """Integer indexing, fancy indexing, newaxis don't break."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(2, 2))

    assert_eq(x[5, :], arr[5, :])
    assert_eq(x[[1, 3, 5], :], arr[[1, 3, 5], :])
    assert_eq(x[None, :5, :5], arr[None, :5, :5])


def test_masked_array():
    """Slice pushdown preserves masks."""
    arr = np.ma.array(np.arange(100).reshape(10, 10), mask=False)
    arr.mask[5, 5] = True
    x = da.from_array(arr, chunks=(3, 3))
    result = x[4:7, 4:7].compute()
    expected = arr[4:7, 4:7]
    assert_eq(result, expected)
    assert_eq(result.mask, expected.mask)


def test_deterministic_names():
    """Same slice -> same name, different slice -> different name."""
    arr = np.arange(100).reshape(10, 10)
    x1 = da.from_array(arr, chunks=(2, 2))
    x2 = da.from_array(arr, chunks=(2, 2))

    assert x1[0:2, 0:2].optimize().name == x2[0:2, 0:2].optimize().name
    assert x1[0:2, 0:2].optimize().name != x1[0:3, 0:3].optimize().name


def test_slice_then_reduction():
    """Slice followed by reduction."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(2, 2))
    assert_eq(x[0:4, 0:4].sum(), arr[0:4, 0:4].sum())

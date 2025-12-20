"""Tests for slice pushdown into IO expressions."""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array._array_expr._io import FromArray
from dask.array._array_expr._slicing import SliceSlicesIntegers
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)

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


def test_region_deferred_slice():
    """Slice pushdown uses _region for deferred slicing (not eager read)."""
    arr = np.arange(10000).reshape(100, 100)
    x = da.from_array(arr, chunks=(10, 10))
    # Use a slice that fits within a single chunk
    y = x[12:18, 34:39]

    opt = y.expr.optimize()

    # Should use _region parameter, not slice the source eagerly
    assert opt.operand("_region") == (slice(12, 18, None), slice(34, 39, None))
    # Source array should still be the full array
    assert opt.array.shape == (100, 100)
    # Chunks should be for the sliced region (6x5)
    assert opt.chunks == ((6,), (5,))

    # Verify correctness
    assert_eq(y, arr[12:18, 34:39])


def test_region_single_chunk():
    """Slice within a single chunk produces one task with direct slice."""
    arr = np.arange(10000 * 10000).reshape(10000, 10000)
    x = da.from_array(arr, chunks=(1000, 1000))
    # Small slice within a single chunk
    y = x[1500:1550, 2300:2350]

    opt = y.expr.optimize()
    graph = dict(opt.__dask_graph__())

    # Should be single task (slice fits within one chunk)
    task_keys = [k for k in graph if isinstance(k, tuple) and len(k) == 3]
    assert len(task_keys) == 1

    # The slice should be direct (1500:1550, 2300:2350), not via 1000x1000 chunk
    graph_str = str(graph)
    assert "1000" not in graph_str, "Should slice directly, not via full chunk"

    # Verify correctness
    assert_eq(y, arr[1500:1550, 2300:2350])


def test_region_multiple_chunks():
    """Slice spanning multiple chunks still produces multiple tasks."""
    arr = np.arange(10000).reshape(100, 100)
    x = da.from_array(arr, chunks=(10, 10))
    # Slice spanning 2x2 chunks: 15-25 spans chunks 1,2 in first dim
    # 35-45 spans chunks 3,4 in second dim
    y = x[15:25, 35:45]

    opt = y.expr.optimize()
    graph = dict(opt.__dask_graph__())

    # Should be 2x2=4 tasks (slice spans multiple chunks)
    task_keys = [k for k in graph if isinstance(k, tuple) and len(k) == 3]
    assert len(task_keys) == 4

    # Verify correctness
    assert_eq(y, arr[15:25, 35:45])


def test_region_zarr_deferred(tmp_path):
    """Zarr slicing is deferred - graph contains zarr array, not numpy data."""
    zarr = pytest.importorskip("zarr")
    # Create zarr array
    zarr_path = tmp_path / "test.zarr"
    z = zarr.open(
        str(zarr_path),
        mode="w",
        shape=(10000, 10000),
        dtype="float64",
        chunks=(1000, 1000),
    )
    z[1500:1550, 2300:2350] = np.arange(2500).reshape(50, 50)

    x = da.from_zarr(str(zarr_path))
    y = x[1500:1550, 2300:2350]

    opt = y.expr.optimize()
    graph = dict(opt.__dask_graph__())

    # Should have zarr array in graph, not numpy data
    zarr_arrays = [v for v in graph.values() if isinstance(v, zarr.Array)]
    numpy_arrays = [v for v in graph.values() if isinstance(v, np.ndarray)]

    assert len(zarr_arrays) == 1, "Graph should contain the zarr array"
    assert (
        len(numpy_arrays) == 0
    ), "Graph should not contain numpy arrays (data not loaded)"

    # The zarr array in graph should be the full array, not sliced
    assert zarr_arrays[0].shape == (10000, 10000)

    # Verify correctness
    assert_eq(y, z[1500:1550, 2300:2350])


def test_integer_indexing_pushdown():
    """Integer indexing uses region pushdown to minimize data loading."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    # Pure integer indexing - should be 2 tasks (FromArray + extract)
    y = x[3, 7]
    opt = y.optimize()
    assert len(opt.__dask_graph__()) == 2

    # The inner FromArray should have region centered on (3, 7)
    from_array_expr = opt.expr.array
    assert from_array_expr.operand("_region") == (slice(3, 4), slice(7, 8))
    assert from_array_expr.array.shape == (10, 10)  # Original array unchanged

    assert_eq(y, arr[3, 7])

    # Mixed slice + integer
    y = x[:3, 5]
    assert_eq(y, arr[:3, 5])

    y = x[5, 2:8]
    assert_eq(y, arr[5, 2:8])

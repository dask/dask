"""Tests for slice pushdown into IO expressions."""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array._array_expr.io import FromArray
from dask.array._array_expr.slicing import SliceSlicesIntegers
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


# ============================================================
# Slice through reduction tests
# ============================================================


def test_slice_through_reduction_optimization():
    """Verify slice pushdown through reduction produces equivalent result.

    x.sum(axis=0)[:5] should simplify to x[:, :5].sum(axis=0)
    """
    x = da.ones((100, 100), chunks=(10, 10))

    # The naive way: full sum then slice
    y = x.sum(axis=0)[:5]

    # The optimized way: slice first, then sum
    expected = x[:, :5].sum(axis=0)

    # After simplification, the names should be equivalent
    # (both sides need simplify since slices also simplify through ones)
    assert y.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_reduction_reduces_tasks():
    """Slice pushdown through reduction should reduce graph size.

    For a from_array with (10, 10) chunks, slicing after reduction
    should result in fewer tasks than computing the full reduction.
    """
    arr = np.arange(10000).reshape(100, 100)
    x = da.from_array(arr, chunks=(10, 10))

    # Full reduction has 10*10 input chunks
    full_sum = x.sum(axis=0)
    full_tasks = len(full_sum.optimize().__dask_graph__())

    # Sliced reduction should have fewer tasks
    sliced_sum = x.sum(axis=0)[:5]
    sliced_tasks = len(sliced_sum.optimize().__dask_graph__())

    # Slicing to first 5 elements (1 chunk column) should have ~10x fewer tasks
    assert sliced_tasks < full_tasks

    # Verify the reduction is correct
    assert_eq(sliced_sum, arr.sum(axis=0)[:5])


def test_slice_through_reduction_axis1():
    """Slice pushdown through sum(axis=1)."""
    x = da.ones((100, 100), chunks=(10, 10))

    # x.sum(axis=1)[:5] should simplify to x[:5, :].sum(axis=1)
    y = x.sum(axis=1)[:5]
    expected = x[:5, :].sum(axis=1)

    assert y.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_reduction_3d():
    """Slice pushdown through reduction on 3D array."""
    x = da.ones((20, 20, 20), chunks=(5, 5, 5))

    # Reduce axis 1, slice result
    # Output axes: [0, 2] become [0, 1] -> slice [:3, :4] maps to input [:3, :, :4]
    y = x.sum(axis=1)[:3, :4]
    expected = x[:3, :, :4].sum(axis=1)

    assert y.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_reduction_multiple_axes():
    """Slice pushdown through reduction on multiple axes."""
    x = da.ones((20, 20, 20), chunks=(5, 5, 5))

    # Reduce axes 0 and 2, only axis 1 remains
    # Output axis 0 -> input axis 1
    y = x.sum(axis=(0, 2))[:5]
    expected = x[:, :5, :].sum(axis=(0, 2))

    assert y.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_reduction_correctness():
    """Verify correctness of optimized slice-through-reduction."""
    arr = np.arange(10000).reshape(100, 100)
    x = da.from_array(arr, chunks=(10, 10))

    # Various cases
    assert_eq(x.sum(axis=0)[:5], arr.sum(axis=0)[:5])
    assert_eq(x.sum(axis=1)[:5], arr.sum(axis=1)[:5])
    assert_eq(x.sum(axis=0)[10:20], arr.sum(axis=0)[10:20])


def test_slice_through_reduction_integer_index():
    """Integer indexing through reduction reduces tasks.

    Integer indices are converted to size-1 slices, pushed through,
    then extracted with [0] at the end.
    """
    arr = np.arange(10000).reshape(100, 100)
    x = da.from_array(arr, chunks=(10, 10))

    # Full reduction
    full_tasks = len(x.sum(axis=0).optimize().__dask_graph__())

    # Integer index should have fewer tasks
    result = x.sum(axis=0)[5]
    indexed_tasks = len(result.optimize().__dask_graph__())

    assert indexed_tasks < full_tasks
    assert_eq(result, arr.sum(axis=0)[5])


# =============================================================================
# Slice through creation expressions (ones, zeros, full, empty)
# =============================================================================


def test_slice_ones_returns_smaller_ones():
    """Slicing ones() returns a new ones() with the sliced shape."""
    from dask.array._array_expr.creation import Ones

    x = da.ones((100, 100), chunks=(10, 10))
    y = x[:15, :25]

    # After simplification, should be Ones with new shape, not Slice(Ones)
    simplified = y.expr.simplify()
    assert isinstance(simplified, Ones)
    assert simplified.shape == (15, 25)


def test_slice_zeros_returns_smaller_zeros():
    """Slicing zeros() returns a new zeros() with the sliced shape."""
    from dask.array._array_expr.creation import Zeros

    x = da.zeros((100, 100), chunks=(10, 10))
    y = x[:15, :25]

    simplified = y.expr.simplify()
    assert isinstance(simplified, Zeros)
    assert simplified.shape == (15, 25)


def test_slice_full_returns_smaller_full():
    """Slicing full() returns a new full() with the sliced shape."""
    from dask.array._array_expr.creation import Full

    x = da.full((100, 100), 42, chunks=(10, 10))
    y = x[:15, :25]

    simplified = y.expr.simplify()
    assert isinstance(simplified, Full)
    assert simplified.shape == (15, 25)
    # Verify fill_value is preserved
    assert_eq(y, np.full((15, 25), 42))


def test_slice_creation_correctness():
    """Verify sliced creation expressions produce correct values."""
    assert_eq(da.ones((100, 100), chunks=10)[:15, :25], np.ones((15, 25)))
    assert_eq(da.zeros((100, 100), chunks=10)[:15, :25], np.zeros((15, 25)))
    assert_eq(da.full((100, 100), 7.5, chunks=10)[:15, :25], np.full((15, 25), 7.5))


def test_slice_creation_preserves_dtype():
    """Verify sliced creation preserves dtype."""
    x = da.ones((100, 100), chunks=10, dtype="int32")[:15, :25]
    assert x.dtype == np.dtype("int32")
    assert_eq(x, np.ones((15, 25), dtype="int32"))


# =============================================================================
# Slice through Concatenate
# =============================================================================


def test_slice_through_concat_same_axis_first_array():
    """Slice entirely within first array of concat -> just first array sliced."""
    a = da.ones((10, 5), chunks=5)
    b = da.ones((10, 5), chunks=5)
    result = da.concatenate([a, b], axis=0)[:5]  # Only needs 'a'
    expected = a[:5]

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_concat_same_axis_spans_arrays():
    """Slice spans multiple arrays in concat."""
    a = da.ones((10, 5), chunks=5)
    b = da.ones((10, 5), chunks=5)
    c = da.ones((10, 5), chunks=5)
    # slice 5:15 spans a[5:10] and b[0:5]
    result = da.concatenate([a, b, c], axis=0)[5:15]
    expected = da.concatenate([a[5:], b[:5]], axis=0)

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_concat_different_axis():
    """Slice on different axis than concat -> push to all inputs."""
    a = da.ones((10, 20), chunks=5)
    b = da.ones((10, 20), chunks=5)
    result = da.concatenate([a, b], axis=0)[:, :5]  # Slice axis 1
    expected = da.concatenate([a[:, :5], b[:, :5]], axis=0)

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_concat_correctness():
    """Verify slice through concat produces correct values."""
    a = np.arange(20).reshape(4, 5)
    b = np.arange(20, 40).reshape(4, 5)
    da_a = da.from_array(a, chunks=2)
    da_b = da.from_array(b, chunks=2)

    # Same axis slice
    result = da.concatenate([da_a, da_b], axis=0)[:3]
    assert_eq(result, np.concatenate([a, b], axis=0)[:3])

    # Different axis slice
    result = da.concatenate([da_a, da_b], axis=0)[:, :3]
    assert_eq(result, np.concatenate([a, b], axis=0)[:, :3])

    # Slice spanning both arrays
    result = da.concatenate([da_a, da_b], axis=0)[2:6]
    assert_eq(result, np.concatenate([a, b], axis=0)[2:6])


def test_slice_through_concat_reduces_tasks():
    """Verify slice through concat reduces task count."""
    a = da.ones((100, 100), chunks=10)
    b = da.ones((100, 100), chunks=10)
    concat = da.concatenate([a, b], axis=0)

    full_tasks = len(concat.optimize().__dask_graph__())
    # Slice only first 5 rows - should only need first array
    sliced_tasks = len(concat[:5].optimize().__dask_graph__())

    assert sliced_tasks < full_tasks


# =============================================================================
# Slice through Stack
# =============================================================================


def test_slice_through_stack_selects_subset():
    """Slice on stacked axis selects subset of inputs."""
    a = da.ones((10, 5), chunks=5)
    b = da.ones((10, 5), chunks=5)
    c = da.ones((10, 5), chunks=5)
    # stack gives shape (3, 10, 5), slice [:1] should be stack([a])
    result = da.stack([a, b, c], axis=0)[:1]
    expected = da.stack([a], axis=0)

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_stack_other_axis():
    """Slice on non-stacked axis pushes to all inputs."""
    a = da.ones((10, 20), chunks=5)
    b = da.ones((10, 20), chunks=5)
    # stack gives shape (2, 10, 20), slice [:, :5, :10] pushes to each array
    result = da.stack([a, b], axis=0)[:, :5, :10]
    expected = da.stack([a[:5, :10], b[:5, :10]], axis=0)

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_stack_mixed():
    """Slice on both stacked and other axes."""
    a = da.ones((10, 20), chunks=5)
    b = da.ones((10, 20), chunks=5)
    c = da.ones((10, 20), chunks=5)
    # stack gives shape (3, 10, 20), slice [:2, :5] keeps a and b, sliced
    result = da.stack([a, b, c], axis=0)[:2, :5]
    expected = da.stack([a[:5], b[:5]], axis=0)

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_stack_correctness():
    """Verify slice through stack produces correct values."""
    a = np.arange(20).reshape(4, 5)
    b = np.arange(20, 40).reshape(4, 5)
    c = np.arange(40, 60).reshape(4, 5)
    da_a = da.from_array(a, chunks=2)
    da_b = da.from_array(b, chunks=2)
    da_c = da.from_array(c, chunks=2)

    # Slice on stacked axis
    result = da.stack([da_a, da_b, da_c], axis=0)[:2]
    assert_eq(result, np.stack([a, b, c], axis=0)[:2])

    # Slice on other axis
    result = da.stack([da_a, da_b, da_c], axis=0)[:, :2, :3]
    assert_eq(result, np.stack([a, b, c], axis=0)[:, :2, :3])


def test_slice_through_stack_reduces_tasks():
    """Verify slice through stack reduces task count."""
    a = da.ones((100, 100), chunks=10)
    b = da.ones((100, 100), chunks=10)
    c = da.ones((100, 100), chunks=10)
    stacked = da.stack([a, b, c], axis=0)

    full_tasks = len(stacked.optimize().__dask_graph__())
    # Slice only first array
    sliced_tasks = len(stacked[:1].optimize().__dask_graph__())

    assert sliced_tasks < full_tasks


# =============================================================================
# Slice through BroadcastTo
# =============================================================================


def test_slice_through_broadcast_to_new_dim():
    """Slice on dimension added by broadcast."""
    x = da.ones((10,), chunks=5)
    # broadcast_to adds a new dimension at front: (10,) -> (20, 10)
    result = da.broadcast_to(x, (20, 10))[:5, :]
    expected = da.broadcast_to(x, (5, 10))

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_broadcast_to_existing_dim():
    """Slice on dimension that exists in input."""
    x = da.ones((10,), chunks=5)
    # broadcast_to adds new dim: (10,) -> (20, 10)
    result = da.broadcast_to(x, (20, 10))[:, :5]
    expected = da.broadcast_to(x[:5], (20, 5))

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_broadcast_to_both_dims():
    """Slice on both new and existing dimensions."""
    x = da.ones((10,), chunks=5)
    result = da.broadcast_to(x, (20, 10))[:5, :3]
    expected = da.broadcast_to(x[:3], (5, 3))

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_broadcast_to_broadcasted_dim():
    """Slice on dimension that was size-1 in input."""
    x = da.ones((1, 10), chunks=(1, 5))
    # broadcast_to expands first dim: (1, 10) -> (20, 10)
    result = da.broadcast_to(x, (20, 10))[:5, :3]
    # First dim can't push (was 1), second dim pushes
    expected = da.broadcast_to(x[:, :3], (5, 3))

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_broadcast_to_correctness():
    """Verify slice through broadcast_to produces correct values."""
    x = np.arange(10)
    da_x = da.from_array(x, chunks=5)

    # Broadcast to 2D then slice
    result = da.broadcast_to(da_x, (20, 10))[:5, :3]
    expected = np.broadcast_to(x, (20, 10))[:5, :3]
    assert_eq(result, expected)


def test_slice_through_broadcast_to_reduces_tasks():
    """Verify slice through broadcast_to reduces task count."""
    x = da.ones((100,), chunks=10)
    broadcasted = da.broadcast_to(x, (100, 100))

    full_tasks = len(broadcasted.optimize().__dask_graph__())
    # Slice to smaller output
    sliced_tasks = len(broadcasted[:5, :5].optimize().__dask_graph__())

    assert sliced_tasks < full_tasks


# --- Shuffle (take) through Elemwise Tests ---


def test_shuffle_pushes_through_elemwise_add():
    """(x + y)[[1,3,5]] should optimize to x[[1,3,5]] + y[[1,3,5]]."""
    x = da.arange(20, chunks=5)
    y = da.arange(20, chunks=5)

    indices = [1, 3, 5, 7, 9]
    result = (x + y)[indices]
    expected = x[indices] + y[indices]

    # Structure should match
    assert result.expr.simplify()._name == expected.expr.simplify()._name

    # Verify correctness
    x_np = np.arange(20)
    y_np = np.arange(20)
    assert_eq(result, (x_np + y_np)[indices])


def test_shuffle_pushes_through_elemwise_mul():
    """(x * y)[[2,4,6]] should optimize to x[[2,4,6]] * y[[2,4,6]]."""
    x = da.arange(30, chunks=10)
    y = da.arange(30, chunks=10)

    indices = [2, 4, 6, 8]
    result = (x * y)[indices]
    expected = x[indices] * y[indices]

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_shuffle_pushes_through_elemwise_2d():
    """Shuffle on 2D array along axis 0."""
    x = da.ones((10, 8), chunks=(5, 4))
    y = da.ones((10, 8), chunks=(5, 4))

    indices = [0, 2, 4, 6]
    result = (x + y)[indices, :]
    expected = x[indices, :] + y[indices, :]

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_shuffle_pushes_through_elemwise_scalar():
    """Shuffle through elemwise with scalar."""
    x = da.arange(20, chunks=5)

    indices = [1, 5, 9, 13]
    result = (x + 1)[indices]
    expected = x[indices] + 1

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_shuffle_pushes_through_unary_elemwise():
    """Shuffle through unary elemwise (e.g. negative)."""
    x = da.arange(20, chunks=5)

    indices = [2, 4, 6, 8]
    result = (-x)[indices]
    expected = -(x[indices])

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_shuffle_through_elemwise_reduces_work():
    """Taking a subset should reduce computation by only computing needed elements."""
    x = da.ones((100,), chunks=10)
    y = da.ones((100,), chunks=10)

    # Take only 10 of 100 elements
    indices = list(range(0, 100, 10))  # [0, 10, 20, ..., 90]
    result = (x + y)[indices]

    # Optimized should have fewer tasks since we only compute what we need
    unopt_tasks = len(result.__dask_graph__())
    opt_tasks = len(result.optimize().__dask_graph__())

    # Optimization should reduce task count
    assert opt_tasks <= unopt_tasks


# --- Shuffle through Transpose Tests ---


def test_shuffle_pushes_through_transpose():
    """x.T[[1,3,5]] should optimize to x[:, [1,3,5]].T."""
    x = da.arange(20, chunks=5).reshape((4, 5))

    indices = [1, 3]
    result = x.T[indices, :]  # Take rows 1, 3 from transposed (5, 4)
    expected = x[:, indices].T  # Take cols 1, 3 from (4, 5), then transpose

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_shuffle_pushes_through_transpose_axis1():
    """x.T[:, [0,2]] should optimize to x[[0,2], :].T."""
    x = da.arange(20, chunks=5).reshape((4, 5))

    indices = [0, 2]
    result = x.T[:, indices]  # Take cols 0, 2 from transposed (5, 4)
    expected = x[indices, :].T  # Take rows 0, 2 from (4, 5), then transpose

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_shuffle_pushes_through_transpose_3d():
    """Shuffle through 3D transpose."""
    x = da.ones((2, 3, 4), chunks=2)

    indices = [0, 2]
    # Transpose (2,3,4) -> (4,3,2), then take along axis 0
    result = x.transpose((2, 1, 0))[indices, :, :]
    # Equivalent: take along axis 2 of original, then transpose
    expected = x[:, :, indices].transpose((2, 1, 0))

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


# --- Shuffle through Concatenate/Stack Tests ---


def test_shuffle_pushes_through_concatenate():
    """Shuffle on non-concat axis pushes to all inputs."""
    a = da.arange(20, chunks=5).reshape((4, 5))
    b = da.arange(20, 40, chunks=5).reshape((4, 5))

    concat = da.concatenate([a, b], axis=1)  # (4, 10)
    indices = [0, 2]
    result = concat[indices, :]  # Take rows 0, 2

    expected = da.concatenate([a[indices, :], b[indices, :]], axis=1)

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_shuffle_pushes_through_stack():
    """Shuffle on non-stack axis pushes to all inputs."""
    a = da.arange(12, chunks=4).reshape((3, 4))
    b = da.arange(12, 24, chunks=4).reshape((3, 4))

    stacked = da.stack([a, b], axis=0)  # (2, 3, 4)
    indices = [0, 2]
    result = stacked[:, indices, :]  # Take along axis 1

    expected = da.stack([a[indices, :], b[indices, :]], axis=0)

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


# --- Shuffle through Blockwise Tests ---


def test_shuffle_pushes_through_blockwise():
    """Shuffle through blockwise when adjust_chunks doesn't affect shuffle axis."""
    from dask.array._array_expr._blockwise import Blockwise

    # map_blocks creates a generic Blockwise with no adjust_chunks
    x = da.ones((4, 6), chunks=(2, 3))
    mapped = x.map_blocks(lambda b: b * 2)

    indices = [0, 2]
    result = mapped[indices, :]

    # Expected: shuffle first, then map_blocks
    expected = x[indices, :].map_blocks(lambda b: b * 2)

    # Verify the optimization happened - Blockwise should be at top
    opt = result.expr.simplify()
    assert isinstance(opt, Blockwise)

    # Verify correctness
    assert_eq(result, expected)


def test_shuffle_does_not_push_through_blockwise_adjust_chunks():
    """Shuffle does NOT push through blockwise when adjust_chunks affects shuffle axis."""
    from dask.array._array_expr._shuffle import Shuffle

    # map_blocks with explicit chunks sets adjust_chunks
    x = da.ones((8, 6), chunks=(2, 3))
    # Providing chunks means each output block has these chunk sizes (adjust_chunks)
    # This creates output with shape (4, 6) chunks (1, 3)
    mapped = x.map_blocks(lambda b: b * 2, chunks=(1, 3))

    indices = [0, 2]  # Taking along axis 0 - NOT all indices
    result = mapped[indices, :]

    # Shuffle should stay at top (not push through) because axis 0 has adjust_chunks
    opt = result.expr.simplify()
    assert isinstance(opt, Shuffle)

    # Still correct
    assert_eq(result, mapped.compute()[indices, :])

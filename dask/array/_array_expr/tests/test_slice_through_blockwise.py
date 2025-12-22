"""Tests for slice pushdown through Blockwise expressions.

These tests explore when slice pushdown is safe and correct for different
Blockwise configurations.
"""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


# =============================================================================
# Case 1: Standard Blockwise (reduction chunk step)
# - out_ind matches input indices
# - No new_axes, no adjust_chunks
# - Slice should push through directly
# =============================================================================


def test_slice_through_reduction_blockwise():
    """Slice pushes through the Blockwise chunk step of a reduction."""
    x = da.ones((100, 100), chunks=(10, 10))

    # x.sum(axis=0)[:5] should simplify to x[:, :5].sum(axis=0)
    result = x.sum(axis=0)[:5]
    expected = x[:, :5].sum(axis=0)

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_reduction_blockwise_axis1():
    """Slice through reduction on axis 1."""
    x = da.ones((100, 100), chunks=(10, 10))

    result = x.sum(axis=1)[:5]
    expected = x[:5, :].sum(axis=1)

    assert result.expr.simplify()._name == expected.expr.simplify()._name


# =============================================================================
# Case 2: Elemwise operations
# - Already handled by _pushdown_through_elemwise
# - Included here for completeness
# =============================================================================


def test_slice_through_elemwise_add():
    """Slice through addition."""
    x = da.ones((100, 100), chunks=(10, 10))
    y = da.ones((100, 100), chunks=(10, 10))

    result = (x + y)[:5, :10]
    expected = x[:5, :10] + y[:5, :10]

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_elemwise_unary():
    """Slice through unary function."""
    x = da.ones((100, 100), chunks=(10, 10))

    result = da.sin(x)[:5, :10]
    expected = da.sin(x[:5, :10])

    assert result.expr.simplify()._name == expected.expr.simplify()._name


# =============================================================================
# Case 3: Broadcasting
# - Smaller input has fewer indices
# - Need to only slice dimensions that exist in the smaller input
# =============================================================================


def test_slice_through_broadcast_row():
    """Slice through broadcasting with a row vector."""
    arr = np.arange(100).reshape(10, 10)
    row = np.arange(10)

    x = da.from_array(arr, chunks=(5, 5))
    r = da.from_array(row, chunks=5)

    # (x + r)[:3, :4] should simplify to x[:3, :4] + r[:4]
    # Note: expected also needs simplify because slices push into from_array regions
    result = (x + r)[:3, :4]
    expected = x[:3, :4] + r[:4]

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr[:3, :4] + row[:4])


def test_slice_through_broadcast_column():
    """Slice through broadcasting with a column vector."""
    arr = np.arange(100).reshape(10, 10)
    col = np.arange(10).reshape(10, 1)

    x = da.from_array(arr, chunks=(5, 5))
    c = da.from_array(col, chunks=(5, 1))

    # (x + c)[:3, :4] should simplify to x[:3, :4] + c[:3, :]
    result = (x + c)[:3, :4]
    expected = x[:3, :4] + c[:3, :]

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr[:3, :4] + col[:3, :])


def test_slice_through_broadcast_scalar():
    """Slice through broadcasting with a scalar."""
    arr = np.arange(100).reshape(10, 10)

    x = da.from_array(arr, chunks=(5, 5))

    # (x + 5)[:3, :4] should simplify to x[:3, :4] + 5
    result = (x + 5)[:3, :4]
    expected = x[:3, :4] + 5

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr[:3, :4] + 5)


# =============================================================================
# Case 4: new_axes - Blockwise adds dimensions
# - Slice on a new axis doesn't correspond to input
# - Should NOT push through (or handle specially)
# =============================================================================


def test_slice_new_axis_not_pushed():
    """Slicing on a new_axis dimension should not push through naively."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    # map_blocks that adds a new axis
    y = da.map_blocks(lambda b: b[..., np.newaxis], x, new_axis=2, dtype=arr.dtype)

    # Slice on the new axis - this shouldn't cause issues
    result = y[:3, :4, :]
    expected = arr[:3, :4, np.newaxis]

    assert_eq(result, expected)


def test_slice_only_new_axis():
    """Slicing only the new axis dimension."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    # Add new axis of size > 1
    y = da.map_blocks(
        lambda b: np.repeat(b[..., np.newaxis], 3, axis=2),
        x,
        new_axis=2,
        chunks=(5, 5, 3),
        dtype=arr.dtype,
    )

    # Slice on the new axis
    result = y[:, :, :2]
    # This is complex - the slice on axis 2 can't push to input

    assert_eq(result, np.repeat(arr[..., np.newaxis], 3, axis=2)[:, :, :2])


# =============================================================================
# Case 5: drop_axis / contraction
# - Input has more dimensions than output
# - Some input indices don't appear in output
# =============================================================================


def test_slice_through_drop_axis():
    """Slice through a drop_axis operation."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    # map_blocks that drops axis 0
    y = da.map_blocks(lambda b: b.sum(axis=0), x, drop_axis=0, dtype=arr.dtype)

    # y has shape (10,), slicing [:5] should map to x[:, :5]
    result = y[:5]
    expected = arr.sum(axis=0)[:5]

    assert_eq(result, expected)


def test_slice_through_drop_axis_1():
    """Slice through dropping axis 1."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    # map_blocks that drops axis 1
    y = da.map_blocks(lambda b: b.sum(axis=1), x, drop_axis=1, dtype=arr.dtype)

    # y has shape (10,), slicing [:5] should map to x[:5, :]
    result = y[:5]
    expected = arr.sum(axis=1)[:5]

    assert_eq(result, expected)


# =============================================================================
# Case 6: adjust_chunks
# - Chunk sizes change in the output
# - Slice indices may not map correctly
# =============================================================================


def test_slice_adjust_chunks():
    """Slice through an operation that adjusts chunks."""
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    # Double each chunk along axis 0
    def double_rows(block):
        return np.repeat(block, 2, axis=0)

    y = da.map_blocks(
        double_rows,
        x,
        chunks=(10, 5),  # chunks double in size
        dtype=arr.dtype,
    )

    # y has shape (20, 10)
    result = y[:5, :5]
    expected = np.repeat(arr, 2, axis=0)[:5, :5]

    assert_eq(result, expected)


# =============================================================================
# Case 7: Multiple inputs with different shapes
# - Inputs align via broadcasting
# - Need to map slice to each input appropriately
# =============================================================================


def test_slice_multiple_inputs_same_shape():
    """Slice through blockwise with multiple same-shaped inputs."""
    arr1 = np.arange(100).reshape(10, 10)
    arr2 = np.arange(100, 200).reshape(10, 10)

    x = da.from_array(arr1, chunks=(5, 5))
    y = da.from_array(arr2, chunks=(5, 5))

    # (x + y)[:3, :4] should simplify to x[:3, :4] + y[:3, :4]
    result = (x + y)[:3, :4]
    expected = x[:3, :4] + y[:3, :4]

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr1[:3, :4] + arr2[:3, :4])


def test_slice_multiple_inputs_broadcast():
    """Slice through blockwise with broadcasting inputs."""
    arr = np.arange(100).reshape(10, 10)
    vec = np.arange(10)

    x = da.from_array(arr, chunks=(5, 5))
    v = da.from_array(vec, chunks=5)

    # (x * v)[:3, :4] should simplify to x[:3, :4] * v[:4]
    result = (x * v)[:3, :4]
    expected = x[:3, :4] * v[:4]

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr[:3, :4] * vec[:4])


# =============================================================================
# Correctness tests - verify computed values
# =============================================================================


@pytest.mark.parametrize(
    "shape,chunks,axis,slice_",
    [
        ((100, 100), (10, 10), 0, slice(5)),
        ((100, 100), (10, 10), 1, slice(5)),
        ((100, 100), (10, 10), 0, slice(10, 20)),
        ((100, 100), (10, 10), 1, slice(10, 20)),
        ((50, 50, 50), (10, 10, 10), 0, slice(5)),
        ((50, 50, 50), (10, 10, 10), 1, slice(5)),
        ((50, 50, 50), (10, 10, 10), 2, slice(5)),
    ],
)
def test_slice_through_reduction_correctness(shape, chunks, axis, slice_):
    """Verify slice-through-reduction produces correct values."""
    arr = np.random.random(shape)
    x = da.from_array(arr, chunks=chunks)

    # Build the slice tuple for the output
    out_ndim = len(shape) - 1  # reduction removes one axis
    slices = [slice(None)] * out_ndim
    slices[0] = slice_

    result = x.sum(axis=axis)[tuple(slices)]
    expected = arr.sum(axis=axis)[tuple(slices)]

    assert_eq(result, expected)


# =============================================================================
# Verify optimization is/isn't applied
# =============================================================================


def test_optimization_applied_to_reduction():
    """Verify optimization IS applied: slice pushed through reduction."""
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    x = da.ones((100, 100), chunks=(10, 10))
    y = x.sum(axis=0)[:5]

    # Before simplification: Slice(PartialReduce(...))
    assert isinstance(y.expr, SliceSlicesIntegers)

    # After simplification: PartialReduce(Blockwise(Slice(...)))
    simplified = y.expr.simplify()
    assert not isinstance(simplified, SliceSlicesIntegers)
    assert "sum-aggregate" in simplified._name


def test_optimization_pushes_through_new_axes_when_safe():
    """Verify slice pushes through new_axes when not slicing the new axis."""
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    x = da.ones((20, 20), chunks=(5, 5))
    y = da.map_blocks(lambda b: b[..., np.newaxis], x, new_axis=2, dtype=float)
    z = y[:5, :5, :]  # Not slicing the new axis (axis 2)

    # The slice CAN push through because we're not slicing axis 2
    simplified = z.expr.simplify()
    assert not isinstance(simplified, SliceSlicesIntegers)
    assert_eq(z, np.ones((20, 20))[:5, :5, np.newaxis])


def test_optimization_not_applied_slicing_new_axes():
    """Verify optimization is NOT applied when slicing new_axes dimension."""
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    x = da.ones((20, 20), chunks=(5, 5))
    # Add new axis of size 3
    y = da.map_blocks(
        lambda b: np.repeat(b[..., np.newaxis], 3, axis=2),
        x,
        new_axis=2,
        chunks=(5, 5, 3),
        dtype=float,
    )
    z = y[:5, :5, :2]  # Slicing the new axis (axis 2)

    # The slice should NOT push through because we're slicing axis 2
    simplified = z.expr.simplify()
    assert isinstance(simplified, SliceSlicesIntegers)


def test_optimization_reduces_tasks():
    """Verify optimization reduces task count for from_array."""
    arr = np.ones((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    full = x.sum(axis=0)
    sliced = x.sum(axis=0)[:5]

    full_tasks = len(full.optimize().__dask_graph__())
    sliced_tasks = len(sliced.optimize().__dask_graph__())

    # Sliced should have fewer tasks (only processes 1 column of chunks)
    assert sliced_tasks < full_tasks


# =============================================================================
# Case 8: Tensordot / Matmul
# - adjust_chunks only affects contracted dimension
# - Slices on non-contracted dimensions can push through
# =============================================================================


@pytest.mark.filterwarnings("ignore::dask.array.core.PerformanceWarning")
def test_slice_through_tensordot_correctness():
    """Verify slice through tensordot produces correct values."""
    arr = np.random.random((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    result = x.dot(x.T)[:5, :5]
    expected = arr.dot(arr.T)[:5, :5]

    assert_eq(result, expected)


@pytest.mark.filterwarnings("ignore::dask.array.core.PerformanceWarning")
def test_slice_through_matmul_correctness():
    """Verify slice through matmul produces correct values."""
    arr1 = np.random.random((100, 50))
    arr2 = np.random.random((50, 100))
    x = da.from_array(arr1, chunks=(10, 10))
    y = da.from_array(arr2, chunks=(10, 10))

    result = (x @ y)[:5, :5]
    expected = (arr1 @ arr2)[:5, :5]

    assert_eq(result, expected)


@pytest.mark.filterwarnings("ignore::dask.array.core.PerformanceWarning")
def test_slice_through_matmul_expression_structure():
    """Verify x.dot(y)[a:b, c:d] simplifies to x[a:b, :].dot(y[:, c:d])."""
    x = da.ones((100, 50), chunks=(10, 10))
    y = da.ones((50, 100), chunks=(10, 10))

    # Use different slices to verify correct operand mapping
    result = (x @ y)[:15, :25]
    expected = x[:15, :] @ y[:, :25]

    # Both should simplify to equivalent expressions
    assert result.expr.simplify()._name == expected.expr.simplify()._name


@pytest.mark.filterwarnings("ignore::dask.array.core.PerformanceWarning")
def test_slice_through_tensordot_reduces_tasks():
    """Verify slice through tensordot reduces task count.

    x.dot(x.T)[0:5, 0:5] should optimize to compute only the
    submatrix, not the full matrix then slice.
    """
    x = da.ones((100, 100), chunks=(10, 10))

    full = x.dot(x.T)
    sliced = x.dot(x.T)[:5, :5]

    full_tasks = len(full.optimize().__dask_graph__())
    sliced_tasks = len(sliced.optimize().__dask_graph__())

    # Sliced should have significantly fewer tasks
    # Full: 10x10 output chunks = 100 output chunks
    # Sliced: 1x1 output chunks = 1 output chunk
    # Task reduction should be ~10x or more
    assert sliced_tasks < full_tasks / 5

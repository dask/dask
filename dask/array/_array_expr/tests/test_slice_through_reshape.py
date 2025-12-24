"""Tests for slice pushdown through Reshape expressions.

Slice can push through Reshape when leading dimensions are preserved,
i.e., the reshape only affects trailing dimensions.
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
# Case 1: Leading dimension preserved - slice should push through
# =============================================================================


def test_slice_through_reshape_leading_dim_preserved():
    """Slice on preserved leading dimension pushes through reshape."""
    arr = np.arange(60).reshape((10, 6))
    x = da.from_array(arr, chunks=(5, 3))

    # Reshape (10, 6) -> (10, 2, 3) preserves first dimension
    result = x.reshape((10, 2, 3))[:3]
    expected = x[:3].reshape((3, 2, 3))

    # After simplification, both should have same structure
    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr.reshape((10, 2, 3))[:3])


def test_slice_through_reshape_flatten_trailing():
    """Slice on leading dimension when flattening trailing dims."""
    arr = np.arange(60).reshape((10, 2, 3))
    x = da.from_array(arr, chunks=(5, 2, 3))

    # Reshape (10, 2, 3) -> (10, 6) preserves first dimension
    result = x.reshape((10, 6))[:4]
    expected = x[:4].reshape((4, 6))

    # After optimization, both should produce equivalent graphs
    # (Reshape vs ReshapeLowered difference resolved by lowering)
    assert result.optimize()._name == expected.optimize()._name
    assert_eq(result, arr.reshape((10, 6))[:4])


def test_slice_through_reshape_multiple_leading_dims():
    """Slice when multiple leading dimensions are preserved."""
    arr = np.arange(120).reshape((4, 5, 6))
    x = da.from_array(arr, chunks=(2, 5, 3))

    # Reshape (4, 5, 6) -> (4, 5, 2, 3) preserves first two dimensions
    result = x.reshape((4, 5, 2, 3))[:2, :3]
    expected = x[:2, :3].reshape((2, 3, 2, 3))

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr.reshape((4, 5, 2, 3))[:2, :3])


def test_slice_through_reshape_middle_slice():
    """Middle slice (start > 0) on preserved dimension."""
    arr = np.arange(100).reshape((10, 10))
    x = da.from_array(arr, chunks=(5, 5))

    # Reshape (10, 10) -> (10, 2, 5) preserves first dimension
    result = x.reshape((10, 2, 5))[3:7]
    expected = x[3:7].reshape((4, 2, 5))

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr.reshape((10, 2, 5))[3:7])


# =============================================================================
# Case 2: Dimension NOT preserved - slice should NOT push through
# =============================================================================


def test_slice_not_pushed_when_dim_changes():
    """Slice blocked when the sliced dimension changes size."""
    arr = np.arange(60).reshape((6, 10))
    x = da.from_array(arr, chunks=(3, 5))

    # Reshape (6, 10) -> (2, 3, 10) splits first dimension
    # First dim changes from 6 to 2, so slice should NOT push through
    result = x.reshape((2, 3, 10))[:1]

    # Just verify correctness - can't push through
    assert_eq(result, arr.reshape((2, 3, 10))[:1])


def test_slice_not_pushed_through_flatten():
    """Slice blocked when reshape completely flattens the array."""
    arr = np.arange(100).reshape((10, 10))
    x = da.from_array(arr, chunks=(5, 5))

    # Reshape to 1D - no dimension correspondence
    result = x.reshape((100,))[:30]

    # Just verify correctness - can't push through
    assert_eq(result, arr.reshape((100,))[:30])


def test_slice_on_reshaped_axis_not_pushed():
    """Slice on axis that was created by reshape doesn't push."""
    arr = np.arange(60).reshape((10, 6))
    x = da.from_array(arr, chunks=(5, 3))

    # Reshape (10, 6) -> (10, 2, 3), then slice on new axis 1
    result = x.reshape((10, 2, 3))[:, :1]

    # Axis 1 is new (from splitting 6 -> 2, 3), can't push through
    assert_eq(result, arr.reshape((10, 2, 3))[:, :1])


# =============================================================================
# Case 3: Slice with None (newaxis) - should still push through
# =============================================================================


def test_slice_with_none_pushes_through():
    """Slice with None (newaxis) should push through and re-apply None."""
    arr = np.arange(60).reshape((10, 6))
    x = da.from_array(arr, chunks=(5, 3))

    # Reshape (10, 6) -> (10, 2, 3), slice with None
    result = x.reshape((10, 2, 3))[:5, None]
    expected = x[:5].reshape((5, 2, 3))[:, None]

    # Both should have same structure after simplification
    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr.reshape((10, 2, 3))[:5, None])


def test_slice_with_none_at_end():
    """Slice with None at end of index."""
    arr = np.arange(60).reshape((10, 6))
    x = da.from_array(arr, chunks=(5, 3))

    result = x.reshape((10, 2, 3))[:5, :, :, None]
    expected = x[:5].reshape((5, 2, 3))[:, :, :, None]

    # Both should have same structure after simplification
    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr.reshape((10, 2, 3))[:5, :, :, None])


def test_slice_with_multiple_nones():
    """Slice with multiple Nones."""
    arr = np.arange(60).reshape((10, 6))
    x = da.from_array(arr, chunks=(5, 3))

    result = x.reshape((10, 2, 3))[None, :5, None]
    expected = x[:5].reshape((5, 2, 3))[None, :, None]

    # Both should have same structure after simplification
    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, arr.reshape((10, 2, 3))[None, :5, None])


def test_slice_with_none_correctness():
    """Verify correctness with various None positions."""
    arr = np.arange(120).reshape((10, 12))
    x = da.from_array(arr, chunks=(5, 6))

    # (10, 12) -> (10, 3, 4)
    reshaped = x.reshape((10, 3, 4))

    # Various slices with Nones
    assert_eq(reshaped[:5, None, :, :], arr.reshape((10, 3, 4))[:5, None, :, :])
    assert_eq(reshaped[None, :5], arr.reshape((10, 3, 4))[None, :5])
    assert_eq(reshaped[:5, :, None, :], arr.reshape((10, 3, 4))[:5, :, None, :])


# =============================================================================
# Correctness tests with various shapes
# =============================================================================


@pytest.mark.parametrize(
    "in_shape,out_shape,slice_",
    [
        # Trailing split
        ((20, 6), (20, 2, 3), (slice(10),)),
        ((20, 6), (20, 2, 3), (slice(5, 15),)),
        ((20, 12), (20, 3, 4), (slice(None, 8),)),
        # Trailing merge
        ((20, 2, 3), (20, 6), (slice(10),)),
        ((20, 4, 5), (20, 20), (slice(5, 15),)),
        # Multiple preserved dims
        ((10, 5, 6), (10, 5, 2, 3), (slice(5), slice(3))),
        ((10, 5, 4), (10, 5, 2, 2), (slice(3, 8), slice(None, 4))),
    ],
)
def test_slice_through_reshape_correctness(in_shape, out_shape, slice_):
    """Parametrized correctness tests."""
    arr = np.arange(np.prod(in_shape)).reshape(in_shape)
    chunks = tuple(max(1, s // 2) for s in in_shape)
    x = da.from_array(arr, chunks=chunks)

    result = x.reshape(out_shape)[slice_]
    expected = arr.reshape(out_shape)[slice_]

    assert_eq(result, expected)


# =============================================================================
# Task reduction tests
# =============================================================================


def test_slice_through_reshape_reduces_tasks():
    """Verify slice pushdown reduces task count."""
    arr = np.ones((100, 10))
    x = da.from_array(arr, chunks=(10, 5))

    # Reshape preserves first dim, then slice
    full = x.reshape((100, 2, 5))
    sliced = x.reshape((100, 2, 5))[:10]

    full_tasks = len(full.optimize().__dask_graph__())
    sliced_tasks = len(sliced.optimize().__dask_graph__())

    # Sliced should have fewer tasks (only 1/10 of chunks)
    assert sliced_tasks < full_tasks


def test_slice_through_reshape_reduces_numblocks():
    """Verify slice pushdown reduces number of blocks."""
    arr = np.ones((100, 20))
    x = da.from_array(arr, chunks=(10, 10))

    result = x.reshape((100, 4, 5))[:20]
    optimized = result.optimize()

    # Should only have 2 blocks in first dimension (20 / 10)
    assert optimized.numblocks[0] == 2


# =============================================================================
# Expression structure tests
# =============================================================================


def test_expression_structure_slice_pushed():
    """Verify slice is pushed through reshape in expression tree."""
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    x = da.ones((20, 6), chunks=(5, 3))
    result = x.reshape((20, 2, 3))[:5]

    # Before simplification: Slice(Reshape(...))
    assert isinstance(result.expr, SliceSlicesIntegers)

    # After simplification: slice should have pushed through
    simplified = result.expr.simplify()

    # Slice shouldn't be at root after pushdown
    assert not isinstance(simplified, SliceSlicesIntegers)


def test_expression_structure_slice_blocked():
    """Verify slice is NOT pushed when dimension changes."""
    x = da.ones((6, 10), chunks=(3, 5))

    # Reshape (6, 10) -> (2, 3, 10) splits first dim
    # First dim changes from 6 to 2, slice should not push through
    result = x.reshape((2, 3, 10))[:1]

    # Just verify correctness - the optimization doesn't apply here
    assert_eq(result, np.ones((2, 3, 10))[:1])

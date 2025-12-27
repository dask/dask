"""Tests for pushing integer slices through transpose operations."""

import pytest

da = pytest.importorskip("dask.array")

from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_transpose_integer_slice_2d():
    """x.T[0] should optimize to x[:, 0] (transpose eliminated)."""
    x = da.ones((3, 4), chunks=2)

    # Naive expression
    result = x.T[0]

    # Expected: direct slice without transpose
    expected = x[:, 0]

    # Optimized naive should match expected structure
    assert result.expr.optimize()._name == expected.expr.optimize()._name

    # Verify correctness
    assert_eq(result, expected)


def test_transpose_integer_slice_scalar():
    """x.T[0, 0] should optimize to x[0, 0] (transpose eliminated)."""
    x = da.ones((3, 4), chunks=2)

    # Naive expression
    result = x.T[0, 0]

    # Expected: direct slice without transpose
    expected = x[0, 0]

    # Optimized naive should match expected structure
    assert result.expr.optimize()._name == expected.expr.optimize()._name

    # Verify correctness
    assert_eq(result, expected)


def test_transpose_integer_slice_3d():
    """x.T[0] on 3D should optimize to x[:, :, 0].T (slice pushed through)."""
    x = da.ones((2, 3, 4), chunks=2)

    # Naive expression: x.T[0] where x.T has shape (4, 3, 2)
    result = x.T[0]

    # Expected: slice first, then transpose remaining dims
    # x[:, :, 0] has shape (2, 3), then transpose to (3, 2)
    expected = x[:, :, 0].T

    # Optimized naive should match expected structure
    assert result.expr.optimize()._name == expected.expr.optimize()._name

    # Verify correctness
    assert_eq(result, expected)


def test_transpose_mixed_slice_integer():
    """x.T[:, 0, :] should push integer through transpose."""
    x = da.ones((3, 4, 5), chunks=2)

    # Naive expression: x.T[:, 0, :] where x.T has shape (5, 4, 3)
    result = x.T[:, 0, :]

    # Expected: x[:, 0, :] then transpose remaining (5, 3) -> (5, 3) with axes (0, 1)
    # Actually x.T[:, 0, :] selects middle dim of x.T which is dim 1 of x
    # So: x[:, 0, :].transpose((1, 0))
    expected = x[:, 0, :].transpose((1, 0))

    # Optimized naive should match expected structure
    assert result.expr.optimize()._name == expected.expr.optimize()._name

    # Verify correctness
    assert_eq(result, expected)


def test_transpose_custom_axes_integer_slice():
    """Integer slice with custom transpose axes."""
    x = da.ones((2, 3, 4), chunks=2)

    # Naive: x.transpose((2, 0, 1))[0] - shape (4, 2, 3) -> slice dim 0 -> (2, 3)
    result = x.transpose((2, 0, 1))[0]

    # Expected: x[:, :, 0] then transpose remaining with reduced axes
    # Original axes (2, 0, 1): out[0]=in[2], out[1]=in[0], out[2]=in[1]
    # Slice out[0] (which is in[2]) -> remaining: out[1]=in[0], out[2]=in[1]
    # After renumbering: axes (0, 1) = identity, so no transpose needed
    expected = x[:, :, 0]

    # Optimized naive should match expected structure
    assert result.expr.optimize()._name == expected.expr.optimize()._name

    # Verify correctness
    assert_eq(result, expected)


def test_transpose_slice_task_count():
    """Verify task count reduction when slice pushes through transpose."""
    from dask.array._array_expr._collection import Array

    x = da.ones((4, 6), chunks=2)

    # Without optimization: slice(transpose(ones)) has transpose layer
    result = x.T[0]
    unopt_graph = dict(result.__dask_graph__())

    # With optimization: transpose is eliminated, becomes slice(ones)
    optimized = result.expr.optimize()
    opt_result = Array(optimized)
    opt_graph = dict(opt_result.__dask_graph__())

    # Optimized should have fewer tasks (no transpose layer)
    assert len(opt_graph) < len(
        unopt_graph
    ), f"Optimized graph should be smaller: {len(opt_graph)} vs {len(unopt_graph)}"

    # Specifically: unoptimized has ones(6) + transpose(6) + getitem(2) = 14
    # Optimized has ones(6) + getitem(2) = 8 (transpose eliminated)
    assert (
        len(opt_graph) == 8
    ), f"Expected 8 tasks (6 ones + 2 getitem), got {len(opt_graph)}"
    assert (
        len(unopt_graph) == 14
    ), f"Expected 14 unoptimized tasks, got {len(unopt_graph)}"


# --- Transpose through Elemwise Tests ---


def test_transpose_pushes_through_elemwise_add():
    """(x + y).T should optimize to x.T + y.T."""
    x = da.ones((10, 5), chunks=5)
    y = da.ones((10, 5), chunks=5)

    result = (x + y).T
    expected = x.T + y.T

    # Simplified should match expected structure
    assert result.expr.simplify()._name == expected.expr.simplify()._name

    # Verify correctness
    assert_eq(result, expected)


def test_transpose_pushes_through_elemwise_mul():
    """(x * y).T should optimize to x.T * y.T."""
    x = da.ones((6, 4), chunks=2)
    y = da.ones((6, 4), chunks=2)

    result = (x * y).T
    expected = x.T * y.T

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_transpose_through_elemwise_broadcasting_no_pushdown():
    """Transpose doesn't push through elemwise when broadcasting (different ndim)."""
    from dask.array._array_expr.manipulation._transpose import Transpose

    x = da.ones((6, 4), chunks=2)
    y = da.ones((4,), chunks=2)  # broadcasts along axis 1

    result = (x + y).T  # (6, 4) + (4,) -> (6, 4), then .T -> (4, 6)

    # We don't push through broadcasting cases, so outer op is still Transpose
    opt = result.expr.simplify()
    assert isinstance(opt, Transpose)

    # But result is still correct
    import numpy as np

    x_np = np.ones((6, 4))
    y_np = np.ones((4,))
    assert_eq(result, (x_np + y_np).T)


def test_transpose_pushes_through_elemwise_scalar():
    """Transpose through elemwise with scalar."""
    x = da.ones((5, 3), chunks=2)

    result = (x + 1).T
    expected = x.T + 1

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_transpose_pushes_through_unary_elemwise():
    """Transpose through unary elemwise (e.g. negative)."""
    x = da.ones((4, 6), chunks=2)

    result = (-x).T
    expected = -(x.T)

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


def test_transpose_custom_axes_through_elemwise():
    """Custom transpose axes through elemwise."""
    x = da.ones((2, 3, 4), chunks=2)
    y = da.ones((2, 3, 4), chunks=2)

    result = (x + y).transpose((2, 0, 1))
    expected = x.transpose((2, 0, 1)) + y.transpose((2, 0, 1))

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)

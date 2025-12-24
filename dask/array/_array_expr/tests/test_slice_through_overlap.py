"""Tests for slice pushdown through MapOverlap expressions.

These tests verify that slicing operations can be pushed through overlap
operations when safe, reducing computation by slicing input arrays before
applying overlap boundaries.

Note: Tests use overlap_internal for structure tests (testing the optimization)
and map_overlap for correctness tests (verifying user-facing behavior).
The full map_overlap pipeline includes Trim operations that currently block
some slice pushdown due to ArrayValuesDep metadata.
"""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


# Identity function for map_overlap - preserves values while adding overlap
def identity(x):
    return x


# =============================================================================
# Case 1: Slice on non-overlap axis (should push through)
# =============================================================================


def test_slice_through_overlap_non_overlap_axis():
    """Slice on axis without overlap pushes through."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    # Overlap only on axis 0
    overlap_result = overlap_internal(x, {0: 2})

    # Slice on axis 1 (no overlap)
    sliced = overlap_result[:, :20]
    expected = overlap_internal(x[:, :20], {0: 2})

    # Both should simplify to the same expression
    assert sliced.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(sliced, expected)


def test_slice_through_overlap_expression_structure():
    """Verify expression structure after pushdown."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})
    sliced = overlap_result[:, :20]

    expected = overlap_internal(x[:, :20], {0: 2})

    # Both should simplify to the same expression
    assert sliced.expr.simplify()._name == expected.expr.simplify()._name


def test_slice_through_overlap_middle_slice():
    """Slice in the middle of non-overlap axis."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})

    # Middle slice on axis 1 (no overlap)
    sliced = overlap_result[:, 30:70]
    expected = overlap_internal(x[:, 30:70], {0: 2})

    # Both should simplify to the same expression
    assert sliced.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(sliced, expected)


def test_slice_through_overlap_correctness():
    """Verify slice through overlap produces correct values."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(64).reshape((8, 8))
    x = da.from_array(arr, chunks=(4, 4))

    overlap_result = overlap_internal(x, {0: 2})

    # Slice on axis 1
    result = overlap_result[:, 2:6]
    expected = overlap_internal(x[:, 2:6], {0: 2})

    # Both should simplify to the same expression
    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, expected)


# =============================================================================
# Case 2: Slice on overlap axis (pushes through with padding)
# =============================================================================


def test_slice_on_overlap_axis_pushes_with_padding():
    """Slice on axis with overlap pushes through with padded input."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})

    # Slice on axis 0 (has overlap) - should push through with padded input
    sliced = overlap_result[:50, :]

    simplified = sliced.expr.simplify()
    # Should be Slice(MapOverlap(...)) with smaller input
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)
    # Slice [:50] covers output chunks 0-3 (12+14+14+10), input chunks 0-3 + padding
    # Input chunks 0-3 = 40 elements + right padding 2 = 42
    assert simplified.array.array.shape[0] == 42

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:50, :])


def test_slice_on_both_axes_one_has_overlap():
    """Slice on both axes when one has overlap pushes through with padding."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})
    sliced = overlap_result[:50, :50]  # Slice axis 0 (overlap) and axis 1

    simplified = sliced.expr.simplify()
    # Outer slice for trimming, MapOverlap with sliced input
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)
    # Axis 0: [:50] covers chunks 0-3, input = 40 + padding 2 = 42
    # Axis 1: directly sliced to 50
    assert simplified.array.array.shape == (42, 50)

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:50, :50])


def test_slice_on_overlap_axis_middle():
    """Slice starting at 0 on overlap axis pushes through with padding."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})

    # Slice on axis 0 starting at 0
    sliced = overlap_result[:60, :]

    simplified = sliced.expr.simplify()
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)
    # [:60] covers chunks 0-4, input = 50 + padding 2 = 52
    assert simplified.array.array.shape[0] == 52

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:60, :])


# =============================================================================
# Case 3: Multi-dimensional overlap
# =============================================================================


def test_slice_through_2d_overlap():
    """Slice through 2D overlap - pushes when beneficial."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 1, 1: 1})

    # Slice on axis 1 - should push through with padding
    sliced = overlap_result[:, :40]

    simplified = sliced.expr.simplify()
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)
    # Axis 1 padded: 40+1=41
    assert simplified.array.array.shape[1] == 41

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:, :40])


def test_slice_through_2d_overlap_middle():
    """Middle slice through 2D overlap on non-overlap dimension."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    # Overlap only on axis 0
    overlap_result = overlap_internal(x, {0: 2})

    # Middle slice on axis 1 (no overlap)
    sliced = overlap_result[:, 25:75]
    expected = overlap_internal(x[:, 25:75], {0: 2})

    # Both should simplify to the same expression
    assert sliced.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(sliced, expected)


def test_slice_through_1d_overlap_on_3d_array():
    """Slice on multiple non-overlap axes."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(1000).reshape((10, 10, 10))
    x = da.from_array(arr, chunks=(5, 5, 5))

    # Overlap only on axis 0
    overlap_result = overlap_internal(x, {0: 1})

    # Slice on axes 1 and 2 (neither has overlap)
    sliced = overlap_result[:, :3, :3]
    expected = overlap_internal(x[:, :3, :3], {0: 1})

    # Both should simplify to the same expression
    assert sliced.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(sliced, expected)


# =============================================================================
# Case 4: Asymmetric overlap
# =============================================================================


def test_slice_through_asymmetric_overlap():
    """Slice through asymmetric overlap (different left/right depth)."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal

    arr = np.arange(64).reshape((8, 8))
    x = da.from_array(arr, chunks=(4, 4))

    # Asymmetric overlap on axis 0
    overlap_result = overlap_internal(x, {0: (2, 1)})

    # Slice on axis 1 (no overlap)
    sliced = overlap_result[:, 2:6]

    simplified = sliced.expr.simplify()
    assert isinstance(simplified, MapOverlap)

    expected = overlap_internal(x[:, 2:6], {0: (2, 1)})
    assert_eq(sliced, expected)


def test_slice_on_asymmetric_overlap_axis_pushes():
    """Slice on axis with asymmetric overlap pushes through with padding."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: (2, 1)})
    sliced = overlap_result[:50, :]  # Slice axis 0 with asymmetric overlap

    simplified = sliced.expr.simplify()
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)
    # [:50] covers output chunks 0-3, input = 40 + right_padding 1 = 41
    assert simplified.array.array.shape[0] == 41

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:50, :])


def test_small_slice_on_overlap_axis_blocked():
    """Very small slice where padding covers full axis should block."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(100).reshape((10, 10))
    x = da.from_array(arr, chunks=(5, 5))

    # Large overlap relative to array size
    overlap_result = overlap_internal(x, {0: 5})

    # Slice [:3] with depth 5 means padded would be [:8] which is close to full
    sliced = overlap_result[:3, :]

    simplified = sliced.expr.simplify()
    # When padded slice covers full axis but original doesn't, we block
    # Because there's no benefit (no input reduction)
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:3, :])


# =============================================================================
# Case 5: Zero overlap (edge case)
# =============================================================================


def test_slice_through_zero_overlap():
    """Slice through axis with zero overlap pushes through."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    # Zero overlap on axis 0
    overlap_result = overlap_internal(x, {0: 0})

    # Slice on axis 0 - zero overlap means no blocking
    sliced = overlap_result[:50, :]

    simplified = sliced.expr.simplify()
    assert isinstance(simplified, MapOverlap)

    # Also test middle slice
    sliced_mid = overlap_result[20:70, :]
    simplified_mid = sliced_mid.expr.simplify()
    assert isinstance(simplified_mid, MapOverlap)

    # Verify correctness
    expected = overlap_internal(x[:50, :], {0: 0})
    assert_eq(sliced, expected)
    expected_mid = overlap_internal(x[20:70, :], {0: 0})
    assert_eq(sliced_mid, expected_mid)


def test_slice_through_empty_depth():
    """Slice through overlap with empty depth dict."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    # No overlap at all
    overlap_result = overlap_internal(x, {})

    sliced = overlap_result[:50, :50]

    simplified = sliced.expr.simplify()
    assert isinstance(simplified, MapOverlap)

    # Also test middle slice
    sliced_mid = overlap_result[20:70, 30:80]
    simplified_mid = sliced_mid.expr.simplify()
    assert isinstance(simplified_mid, MapOverlap)

    # Verify correctness
    expected = overlap_internal(x[:50, :50], {})
    assert_eq(sliced, expected)
    expected_mid = overlap_internal(x[20:70, 30:80], {})
    assert_eq(sliced_mid, expected_mid)


# =============================================================================
# Case 6: Task reduction verification
# =============================================================================


def test_slice_through_overlap_reduces_tasks():
    """Verify slice pushdown reduces task count."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})

    full = overlap_result
    sliced = overlap_result[:, :10]  # Take only first 10 columns
    sliced_mid = overlap_result[:, 40:60]  # Middle slice

    full_tasks = len(full.optimize().__dask_graph__())
    sliced_tasks = len(sliced.optimize().__dask_graph__())
    sliced_mid_tasks = len(sliced_mid.optimize().__dask_graph__())

    # Sliced should have fewer tasks (processes 1 column of chunks vs 10)
    assert sliced_tasks < full_tasks
    assert sliced_mid_tasks < full_tasks


def test_slice_through_overlap_reduces_numblocks():
    """Verify slice pushdown reduces number of output blocks."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})
    sliced = overlap_result[:, :10]
    sliced_mid = overlap_result[:, 40:60]  # Middle slice aligned to chunks

    # Full result: 10x10 chunks
    assert overlap_result.numblocks == (10, 10)

    # Sliced result: 10x1 chunks (only 1 column of blocks)
    assert sliced.numblocks == (10, 1)

    # Middle sliced result: 10x2 chunks (2 columns of blocks)
    assert sliced_mid.numblocks == (10, 2)


# =============================================================================
# Case 7: Correctness with computed values
# =============================================================================


@pytest.mark.parametrize(
    "shape,chunks,depth,slice_",
    [
        # Start slices (:n form) on non-overlap axes
        ((80, 80), (20, 20), {0: 2}, (slice(None), slice(20))),
        ((80, 80), (20, 20), {1: 2}, (slice(20), slice(None))),
        ((60, 60, 60), (20, 20, 20), {0: 1}, (slice(None), slice(20), slice(20))),
        ((60, 60, 60), (20, 20, 20), {1: 1}, (slice(20), slice(None), slice(20))),
        ((60, 60, 60), (20, 20, 20), {2: 1}, (slice(20), slice(20), slice(None))),
        # Middle slices (k:n form) on non-overlap axes
        ((80, 80), (20, 20), {0: 2}, (slice(None), slice(20, 60))),
        ((80, 80), (20, 20), {1: 2}, (slice(20, 60), slice(None))),
        (
            (60, 60, 60),
            (20, 20, 20),
            {0: 1},
            (slice(None), slice(10, 50), slice(15, 45)),
        ),
        (
            (60, 60, 60),
            (20, 20, 20),
            {1: 1},
            (slice(10, 50), slice(None), slice(20, 40)),
        ),
        # End slices (k: form) on non-overlap axes
        ((80, 80), (20, 20), {0: 2}, (slice(None), slice(40, None))),
        ((80, 80), (20, 20), {1: 2}, (slice(40, None), slice(None))),
    ],
)
def test_slice_through_overlap_parametrized(shape, chunks, depth, slice_):
    """Parametrized correctness tests for slice through overlap."""
    from dask.array._array_expr._overlap import overlap_internal

    arr = np.arange(np.prod(shape)).reshape(shape)
    x = da.from_array(arr, chunks=chunks)

    overlap_result = overlap_internal(x, depth)
    sliced = overlap_result[slice_]

    # Build expected: slice input first, then overlap
    input_sliced = x[slice_]
    expected = overlap_internal(input_sliced, depth)

    assert_eq(sliced, expected)


# =============================================================================
# Case 8: Expression structure verification (slice elimination)
# =============================================================================


def test_slice_eliminated_with_from_array():
    """Slice should fuse with from_array, eliminating SliceSlicesIntegers."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(64).reshape((8, 8))
    x = da.from_array(arr, chunks=(4, 4))

    overlap_result = overlap_internal(x, {0: 2})
    sliced = overlap_result[:, 2:6]

    # After full optimization, the slice should be absorbed into from_array
    optimized = sliced.optimize()

    # Walk the expression tree to verify no SliceSlicesIntegers remains
    def has_slice_expr(expr):
        if isinstance(expr, SliceSlicesIntegers):
            return True
        for dep in getattr(expr, "dependencies", lambda: [])():
            if has_slice_expr(dep):
                return True
        return False

    assert not has_slice_expr(
        optimized.expr
    ), "SliceSlicesIntegers should be eliminated after optimization"

    # Verify MapOverlap is still present
    def has_map_overlap(expr):
        if isinstance(expr, MapOverlap):
            return True
        for dep in getattr(expr, "dependencies", lambda: [])():
            if has_map_overlap(dep):
                return True
        return False

    assert has_map_overlap(optimized.expr), "MapOverlap should be in optimized tree"

    # Verify correctness
    expected = overlap_internal(x[:, 2:6], {0: 2})
    assert_eq(sliced, expected)


def test_slice_eliminated_with_from_array_middle():
    """Middle slice should also fuse with from_array."""
    from dask.array._array_expr._overlap import overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})
    sliced = overlap_result[:, 30:70]

    optimized = sliced.optimize()

    def has_slice_expr(expr):
        if isinstance(expr, SliceSlicesIntegers):
            return True
        for dep in getattr(expr, "dependencies", lambda: [])():
            if has_slice_expr(dep):
                return True
        return False

    assert not has_slice_expr(optimized.expr)

    # Verify correctness
    expected = overlap_internal(x[:, 30:70], {0: 2})
    assert_eq(sliced, expected)


def test_expression_structure_after_simplify():
    """Verify the expression structure immediately after simplify."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(64).reshape((8, 8))
    x = da.from_array(arr, chunks=(4, 4))

    overlap_result = overlap_internal(x, {0: 2})
    sliced = overlap_result[:, 2:6]

    assert isinstance(sliced.expr, SliceSlicesIntegers)
    assert isinstance(sliced.expr.array, MapOverlap)

    # After simplify: MapOverlap(Slice(FromArray)) or MapOverlap(FromArray with region)
    simplified = sliced.expr.simplify()

    # MapOverlap should be the root
    assert isinstance(simplified, MapOverlap)


def test_expression_structure_blocked_slice():
    """Verify slice remains when blocking condition applies (small slice)."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(100).reshape((10, 10))
    x = da.from_array(arr, chunks=(5, 5))

    # Large depth relative to array
    overlap_result = overlap_internal(x, {0: 5})
    # Slice [:3] where padded [:8] covers most of the axis
    sliced = overlap_result[:3, :]

    simplified = sliced.expr.simplify()

    # Slice should remain wrapping MapOverlap (no push through - no benefit)
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:3, :])


def test_expression_structure_pushed_slice():
    """Verify slice pushes through with padded input when beneficial."""
    from dask.array._array_expr._overlap import MapOverlap, overlap_internal
    from dask.array._array_expr.slicing import SliceSlicesIntegers

    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    overlap_result = overlap_internal(x, {0: 2})
    sliced = overlap_result[:50, :]

    simplified = sliced.expr.simplify()

    # Slice wraps MapOverlap, but MapOverlap has sliced (padded) input
    assert isinstance(simplified, SliceSlicesIntegers)
    assert isinstance(simplified.array, MapOverlap)
    # [:50] covers output chunks 0-3, input = 40 + padding 2 = 42
    assert simplified.array.array.shape[0] == 42

    # Verify correctness
    assert_eq(sliced, overlap_result.compute()[:50, :])


# =============================================================================
# Case 9: User-facing map_overlap API correctness tests
# These test the full pipeline including Trim, verifying values are correct.
# =============================================================================


def test_map_overlap_slice_correctness():
    """Verify slicing map_overlap output produces correct values."""
    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    result = x.map_overlap(identity, depth={0: 2}, boundary="none")
    sliced = result[:, :50]

    expected = x[:, :50].map_overlap(identity, depth={0: 2}, boundary="none")
    assert_eq(sliced, expected)


def test_map_overlap_middle_slice_correctness():
    """Verify middle slicing map_overlap output produces correct values."""
    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    result = x.map_overlap(identity, depth={0: 2}, boundary="none")
    sliced = result[:, 30:70]

    expected = x[:, 30:70].map_overlap(identity, depth={0: 2}, boundary="none")
    assert_eq(sliced, expected)


def test_map_overlap_no_trim_slice_pushes():
    """With trim=False, slice should push through to input."""
    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    # With trim=False, there's no Trim wrapper, so slice can push through
    result = x.map_overlap(identity, depth={0: 2}, boundary="none", trim=False)
    sliced = result[:, :30]

    # Check the slice pushed through
    simplified = sliced.expr.simplify()

    # The innermost FromArray should have sliced shape
    # Walk the tree to find it
    def find_from_array_shape(expr):
        from dask.array._array_expr.io._from_array import FromArray

        if isinstance(expr, FromArray):
            return expr.shape
        for dep in getattr(expr, "dependencies", lambda: [])():
            result = find_from_array_shape(dep)
            if result:
                return result
        return None

    input_shape = find_from_array_shape(simplified)
    # Should be sliced on axis 1
    assert input_shape is not None
    assert input_shape[1] == 30  # Axis 1 sliced to 30


def test_map_overlap_uniform_depth_correctness():
    """Test with uniform depth (int instead of dict)."""
    arr = np.arange(10000).reshape((100, 100))
    x = da.from_array(arr, chunks=(10, 10))

    result = x.map_overlap(identity, depth=2, boundary="none")
    sliced = result[:, :30]

    expected = x[:, :30].map_overlap(identity, depth=2, boundary="none")
    assert_eq(sliced, expected)

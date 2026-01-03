"""Tests for coarse slice pushdown through Blockwise with adjust_chunks.

When a Blockwise has adjust_chunks set, we can't push the exact slice through
because input/output chunk boundaries don't align. However, we CAN still do
a "coarse" optimization: if the output slice only needs certain blocks, we
only need the corresponding input blocks.

The coarse slice selects whole input blocks, then the original output slice
trims to the exact elements needed.
"""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_coarse_slice_simple():
    """Slice selecting first output block only needs first input block.

    x: 10 blocks of 10 elements (chunks=10)
    y = map_blocks(double, x): 10 blocks of 20 elements (chunks=20)
    y[:20] only needs output block 0, which only needs input block 0.

    Result should be equivalent to: map_blocks(double, x[:10])
    Since output is exactly 20 elements (one block), no outer slice needed.
    """
    arr = np.arange(100)
    x = da.from_array(arr, chunks=10)

    def double_elements(block):
        return np.repeat(block, 2)

    y = da.map_blocks(double_elements, x, chunks=(20,), dtype=arr.dtype)
    result = y[:20]

    # Expected: coarse-slice input, apply blockwise (output exactly matches)
    expected = da.map_blocks(double_elements, x[:10], chunks=(20,), dtype=arr.dtype)

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, np.repeat(arr, 2)[:20])


def test_coarse_slice_middle_blocks():
    """Slice selecting middle blocks coarse-slices input accordingly.

    y[40:80] needs output blocks 2-3, which need input blocks 2-3.
    Result should be equivalent to: map_blocks(double, x[20:40])
    Since we select exactly 2 full blocks (40 elements), no outer slice needed.
    """
    arr = np.arange(100)
    x = da.from_array(arr, chunks=10)

    def double_elements(block):
        return np.repeat(block, 2)

    y = da.map_blocks(double_elements, x, chunks=(20,), dtype=arr.dtype)
    result = y[40:80]

    # Expected: x[20:40] selects input blocks 2-3
    expected = da.map_blocks(double_elements, x[20:40], chunks=(20,), dtype=arr.dtype)

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, np.repeat(arr, 2)[40:80])


def test_coarse_slice_partial_block():
    """Slice that doesn't align to block boundaries needs output trimming.

    y[30:50] spans parts of blocks 1 and 2.
    We need input blocks 1-2 (x[10:30]), then slice output [10:30].
    """
    arr = np.arange(100)
    x = da.from_array(arr, chunks=10)

    def double_elements(block):
        return np.repeat(block, 2)

    y = da.map_blocks(double_elements, x, chunks=(20,), dtype=arr.dtype)
    result = y[30:50]

    # Expected: coarse slice input blocks 1-2, then trim output
    # Block 1 output is [20:40], block 2 output is [40:60]
    # We want [30:50], relative to start of block 1 (offset 20) = [10:30]
    coarse_input = x[10:30]  # input blocks 1-2
    coarse_output = da.map_blocks(
        double_elements, coarse_input, chunks=(20,), dtype=arr.dtype
    )
    expected = coarse_output[10:30]

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, np.repeat(arr, 2)[30:50])


def test_coarse_slice_2d_adjusted_axis():
    """2D with adjust_chunks on axis 0, coarse slice on that axis.

    y[:10, :] needs output block row 0, which needs input block row 0.
    """
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    def double_rows(block):
        return np.repeat(block, 2, axis=0)

    y = da.map_blocks(double_rows, x, chunks=(10, 5), dtype=arr.dtype)
    result = y[:10, :]

    # Coarse slice: x[:5, :] selects first row of blocks
    expected = da.map_blocks(double_rows, x[:5, :], chunks=(10, 5), dtype=arr.dtype)

    assert result.expr.simplify()._name == expected.expr.simplify()._name
    assert_eq(result, np.repeat(arr, 2, axis=0)[:10, :])


def test_coarse_optimization_reduces_tasks():
    """Verify that coarse slice optimization actually reduces task count."""
    arr = np.arange(1000)
    x = da.from_array(arr, chunks=10)  # 100 blocks

    def double_elements(block):
        return np.repeat(block, 2)

    y = da.map_blocks(double_elements, x, chunks=(20,), dtype=arr.dtype)

    full_tasks = len(y.optimize().__dask_graph__())

    # Slice selecting 10% of output (first 10 blocks out of 100)
    sliced = y[:200]
    sliced_tasks = len(sliced.optimize().__dask_graph__())

    # With coarse optimization: ~10 input blocks + ~10 map_blocks + getitem overhead
    # Without optimization: 100 + 100 + slice
    # Should see significant reduction
    assert (
        sliced_tasks < full_tasks / 3
    ), f"Expected significant task reduction: {sliced_tasks} < {full_tasks / 3}"


def test_coarse_slice_multi_input():
    """Coarse slice through blockwise with multiple inputs.

    Both inputs need to be coarse-sliced to select only needed blocks.
    """
    arr1 = np.arange(100)
    arr2 = np.arange(100, 200)
    x = da.from_array(arr1, chunks=10)
    y = da.from_array(arr2, chunks=10)

    def combine_double(a, b):
        return np.repeat(a + b, 2)

    z = da.blockwise(
        combine_double,
        "i",
        x,
        "i",
        y,
        "i",
        dtype=arr1.dtype,
        adjust_chunks={"i": lambda c: c * 2},
    )
    result = z[:20]

    # Values should be correct
    assert_eq(result, np.repeat(arr1 + arr2, 2)[:20])

    # Optimization should reduce tasks (10+10 blockwise + 10+10 inputs -> ~2+1)
    full_tasks = len(z.optimize().__dask_graph__())
    sliced_tasks = len(result.optimize().__dask_graph__())
    assert sliced_tasks < full_tasks / 2


def test_coarse_slice_correctness_various():
    """Value correctness tests for various slice patterns."""
    arr = np.arange(100)
    x = da.from_array(arr, chunks=10)

    def double_elements(block):
        return np.repeat(block, 2)

    y = da.map_blocks(double_elements, x, chunks=(20,), dtype=arr.dtype)
    expected_full = np.repeat(arr, 2)

    # Test various slices
    slices = [
        slice(0, 20),  # First block
        slice(20, 60),  # Blocks 1-2
        slice(180, 200),  # Last block
        slice(15, 45),  # Partial blocks
        slice(0, 100),  # First half
        slice(100, 200),  # Second half
    ]

    for slc in slices:
        result = y[slc]
        assert_eq(result, expected_full[slc], err_msg=f"Failed for slice {slc}")


def test_coarse_slice_with_broadcast():
    """Coarse slice through blockwise with broadcasting input.

    When one input broadcasts (has fewer dimensions or size-1 chunks),
    the coarse slice should only select needed blocks from the non-broadcast input.
    """
    arr = np.arange(100).reshape(10, 10)
    vec = np.arange(10)

    x = da.from_array(arr, chunks=(5, 5))
    v = da.from_array(vec, chunks=5)

    # Broadcast multiply with adjust_chunks on axis 0
    def double_rows(a, b):
        return np.repeat(a * b, 2, axis=0)

    # Use blockwise with broadcasting
    z = da.blockwise(
        double_rows,
        "ij",
        x,
        "ij",
        v,
        "j",  # v broadcasts along axis 0
        dtype=arr.dtype,
        adjust_chunks={"i": lambda c: c * 2},
    )
    assert z.shape == (20, 10)

    # Slice first 10 rows (output block 0)
    result = z[:10, :]

    # Values should be correct
    expected = np.repeat(arr * vec, 2, axis=0)[:10, :]
    assert_eq(result, expected)

    # Task reduction check
    full_tasks = len(z.optimize().__dask_graph__())
    sliced_tasks = len(result.optimize().__dask_graph__())
    assert sliced_tasks < full_tasks


def test_coarse_slice_dimension_reorder():
    """Coarse slice through blockwise that reorders dimensions (ij -> ji).

    When blockwise transposes indices, output block (i, j) depends on
    input block (j, i). The coarse slice needs to correctly map output
    block ranges to input block ranges.
    """
    arr = np.arange(100).reshape(10, 10)
    x = da.from_array(arr, chunks=(5, 5))

    # Transpose with row doubling
    def transpose_double(block):
        return np.repeat(block.T, 2, axis=0)

    z = da.blockwise(
        transpose_double,
        "ji",  # Output is ji (transposed)
        x,
        "ij",
        dtype=arr.dtype,
        adjust_chunks={"j": lambda c: c * 2},  # j (output axis 0) gets doubled
    )
    assert z.shape == (20, 10)

    expected_full = np.repeat(arr.T, 2, axis=0)

    # Test slices on both axes
    for slc, desc in [
        ((slice(None, 10), slice(None)), "first 10 rows"),
        ((slice(None, 10), slice(None, 5)), "first quadrant"),
    ]:
        result = z[slc]
        assert_eq(result, expected_full[slc], err_msg=f"Failed for {desc}")

    # Verify task reduction
    full_tasks = len(z.optimize().__dask_graph__())
    sliced_tasks = len(z[:10, :5].optimize().__dask_graph__())
    assert sliced_tasks < full_tasks


def test_coarse_slice_tuple_adjust_chunks():
    """Coarse slice with tuple adjust_chunks (per-block specification).

    When adjust_chunks is a tuple like (5, 15, 10), it specifies exact
    chunk sizes for each block. Coarse slicing should slice this tuple
    to match the selected blocks.
    """
    arr = np.arange(30)
    x = da.from_array(arr, chunks=10)  # 3 blocks of 10

    # Use blockwise with tuple adjust_chunks directly
    def shrink_first(block):
        # First 5 elements only
        return block[:5]

    y = da.blockwise(
        shrink_first,
        "i",
        x,
        "i",
        dtype=arr.dtype,
        adjust_chunks={"i": (5, 5, 5)},  # tuple specifying per-block sizes
    )
    assert y.shape == (15,)
    assert y.chunks == ((5, 5, 5),)

    # Slice to get blocks 1-2 (output elements 5:15)
    result = y[5:]

    # Values should be correct (elements 5-9 from block 1, 5-9 from block 2)
    expected = np.concatenate([arr[10:15], arr[20:25]])
    assert_eq(result, expected)

    # Should have sliced adjust_chunks from (5,5,5) to (5,5)
    # and reduced task count
    full_tasks = len(y.optimize().__dask_graph__())
    sliced_tasks = len(result.optimize().__dask_graph__())
    assert sliced_tasks < full_tasks


def test_coarse_slice_irregular_chunks():
    """Coarse slice with non-uniform (irregular) output chunks.

    Tests that coarse slicing works correctly when output chunks have
    different sizes, as is common with map_blocks(..., chunks=...).
    """
    arr = np.arange(100)
    x = da.from_array(arr, chunks=10)  # 10 uniform input blocks

    # Output has irregular chunks: 15, 25, 15, 25, 15, 25, 15, 25, 15, 25
    def expand_variable(block):
        # Alternating expansion: some blocks grow more than others
        return np.repeat(block, 2) if block[0] % 20 == 0 else np.repeat(block, 3)

    # Manually specify the expected output chunk sizes
    output_chunks = tuple(20 if i % 2 == 0 else 30 for i in range(10))
    y = da.blockwise(
        expand_variable,
        "i",
        x,
        "i",
        dtype=arr.dtype,
        adjust_chunks={"i": output_chunks},
    )
    assert y.chunks == (output_chunks,)

    # Slice selecting middle blocks (skip first block of 20, take next 50)
    result = y[20:70]

    # Values should be correct
    expected = np.concatenate(
        [
            np.repeat(arr[10:20], 3),  # block 1: 30 elements, take all
            np.repeat(arr[20:30], 2),  # block 2: 20 elements, take all
        ]
    )
    assert_eq(result, expected)

    # Task reduction check
    full_tasks = len(y.optimize().__dask_graph__())
    sliced_tasks = len(result.optimize().__dask_graph__())
    assert sliced_tasks < full_tasks

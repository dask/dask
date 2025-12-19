"""Tests for array-expr blockwise fusion."""
from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array._array_expr._blockwise import FusedBlockwise
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_simple_chain_fusion():
    """Simple chain fuses: (x + 1) * 2"""
    x = da.ones((10,), chunks=5)
    y = (x + 1) * 2
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 3  # ones + two elemwise ops
    assert_eq(y, (np.ones(10) + 1) * 2)


def test_diamond_fusion():
    """Diamond pattern fuses: (x+1) + (x*2)"""
    x = da.ones((10,), chunks=5)
    a = x + 1
    b = x * 2
    c = a + b
    expr = c.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 4  # ones + three elemwise ops
    assert_eq(c, (np.ones(10) + 1) + (np.ones(10) * 2))


def test_no_fusion_single_op():
    """Single operation does not create FusedBlockwise"""
    # Use from_array (not fusable) to isolate a single elemwise op
    x = da.from_array(np.ones(10), chunks=5)
    y = x + 1
    expr = y.expr.optimize(fuse=True)
    assert not isinstance(expr, FusedBlockwise)


def test_task_count_reduced():
    """Fusion reduces task count"""
    x = da.ones((10, 10), chunks=5)
    y = (x + 1) * 2 + 3  # 3 elemwise ops

    unfused = y.expr.optimize(fuse=False)
    fused = y.expr.optimize(fuse=True)

    unfused_tasks = len(dict(unfused.__dask_graph__()))
    fused_tasks = len(dict(fused.__dask_graph__()))

    assert fused_tasks < unfused_tasks


def test_broadcast_fusion():
    """Fusion works with broadcasting"""
    x = da.ones((10, 10), chunks=5)  # 2D
    y = da.ones((10,), chunks=5)  # 1D, broadcasts
    z = (x + y) * 2
    expr = z.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert_eq(z, (np.ones((10, 10)) + np.ones(10)) * 2)


def test_longer_chain():
    """Longer chains fuse completely"""
    x = da.ones((10,), chunks=5)
    y = ((((x + 1) * 2) - 3) / 4) + 5
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 6  # ones + five elemwise ops
    assert_eq(y, ((((np.ones(10) + 1) * 2) - 3) / 4) + 5)


def test_fusion_with_different_chunks():
    """Fusion works when arrays have different but compatible chunks"""
    x = da.ones((12,), chunks=4)  # 3 chunks
    y = da.ones((12,), chunks=6)  # 2 chunks
    # After unify_chunks, both will have same chunking
    z = (x + y) * 2
    expr = z.expr.optimize(fuse=True)
    # May or may not fuse depending on chunk unification, but should compute correctly
    assert_eq(z, (np.ones(12) + np.ones(12)) * 2)


def test_optimize_with_fuse_false():
    """optimize(fuse=False) does not fuse"""
    x = da.ones((10,), chunks=5)
    y = (x + 1) * 2
    expr = y.expr.optimize(fuse=False)
    assert not isinstance(expr, FusedBlockwise)


def test_fusion_correctness_random():
    """Fused and unfused produce same result with random data"""
    rng = np.random.default_rng(42)
    data = rng.random((100, 100))
    x = da.from_array(data, chunks=25)
    y = ((x + 1) * 2 - 3) / 4

    # Just verify the result is correct
    assert_eq(y, ((data + 1) * 2 - 3) / 4)


def test_transpose_elemwise_fusion():
    """Transpose followed by elemwise fuses"""
    x = da.ones((6, 8), chunks=(3, 4))
    y = x.T + 1
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 3  # ones + transpose + elemwise
    assert_eq(y, np.ones((6, 8)).T + 1)
    assert y.shape == (8, 6)


def test_elemwise_transpose_elemwise_fusion():
    """Elemwise + transpose + elemwise chain fuses"""
    x = da.ones((6, 8), chunks=(3, 4))
    y = ((x + 1).T * 2)
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 4  # ones + add + transpose + mul
    assert_eq(y, (np.ones((6, 8)) + 1).T * 2)


def test_swapaxes_fusion():
    """Swapaxes (uses Transpose) fuses with elemwise"""
    x = da.ones((6, 8, 4), chunks=(3, 4, 2))
    y = da.swapaxes(x, 0, 2) + 1
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert_eq(y, np.swapaxes(np.ones((6, 8, 4)), 0, 2) + 1)


def test_transpose_broadcast_fusion():
    """Transpose with broadcast fuses correctly"""
    x = da.ones((6, 8), chunks=(3, 4))
    b = da.ones((6,), chunks=3)  # broadcasts against (8, 6) last dim
    z = ((x + 1).T + b) * 2
    expr = z.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert_eq(z, ((np.ones((6, 8)) + 1).T + np.ones(6)) * 2)


def test_creation_fusion():
    """Creation operations (ones, zeros, etc.) fuse with elemwise"""
    # ones fuses with elemwise
    x = da.ones((10,), chunks=5)
    y = x + 1
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 2  # ones + add
    assert_eq(y, np.ones(10) + 1)

    # zeros fuses too
    x = da.zeros((10,), chunks=5)
    y = x + 1
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert_eq(y, np.zeros(10) + 1)

    # full fuses too
    x = da.full((10,), 5, chunks=5)
    y = x * 2
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert_eq(y, np.full(10, 5) * 2)


def test_random_fusion():
    """Random arrays fuse with elemwise operations."""
    rng = da.random.default_rng(42)
    x = rng.random((10, 10), chunks=5)
    y = x + 1

    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 2  # elemwise + random

    # Verify correctness - values should be in [1, 2)
    result = y.compute()
    assert result.min() >= 1.0
    assert result.max() < 2.0

    # Multiple operations chain
    z = (x * 2 + 1) / 3
    expr = z.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 4  # 3 elemwise + random


def test_same_array_different_patterns():
    """Same array accessed with different index patterns (a + a.T)."""
    # a + a.T - same array, different access patterns
    a = da.ones((8, 8), chunks=4)
    b = a + a.T
    assert_eq(b, np.ones((8, 8)) + np.ones((8, 8)).T)

    # Check that add+transpose are fused, but ones stays separate
    expr = b.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    # Should have 2 exprs (add + transpose), not 3 (ones excluded due to conflict)
    assert len(expr.exprs) == 2

    # Verify graph structure
    graph = dict(expr.__dask_graph__())
    # 4 fused tasks + 4 ones tasks = 8 total
    assert len(graph) == 8

    # a * a.T - same pattern with multiplication
    a = da.arange(16, chunks=4).reshape((4, 4))
    b = a * a.T
    expected = np.arange(16).reshape((4, 4)) * np.arange(16).reshape((4, 4)).T
    assert_eq(b, expected)

    # mid + mid.T - intermediate expression with different patterns
    x = da.ones((8, 8), chunks=4)
    mid = x + 1
    y = mid + mid.T
    expected = (np.ones((8, 8)) + 1) + (np.ones((8, 8)) + 1).T
    assert_eq(y, expected)


def test_deep_nesting():
    """Deep chain of 20 operations fuses into minimal tasks."""
    x = da.ones((10,), chunks=5)
    for _ in range(20):
        x = x + 1
    assert_eq(x, np.ones(10) + 20)
    # 20 ops should fuse to just 2 tasks (one per chunk)
    assert len(dict(x.optimize().__dask_graph__())) == 2


def test_wide_diamond():
    """Wide diamond with 10 branches fuses into minimal tasks."""
    x = da.ones((10,), chunks=5)
    branches = [x + i for i in range(10)]
    y = sum(branches)
    assert_eq(y, np.ones(10) * 10 + 45)
    # All branches should fuse to just 2 tasks (one per chunk)
    assert len(dict(y.optimize().__dask_graph__())) == 2


def test_complex_broadcasting():
    """Complex broadcasting with 3D, 2D, and 1D arrays fuses."""
    a = da.ones((4, 3, 2), chunks=2)
    b = da.ones((3, 1), chunks=2)
    c = da.ones((2,), chunks=2)
    d = (a + b) * c
    expected = (np.ones((4, 3, 2)) + np.ones((3, 1))) * np.ones(2)
    assert_eq(d, expected)
    # Should fuse - verify reasonable task count
    assert len(dict(d.optimize().__dask_graph__())) <= 8


def test_broadcasting_outer_product():
    """Broadcasting (N,1) + (1,N) creates outer-product-like pattern."""
    a = da.ones((10, 1), chunks=5)
    b = da.ones((1, 10), chunks=5)
    c = a + b
    assert_eq(c, np.ones((10, 1)) + np.ones((1, 10)))
    # 2x2 output blocks, should fuse
    assert len(dict(c.optimize().__dask_graph__())) == 4

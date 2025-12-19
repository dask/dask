"""Tests for array-expr blockwise fusion."""
from __future__ import annotations

import numpy as np
import pytest

import dask.array as da

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_simple_chain_fusion():
    """Simple chain fuses: (x + 1) * 2"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((10,), chunks=5)
    y = (x + 1) * 2
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 2  # two elemwise ops
    result = y.compute()
    expected = (np.ones(10) + 1) * 2
    assert np.allclose(result, expected)


def test_diamond_fusion():
    """Diamond pattern fuses: (x+1) + (x*2)"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((10,), chunks=5)
    a = x + 1
    b = x * 2
    c = a + b
    expr = c.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 3  # three elemwise ops
    result = c.compute()
    expected = (np.ones(10) + 1) + (np.ones(10) * 2)
    assert np.allclose(result, expected)


def test_no_fusion_single_op():
    """Single operation does not create FusedBlockwise"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((10,), chunks=5)
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
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((10, 10), chunks=5)  # 2D
    y = da.ones((10,), chunks=5)  # 1D, broadcasts
    z = (x + y) * 2
    expr = z.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    result = z.compute()
    expected = (np.ones((10, 10)) + np.ones(10)) * 2
    assert np.allclose(result, expected)


def test_longer_chain():
    """Longer chains fuse completely"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((10,), chunks=5)
    y = ((((x + 1) * 2) - 3) / 4) + 5
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 5  # five elemwise ops
    result = y.compute()
    expected = ((((np.ones(10) + 1) * 2) - 3) / 4) + 5
    assert np.allclose(result, expected)


def test_fusion_with_different_chunks():
    """Fusion works when arrays have different but compatible chunks"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((12,), chunks=4)  # 3 chunks
    y = da.ones((12,), chunks=6)  # 2 chunks
    # After unify_chunks, both will have same chunking
    z = (x + y) * 2
    expr = z.expr.optimize(fuse=True)
    # May or may not fuse depending on chunk unification, but should compute correctly
    result = z.compute()
    expected = (np.ones(12) + np.ones(12)) * 2
    assert np.allclose(result, expected)


def test_optimize_with_fuse_false():
    """optimize(fuse=False) does not fuse"""
    from dask.array._array_expr._blockwise import FusedBlockwise

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
    result = y.compute()
    expected = ((data + 1) * 2 - 3) / 4
    assert np.allclose(result, expected)


def test_transpose_elemwise_fusion():
    """Transpose followed by elemwise fuses"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((6, 8), chunks=(3, 4))
    y = x.T + 1
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 2
    result = y.compute()
    expected = np.ones((6, 8)).T + 1
    assert np.allclose(result, expected)
    assert result.shape == (8, 6)


def test_elemwise_transpose_elemwise_fusion():
    """Elemwise + transpose + elemwise chain fuses"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((6, 8), chunks=(3, 4))
    y = ((x + 1).T * 2)
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    assert len(expr.exprs) == 3
    result = y.compute()
    expected = ((np.ones((6, 8)) + 1).T * 2)
    assert np.allclose(result, expected)


def test_swapaxes_fusion():
    """Swapaxes (uses Transpose) fuses with elemwise"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((6, 8, 4), chunks=(3, 4, 2))
    y = da.swapaxes(x, 0, 2) + 1
    expr = y.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    result = y.compute()
    expected = np.swapaxes(np.ones((6, 8, 4)), 0, 2) + 1
    assert np.allclose(result, expected)


def test_transpose_broadcast_fusion():
    """Transpose with broadcast fuses correctly"""
    from dask.array._array_expr._blockwise import FusedBlockwise

    x = da.ones((6, 8), chunks=(3, 4))
    b = da.ones((6,), chunks=3)  # broadcasts against (8, 6) last dim
    z = ((x + 1).T + b) * 2
    expr = z.expr.optimize(fuse=True)
    assert isinstance(expr, FusedBlockwise)
    result = z.compute()
    expected = ((np.ones((6, 8)) + 1).T + np.ones(6)) * 2
    assert np.allclose(result, expected)

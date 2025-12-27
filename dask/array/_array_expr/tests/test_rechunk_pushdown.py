"""Tests for rechunk pushdown optimizations."""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array._array_expr.io import FromArray
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_rechunk_dict_simplifies_through_from_array():
    """Dict chunks should simplify Rechunk(FromArray) -> FromArray."""
    np_arr = np.ones((100, 50))
    darr = da.from_array(np_arr, chunks=(25, 25))

    rechunked_tuple = darr.rechunk((50, 50))
    rechunked_dict = darr.rechunk({0: 50, 1: 50})

    assert isinstance(rechunked_tuple.expr.simplify(), FromArray)
    assert isinstance(rechunked_dict.expr.simplify(), FromArray)


def test_rechunk_dict_partial_dims():
    """Dict chunks specifying only some dimensions."""
    np_arr = np.ones((100, 50))
    darr = da.from_array(np_arr, chunks=(25, 25))

    rechunked = darr.rechunk({0: 50})
    simplified = rechunked.expr.simplify()

    assert isinstance(simplified, FromArray)
    assert simplified.chunks == ((50, 50), (25, 25))


def test_rechunk_dict_correctness():
    """Dict and tuple chunks produce identical results."""
    np_arr = np.arange(100).reshape(10, 10)
    darr = da.from_array(np_arr, chunks=(5, 5))

    result_tuple = darr.rechunk((2, 3))
    result_dict = darr.rechunk({0: 2, 1: 3})

    assert_eq(result_tuple, result_dict)
    assert_eq(result_tuple, np_arr)


def test_rechunk_dict_through_elemwise():
    """Rechunk with dict chunks pushes through elemwise."""
    from dask.array._array_expr._blockwise import Elemwise

    x = da.ones((10, 10), chunks=(5, 5))
    y = da.ones((10, 10), chunks=(5, 5))

    result = (x + y).rechunk({0: 2, 1: 2})
    simplified = result.expr.simplify()

    assert isinstance(simplified, Elemwise)


def test_rechunk_dict_through_elemwise_correctness():
    """Verify correctness of rechunk through elemwise with dict chunks."""
    np_x = np.arange(100).reshape(10, 10)
    np_y = np.arange(100, 200).reshape(10, 10)
    x = da.from_array(np_x, chunks=(5, 5))
    y = da.from_array(np_y, chunks=(5, 5))

    result_tuple = (x + y).rechunk((2, 3))
    result_dict = (x + y).rechunk({0: 2, 1: 3})

    expected = np_x + np_y
    assert_eq(result_tuple, expected)
    assert_eq(result_dict, expected)


def test_rechunk_pushdown_broadcast_elemwise():
    """Test rechunk optimization through elemwise with broadcast args."""
    import dask

    # Array with broadcast dimension
    a = da.ones((10, 10), chunks=(5, 5))
    b = da.ones((10,), chunks=(5,))  # broadcasts along axis 0

    # Elemwise with broadcast, then rechunk
    c = (a + b).rechunk((2, 2))

    # Verify chunks and result
    assert c.chunks == ((2, 2, 2, 2, 2), (2, 2, 2, 2, 2))
    result = c.compute()
    assert result.shape == (10, 10)
    assert (result == 2).all()

    # Joint compute case
    d = (a * b).rechunk((2, 2)).sum(axis=0)
    e = (a - b).rechunk((2, 2)).mean(axis=0)
    r1, r2 = dask.compute(d, e)
    assert r1.shape == (10,)
    assert r2.shape == (10,)


def test_rechunk_pushdown_through_transpose():
    """Test rechunk pushes through transpose with correct chunk mapping."""
    # Create array with shape (2, 3, 4) and specific chunks
    x = da.ones((2, 3, 4), chunks=(1, 1, 2))

    # Transpose with axes (2, 0, 1): output shape (4, 2, 3)
    # Output axis 0 <- input axis 2
    # Output axis 1 <- input axis 0
    # Output axis 2 <- input axis 1
    y = x.transpose((2, 0, 1))

    # Rechunk the transposed output to (2, 1, 3)
    result = y.rechunk((2, 1, 3))

    # Build expected expression: transpose of rechunked input
    # Input axis 0 needs output axis 1's chunks = 1
    # Input axis 1 needs output axis 2's chunks = 3
    # Input axis 2 needs output axis 0's chunks = 2
    # So input rechunk should be (1, 3, 2)
    expected = x.rechunk((1, 3, 2)).transpose((2, 0, 1))

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_rechunk_pushdown_through_transpose_simple():
    """Test rechunk pushes through simple 2D transpose."""
    x = da.arange(12, chunks=4).reshape(3, 4).rechunk((1, 2))

    y = x.T  # axes = (1, 0)

    # Rechunk transposed to (2, 3)
    result = y.rechunk((2, 3))

    # After pushdown: input should be rechunked to (3, 2)
    # Because input axis 0 -> output axis 1 (needs chunks 3)
    #         input axis 1 -> output axis 0 (needs chunks 2)
    expected = x.rechunk((3, 2)).T

    assert result.expr.simplify()._name == expected.expr.simplify()._name


def test_rechunk_pushdown_through_transpose_dict():
    """Test rechunk with dict chunks through transpose."""
    x = da.ones((2, 3, 4), chunks=(1, 1, 2))

    # Transpose: output axis 0 <- input axis 2
    y = x.transpose((2, 0, 1))

    # Rechunk only output axis 0 to chunk size 2
    result = y.rechunk({0: 2})

    # Output axis 0 comes from input axis 2, so input axis 2 should be rechunked
    expected = x.rechunk({2: 2}).transpose((2, 0, 1))

    assert result.expr.simplify()._name == expected.expr.simplify()._name


# =============================================================================
# Regression tests
# =============================================================================


def test_rechunk_noop_preserves_identity():
    """Rechunk with matching chunks should return identical expression.

    Regression test: rechunk was creating new expressions even when
    target chunks matched input chunks exactly.
    """
    x = da.ones((10, 10), chunks=(5, 5))

    # All of these should return the same expression
    y_tuple = x.rechunk((5, 5))
    y_dict = x.rechunk({0: 5, 1: 5})
    y_none = x.rechunk((None, None))

    assert x.expr is y_tuple.expr
    assert x.expr is y_dict.expr
    assert x.expr is y_none.expr
    assert x.name == y_tuple.name == y_dict.name == y_none.name


def test_rechunk_noop_negative_index():
    """Rechunk no-op with negative axis index."""
    x = da.ones((10, 10), chunks=(5, 5))

    y = x.rechunk({-1: 5, -2: 5})

    assert x.expr is y.expr


def test_rechunk_multistep_no_cycle():
    """Multi-step rechunk should not create cyclic dependencies.

    Regression test: when rechunk required multiple steps (split one dim,
    merge another), incorrect task name propagation between steps caused
    cyclic task graph.
    """
    x = da.ones((16, 50), chunks=(16, 1))
    y = x.rechunk((3, 10))

    # This was raising RuntimeError: Cycle detected
    result = y.compute()
    assert result.shape == (16, 50)
    assert np.all(result == 1)


def test_rechunk_split_and_merge_correctness():
    """Verify multi-step rechunk produces correct values."""
    np_arr = np.arange(16 * 50).reshape(16, 50)
    x = da.from_array(np_arr, chunks=(16, 1))
    y = x.rechunk((3, 10))

    assert_eq(y, np_arr)

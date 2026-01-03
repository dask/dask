"""Tests for shuffle output chunk sizing with input chunk locality grouping."""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_contiguous_indexing_aligns_with_input_chunks():
    """np.repeat pattern: outputs align with input chunks -> fewer output chunks."""
    np_x = np.arange(100 * 10).reshape(100, 10)
    x = da.from_array(np_x, chunks=(25, 10))  # 4 input chunks

    # Contiguous: each input element repeated 3 times
    indexer = np.repeat(np.arange(100), 3)  # [0,0,0,1,1,1,...,99,99,99]
    result = x[indexer, :]

    # Output chunks align with input chunks (not one per output element)
    assert result.numblocks[0] == x.numblocks[0]  # 4, not 300
    assert_eq(result, np_x[indexer, :])


def test_scattered_indexing_correctness():
    """np.tile pattern: scattered access still produces correct results."""
    np_x = np.arange(100 * 10).reshape(100, 10)
    x = da.from_array(np_x, chunks=(25, 10))

    indexer = np.tile(np.arange(100), 3)  # [0,1,...,99,0,1,...,99,...]
    result = x[indexer, :]

    assert_eq(result, np_x[indexer, :])


def test_identity_indexing_no_shuffle():
    """Identity indexing should not create a shuffle."""
    from dask.array._array_expr._shuffle import Shuffle

    np_x = np.arange(120).reshape(12, 10)
    x = da.from_array(np_x, chunks=(3, 10))

    result = x[np.arange(12), :]

    assert not isinstance(result.expr, Shuffle)
    assert_eq(result, np_x)

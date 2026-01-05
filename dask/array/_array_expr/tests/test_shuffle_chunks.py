"""Tests for shuffle output chunk sizing with input chunk locality grouping."""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_contiguous_indexing_splits_to_input_chunk_size():
    """np.repeat pattern: output chunks stay close to input chunk size."""
    np_x = np.arange(100 * 10).reshape(100, 10)
    x = da.from_array(np_x, chunks=(25, 10))  # 4 input chunks of 25 each

    # Contiguous: each input element repeated 3 times
    # Each input chunk of 25 elements becomes 75 output elements
    # These get split into chunks of 25, so 3 output chunks per input chunk = 12 total
    indexer = np.repeat(np.arange(100), 3)  # [0,0,0,1,1,1,...,99,99,99]
    result = x[indexer, :]

    assert max(result.chunks[0]) == 25
    assert result.numblocks[0] == 12  # 4 input chunks * 3 splits each
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


def test_large_repeat_splits_oversized_groups():
    """np.repeat with large factor should not create oversized chunks.

    When each element is repeated many times, the output chunks should be
    split to match input chunk sizes, not grow unboundedly.
    """
    np_x = np.arange(100 * 10).reshape(100, 10)
    x = da.from_array(np_x, chunks=(25, 10))  # 4 input chunks, 25 elements each

    # Each element repeated 100 times -> naive would give chunks of 25*100=2500
    # With max input chunk size of 25, groups get split into chunks of 25
    indexer = np.repeat(np.arange(100), 100)
    result = x[indexer, :]

    assert max(result.chunks[0]) == 25
    assert_eq(result, np_x[indexer, :])

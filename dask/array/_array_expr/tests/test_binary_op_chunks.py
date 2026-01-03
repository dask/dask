"""Tests for coarse_blockdim: preferring larger chunks in binary operations.

When combining arrays with different chunk granularities, we prefer coarser
chunks (fewer blocks) when boundaries align. This reduces task overhead.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def total_chunks(arr):
    """Total number of chunks across all dimensions."""
    return math.prod(arr.numblocks)


class TestCoarseChunkPreference:
    """Tests for preferring coarser chunks when boundaries align."""

    def test_shuffle_indexed_array(self):
        """Main use case: xarray groupby pattern.

        Binary op between array with nice chunks and shuffle-indexed array
        (which has per-element chunks) should preserve the nice chunks.
        """
        # Original data: 10 chunks of size 12
        arr = da.random.random((120, 20, 30), chunks=(12, 20, 30))

        # Aggregated data indexed to match original shape
        n_groups = 4
        mean_arr = da.random.random((n_groups, 20, 30), chunks=(1, 20, 30))
        indexer = np.tile(np.arange(n_groups), 30)
        indexed_mean = mean_arr[indexer, ...]

        result = arr - indexed_mean

        # Should preserve arr's chunk count, not explode to 120
        assert total_chunks(result) <= total_chunks(arr) * 2
        assert_eq(result, arr.compute() - indexed_mean.compute())

    def test_aligned_1d(self):
        """1D: (20,20) + (10,10,10,10) -> (20,20)"""
        coarse = da.ones(40, chunks=20)
        fine = da.ones(40, chunks=10)

        result = coarse + fine

        assert result.chunks == ((20, 20),)

    def test_aligned_2d(self):
        """2D: coarse chunks preferred in both dimensions."""
        coarse = da.ones((40, 40), chunks=(20, 20))
        fine = da.ones((40, 40), chunks=(10, 10))

        result = coarse + fine

        assert result.chunks == ((20, 20), (20, 20))
        assert total_chunks(result) == 4  # not 16

    def test_multiples_align(self):
        """Chunk sizes that are multiples align: (30,30) + (10,...) -> (30,30)"""
        coarse = da.ones(60, chunks=30)
        fine = da.ones(60, chunks=10)

        result = coarse + fine

        assert result.chunks == ((30, 30),)


class TestFallbackToCommonBlockdim:
    """Tests for falling back when boundaries don't align."""

    def test_misaligned_boundaries(self):
        """(15,15) vs (10,20): boundary 15 not in {10}, must subdivide."""
        a = da.ones(30, chunks=(15, 15))
        b = da.ones(30, chunks=(10, 20))

        result = a + b

        # Neither input's chunks work; uses finest common divisor
        assert result.chunks != ((15, 15),)
        assert result.chunks != ((10, 20),)

    def test_non_divisible(self):
        """(12,12) vs (8,8,8): boundary 12 not in {8,16}, must subdivide."""
        a = da.ones(24, chunks=12)
        b = da.ones(24, chunks=8)

        result = a + b

        # More chunks than either input
        assert len(result.chunks[0]) > 2

    def test_classic_uneven(self):
        """(4,6) vs (6,4): different boundaries, uses (4,2,4)."""
        a = da.arange(10, chunks=((4, 6),))
        b = da.ones(10, chunks=((6, 4),))

        result = a + b

        assert result.chunks == ((4, 2, 4),)

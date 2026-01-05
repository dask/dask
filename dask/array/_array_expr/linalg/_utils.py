"""Shared utilities for linear algebra operations."""

from __future__ import annotations

import numpy as np


def _cumsum_blocks(it):
    """Yield cumulative (start, end) pairs for block sizes."""
    total = 0
    for x in it:
        total_previous = total
        total += x
        yield (total_previous, total)


def _nanmin(m, n):
    """Return min(m, n), handling NaN values.

    If either value is NaN, return the other value if it's not NaN,
    otherwise return NaN.
    """
    k_0 = min([m, n])
    k_1 = m if np.isnan(n) else n
    return k_1 if np.isnan(k_0) else k_0


def _has_uncertain_chunks(chunks):
    """Check if any chunk sizes are uncertain (NaN)."""
    return any(np.isnan(c) for cs in chunks for c in cs)


def _cumsum_part(last, new):
    """Compute (start, end) from previous cumsum and new value."""
    return (last[1], last[1] + new)


def _get_block_size(block):
    """Get min dimension from a block (used for R block sizes)."""
    return min(block.shape)


def _make_slice(cumsum_pair, n):
    """Create slice tuple from cumsum pair and column count."""
    return (slice(cumsum_pair[0], cumsum_pair[1]), slice(0, n))


def _getitem_with_slice(arr, slc):
    """Apply slice to array."""
    return arr[slc]


def _get_n(arr):
    """Get the number of columns from an array."""
    return arr.shape[1]


def _solve_triangular_lower(a, b):
    """Solve triangular system with lower triangular matrix."""
    from dask.array.utils import solve_triangular_safe

    return solve_triangular_safe(a, b, lower=True)


def _solve_triangular_upper(a, b):
    """Solve triangular system with upper triangular matrix."""
    from dask.array.utils import solve_triangular_safe

    return solve_triangular_safe(a, b, lower=False)


def _transpose(x):
    """Transpose a matrix (used for P_inv and U.T)."""
    return x.T

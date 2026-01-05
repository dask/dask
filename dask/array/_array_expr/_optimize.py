"""Legacy graph-level optimize API (no-op for array-expr)."""

from __future__ import annotations


def optimize(dsk, keys, **kwargs):
    """No-op for API compatibility. Use arr.optimize() instead."""
    return dsk

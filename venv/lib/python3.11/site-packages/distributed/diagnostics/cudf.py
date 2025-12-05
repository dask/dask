"""
Diagnostics for memory spilling managed by cuDF.
"""

from __future__ import annotations

try:
    from cudf.core.buffer.spill_manager import get_global_manager
except ImportError:
    get_global_manager = None


def real_time():
    if get_global_manager is None:
        return {"cudf-spilled": None}
    mgr = get_global_manager()
    if mgr is None:
        return {"cudf-spilled": None}

    totals = mgr.statistics.spill_totals

    return {
        "cudf-spilled": totals.get(("gpu", "cpu"), (0,))[0]
        - totals.get(("cpu", "gpu"), (0,))[0]
    }

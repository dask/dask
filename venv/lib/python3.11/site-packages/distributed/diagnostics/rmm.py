"""
Diagnostics for GPU memory managed by RMM (RAPIDS memory manager).
"""

from __future__ import annotations

try:
    import rmm
except ImportError:
    rmm = None


def _get_pool_size(mr):
    # if the memory resource or any of its upstreams
    # is a `PoolMemoryResource`, get its pool size
    if not isinstance(mr, rmm.mr.PoolMemoryResource):
        if hasattr(mr, "upstream_mr"):
            return _get_pool_size(mr.upstream_mr)
        else:
            return 0
    else:
        pool_size = mr.pool_size()
        return pool_size


def _get_allocated_bytes(mr):
    if not hasattr(mr, "get_allocated_bytes"):
        if hasattr(mr, "upstream_mr"):
            return _get_allocated_bytes(mr.upstream_mr)
        else:
            return 0
    else:
        return mr.get_allocated_bytes()


def real_time():
    if rmm is None:
        return {"rmm-used": None, "rmm-total": None}
    mr = rmm.mr.get_current_device_resource()
    rmm_pool_size = _get_pool_size(mr)
    rmm_used = _get_allocated_bytes(mr)
    rmm_total = max(rmm_pool_size, rmm_used)
    return {"rmm-used": rmm_used, "rmm-total": rmm_total}

"""
Dispatch in dask.array.

Also see backends.py
"""

from dask import config

from ..utils import BackendDispatch, Dispatch

concatenate_lookup = Dispatch("concatenate")
tensordot_lookup = Dispatch("tensordot")
einsum_lookup = Dispatch("einsum")
empty_lookup = Dispatch("empty")
divide_lookup = Dispatch("divide")
percentile_lookup = Dispatch("percentile")


class ArrayBackendDispatch(BackendDispatch):
    def get_backend(self):
        return config.get("array.backend") or "numpy"

    @property
    def allow_fallback(self):
        return config.get("array.backend-options.allow-fallback", True)

    @property
    def warn_fallback(self):
        return config.get("array.backend-options.warn-fallback", True)


array_backend_dispatch = ArrayBackendDispatch("array_backend_dispatch")

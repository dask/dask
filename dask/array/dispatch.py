"""
Dispatch in dask.array.

Also see backends.py
"""

from dask import config
from dask.backends import BackendDispatch
from dask.utils import Dispatch

concatenate_lookup = Dispatch("concatenate")
tensordot_lookup = Dispatch("tensordot")
einsum_lookup = Dispatch("einsum")
empty_lookup = Dispatch("empty")
divide_lookup = Dispatch("divide")
percentile_lookup = Dispatch("percentile")
numel_lookup = Dispatch("numel")
nannumel_lookup = Dispatch("nannumel")


class ArrayBackendDispatch(BackendDispatch):
    def get_backend(self):
        return config.get("array.backend.library") or "numpy"

    @property
    def allow_fallback(self):
        return config.get("array.backend.allow-fallback")

    @property
    def warn_fallback(self):
        return config.get("array.backend.warn-fallback")


array_backend_dispatch = ArrayBackendDispatch("array_backend_dispatch")

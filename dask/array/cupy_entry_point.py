import dask.array as da
from dask import config
from dask.array.backends import (
    ArrayBackendEntrypoint,
    array_creation_dispatch,
    register_cupy,
)

try:
    import cupy

    class CupyBackendEntrypoint(ArrayBackendEntrypoint):
        def __init__(self):
            """Register data-directed dispatch functions"""
            register_cupy()

        @property
        def RandomState(self):
            return cupy.random.RandomState

        @staticmethod
        def ones(*args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            with config.set({"array.backend": "numpy"}):
                return da.ones(*args, meta=meta, **kwargs)

        @staticmethod
        def zeros(*args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            with config.set({"array.backend": "numpy"}):
                return da.zeros(*args, meta=meta, **kwargs)

        @staticmethod
        def empty(*args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            with config.set({"array.backend": "numpy"}):
                return da.empty(*args, meta=meta, **kwargs)

        @staticmethod
        def full(*args, meta=None, **kwargs):
            meta = cupy.empty(()) if meta is None else meta
            with config.set({"array.backend": "numpy"}):
                return da.full(*args, meta=meta, **kwargs)

        @staticmethod
        def arange(*args, like=None, **kwargs):
            like = cupy.empty(()) if like is None else like
            with config.set({"array.backend": "numpy"}):
                return da.arange(*args, like=like, **kwargs)

    array_creation_dispatch.register_backend("cupy", CupyBackendEntrypoint())

except ImportError:
    raise ImportError("Please install `cupy` to utilize `CupyBackendEntrypoint`.")

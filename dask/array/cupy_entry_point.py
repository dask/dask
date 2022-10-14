import dask.array as da
from dask import config
from dask.array.backends import ArrayBackendEntrypoint, register_cupy


def _cupy(strict=True):
    try:
        import cupy
    except ImportError:
        if strict:
            raise ImportError("Please install `cupy` to use `CupyBackendEntrypoint`")
        return None
    return cupy


class CupyBackendEntrypoint(ArrayBackendEntrypoint):
    def __init__(self):
        """Register data-directed dispatch functions"""
        if _cupy(strict=False):
            register_cupy()

    @property
    def RandomState(self):
        return _cupy().random.RandomState

    @staticmethod
    def ones(*args, meta=None, **kwargs):
        meta = _cupy().empty(()) if meta is None else meta
        with config.set({"array.backend": "numpy"}):
            return da.ones(*args, meta=meta, **kwargs)

    @staticmethod
    def zeros(*args, meta=None, **kwargs):
        meta = _cupy().empty(()) if meta is None else meta
        with config.set({"array.backend": "numpy"}):
            return da.zeros(*args, meta=meta, **kwargs)

    @staticmethod
    def empty(*args, meta=None, **kwargs):
        meta = _cupy().empty(()) if meta is None else meta
        with config.set({"array.backend": "numpy"}):
            return da.empty(*args, meta=meta, **kwargs)

    @staticmethod
    def full(*args, meta=None, **kwargs):
        meta = _cupy().empty(()) if meta is None else meta
        with config.set({"array.backend": "numpy"}):
            return da.full(*args, meta=meta, **kwargs)

    @staticmethod
    def arange(*args, like=None, **kwargs):
        like = _cupy().empty(()) if like is None else like
        with config.set({"array.backend": "numpy"}):
            return da.arange(*args, like=like, **kwargs)

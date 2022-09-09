from __future__ import annotations

import warnings
from functools import lru_cache

from dask.compatibility import entry_points


class DaskBackendEntrypoint:
    """Base Collection-Backend Entrypoint Class"""

    @property
    def fallback(self) -> DaskBackendEntrypoint | None:
        """Fallback entrypoint object.

        Returning anything other than ``None`` requires that
        ``move_from_fallback`` be properly defined.
        """
        return None

    def move_from_fallback(self, x):
        """Move a Dask collection from the fallback backend"""
        raise NotImplementedError

    def _get_fallback_attr(self, attr):
        """Return the fallback version of the specified attribute"""
        if self.fallback is None:
            raise ValueError(f"Fallback is not supported for {self}")

        def wrapper(*args, **kwargs):
            return self.move_from_fallback(
                getattr(self.fallback, attr)(*args, **kwargs)
            )

        return wrapper


@lru_cache(maxsize=1)
def detect_entrypoints():
    entrypoints = entry_points("dask.backends")
    return {ep.name: ep for ep in entrypoints}


class BackendDispatch:
    """Simple backend dispatch for collection functions"""

    def __init__(self, name=None):
        self._lookup = {}
        if name:
            self.__name__ = name

    def register(self, backend: str, cls_target: DaskBackendEntrypoint):
        """Register a target class for a specific backend label"""

        def wrapper(cls_target):
            if isinstance(backend, tuple):
                for b in backend:
                    self.register(b, cls_target)
            elif isinstance(cls_target, DaskBackendEntrypoint):
                self._lookup[backend] = cls_target
            else:
                raise ValueError(
                    f"BackendDispatch only supports DaskBackendEntrypoint "
                    f"registration. Got {cls_target}"
                )
            return cls_target

        return wrapper(cls_target)

    def dispatch(self, backend):
        """Return the function implementation for the given backend"""
        try:
            impl = self._lookup[backend]
        except KeyError:
            # Check entrypoints for the specified backend
            entrypoints = detect_entrypoints()
            if backend in entrypoints:
                self.register(backend, entrypoints[backend].load()())
                return self._lookup[backend]
        else:
            return impl
        raise ValueError(f"No backend dispatch registered for {backend}")

    def get_backend(self):
        """Return the desired collection backend"""
        raise NotImplementedError

    @property
    def allow_fallback(self):
        """Check if using the backend fallback is allowed"""
        raise NotImplementedError

    @property
    def warn_fallback(self):
        """Check if backend-fallback usage should raise a warning"""
        raise NotImplementedError

    def __getattr__(self, item):
        """
        Return the appropriate attribute for the current backend
        """
        backend = self.dispatch(self.get_backend())
        try:
            return getattr(backend, item)
        except AttributeError as err:

            if not backend.fallback or not self.allow_fallback:
                raise err

            if self.warn_fallback:
                warnings.warn(
                    f"Falling back to {backend.fallback} for {item}. "
                    f"Expect data-movement overhead!"
                )

            return backend._get_fallback_attr(item)

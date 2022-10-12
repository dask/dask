from __future__ import annotations

from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Callable, TypeVar

from dask import config
from dask.compatibility import entry_points


class DaskBackendEntrypoint(ABC):
    """Base Collection-Backend Entrypoint Class

    Most methods in this class correspond to collection-creation
    for a specific library backend. Once a collection is created,
    the existing data will be used to dispatch compute operations
    within individual tasks. The backend is responsible for
    ensuring that these data-directed dispatch functions are
    registered when ``__init__`` is called.
    """

    @abstractmethod
    def __init__(self):
        """Register data-directed dispatch functions

        This may be a ``pass`` operation if the data dispatch functions
        are already registered within the same module that the
        ``DaskBackendEntrypoint`` subclass is defined.
        """
        raise NotImplementedError


@lru_cache(maxsize=1)
def detect_entrypoints():
    entrypoints = entry_points("dask.backends")
    return {ep.name: ep for ep in entrypoints}


BackendEntrypointType = TypeVar(
    "BackendEntrypointType",
    bound="DaskBackendEntrypoint",
)


class CreationDispatch:
    """Simple backend dispatch for collection-creation functions"""

    def __init__(self, config_field, default, name=None):
        self._lookup = {}
        self.config_field = config_field
        self.default = default
        if name:
            self.__name__ = name

    def register_backend(
        self, name: str, backend: BackendEntrypointType
    ) -> BackendEntrypointType:
        """Register a target class for a specific backend label"""
        raise NotImplementedError

    def dispatch(self, backend):
        """Return the desired backend entrypoint"""
        try:
            impl = self._lookup[backend]
        except KeyError:
            # Check entrypoints for the specified backend
            entrypoints = detect_entrypoints()
            if backend in entrypoints:
                return self.register_backend(backend, entrypoints[backend].load()())
        else:
            return impl
        raise ValueError(f"No backend dispatch registered for {backend}")

    def get_backend(self):
        """Return the desired collection backend"""
        if self.config_field:
            return config.get(self.config_field) or self.default
        return self.default

    def register_inplace(self, backend=None, func_name=None, function=None) -> Callable:
        """Register dispatchable function"""
        if function is not None:
            function.__name__ = func_name or function.__name__

        def inner(function):
            func_name = function.__name__
            if backend:
                self.dispatch(backend).__setattr__(func_name, function)

            def _func(*args, **kwargs):
                return getattr(self, func_name)(*args, **kwargs)

            _func.__doc__ = function.__doc__
            return _func

        return inner(function) if function is not None else inner

    def __getattr__(self, item):
        """
        Return the appropriate attribute for the current backend
        """
        backend = self.dispatch(self.get_backend())
        return getattr(backend, item)

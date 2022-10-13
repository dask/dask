from __future__ import annotations

from abc import ABC, abstractmethod
from functools import lru_cache, wraps
from typing import Any, Callable, TypeVar, cast

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
def detect_entrypoints(entry_point_name):
    entrypoints = entry_points(entry_point_name)
    return {ep.name: ep for ep in entrypoints}


BackendEntrypointType = TypeVar(
    "BackendEntrypointType",
    bound="DaskBackendEntrypoint",
)
BackendFuncType = TypeVar("BackendFuncType", bound=Callable[..., Any])


class CreationDispatch:
    """Simple backend dispatch for collection-creation functions"""

    _lookup: dict
    _module_name: str
    _config_field: str
    default: str

    def __init__(self, module_name: str, default: str, name: str | None = None):
        self._lookup = {}
        self._module_name = module_name
        self._config_field = f"{module_name}.backend"
        self.default = default
        if name:
            self.__name__ = name

    def register_backend(
        self, name: str, backend: BackendEntrypointType
    ) -> BackendEntrypointType:
        """Register a target class for a specific backend label"""
        raise NotImplementedError

    def dispatch(self, backend: str):
        """Return the desired backend entrypoint"""
        try:
            impl = self._lookup[backend]
        except KeyError:
            # Check entrypoints for the specified backend
            entrypoints = detect_entrypoints(f"dask.{self._module_name}.backends")
            if backend in entrypoints:
                return self.register_backend(backend, entrypoints[backend].load()())
        else:
            return impl
        raise ValueError(f"No backend dispatch registered for {backend}")

    @property
    def backend(self) -> str:
        """Return the desired collection backend"""
        return config.get(self._config_field, self.default) or self.default

    @backend.setter
    def backend(self, value: str):
        raise RuntimeError(
            f"Set the backend by configuring the {self._config_field} option"
        )

    def register_inplace(
        self,
        backend: str,
        name: str | None = None,
    ) -> Callable:
        """Register dispatchable function"""

        def decorator(fn: BackendFuncType) -> BackendFuncType:
            dispatch_name = name or fn.__name__
            dispatcher = self.dispatch(backend)
            dispatcher.__setattr__(dispatch_name, fn)

            @wraps(fn)
            def wrapper(*args, **kwargs):
                return getattr(self, dispatch_name)(*args, **kwargs)

            wrapper.__name__ = dispatch_name
            return cast(BackendFuncType, wrapper)

        return decorator

    def __getattr__(self, item: str):
        """
        Return the appropriate attribute for the current backend
        """
        backend = self.dispatch(self.backend)
        return getattr(backend, item)

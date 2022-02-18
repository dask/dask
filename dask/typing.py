from __future__ import annotations

from typing import Any, Callable, Mapping, MutableMapping, Protocol, runtime_checkable

try:
    from IPython.display import DisplayObject
except ImportError:
    DisplayObject = object


@runtime_checkable
class DaskCollection(Protocol):
    def __dask_graph__(self) -> Mapping | None:
        pass

    def __dask_keys__(self) -> list[Hashable]:
        pass

    def __dask_postcompute__(self) -> tuple[Callable, tuple]:
        pass

    def __dask_postpersist__(self) -> tuple[Callable, tuple]:
        pass

    def __dask_tokenize__(self) -> Any:
        pass

    @staticmethod
    def __dask_optimize__(dsk: MutableMapping, keys: Any, **kwargs) -> MutableMapping:
        pass

    @staticmethod
    def __dask_scheduler__(dsk: Mapping, keys: Any, **kwargs) -> Any:
        pass

    def compute(self, **kwargs) -> Any:
        pass

    def persist(self, **kwargs) -> Any:
        pass

    def visualize(self, **kwargs) -> DisplayObject:
        pass

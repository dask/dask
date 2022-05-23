from __future__ import annotations

from typing import Any, Hashable

from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.operation import CollectionOperation


class FrameOperation(CollectionOperation):
    """Abtract DataFrame-based CollectionOperation"""

    def __init__(
        self,
        name: str,
        meta: Any,
        divisions: tuple | None,
    ):
        self._name = name
        self._meta = meta
        self._divisions = tuple(divisions) if divisions is not None else None

    @property
    def name(self) -> str:
        """Return the unique name for this CollectionOperation"""
        return self._name

    @property
    def meta(self) -> Any:
        """Return DataFrame metadata"""
        return self._meta

    @property
    def divisions(self) -> tuple | None:
        """Return DataFrame divisions"""
        return self._divisions

    @property
    def npartitions(self) -> int | None:
        """Return partition count"""
        if not self.divisions:
            return None
        return len(self.divisions) - 1

    @property
    def collection_keys(self) -> list[Hashable]:
        """Return list of all collection keys"""
        if self.npartitions is None:
            raise ValueError
        return [(self.name, i) for i in range(self.npartitions)]

    def __hash__(self):
        """Hash a FrameOperation using its name, meta and divisions"""
        return hash(tokenize(self.name, self.meta, self.divisions))


class CompatFrameOperation(FrameOperation):
    """Pass-through FrameOperation

    This class acts as a container for the name, meta,
    divisions, and graph (HLG) of a "legacy" collection.
    Note that a ``CompatFrameOperation`` may not have any
    dependencies.
    """

    def __init__(
        self,
        dsk: dict | HighLevelGraph,
        name: str,
        meta: Any = None,
        divisions: tuple | None = None,
        parent_meta: Any = None,
    ):
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[])
        if meta is None:
            raise ValueError("meta cannot be None")
        super().__init__(name, meta, divisions)
        self._dask = dsk
        self._parent_meta = parent_meta

    def copy(self) -> CompatFrameOperation:
        return CompatFrameOperation(
            self.dask,
            self.name,
            meta=self.meta.copy(),
            divisions=self.divisions,
            parent_meta=self._parent_meta,
        )

    @property
    def dask(self) -> HighLevelGraph:
        if not isinstance(self._dask, HighLevelGraph):
            raise TypeError
        return self._dask

    @property
    def dependencies(self) -> set[CollectionOperation]:
        return set()

    def reinitialize(self, new_dependencies: dict, **new_kwargs):
        if new_dependencies:
            raise ValueError("CompatFrameOperation cannot have dependencies")
        kwargs = {
            "meta": self.meta,
            "divisions": self._divisions,
            "parent_meta": self._parent_meta,
        }
        kwargs.update(new_kwargs)
        return CompatFrameOperation(self._dask, self.name, **kwargs)

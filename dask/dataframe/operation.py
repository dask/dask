from __future__ import annotations

from dataclasses import dataclass, replace
from functools import cached_property
from typing import Any, Hashable, Tuple

from dask.highlevelgraph import HighLevelGraph
from dask.operation import CollectionOperation


@dataclass(frozen=True)
class FrameOperation(CollectionOperation[Tuple[str, int]]):
    """Abtract DataFrame-based CollectionOperation"""

    @property
    def meta(self) -> Any:
        """Return DataFrame metadata"""
        raise NotImplementedError

    def replace_meta(self, value) -> CollectionOperation[tuple[str, int]]:
        """Return a new operation with different meta"""
        raise ValueError(f"meta cannot be modified for {type(self)}")

    @property
    def divisions(self) -> tuple | None:
        """Return DataFrame divisions"""
        raise NotImplementedError

    def replace_divisions(self, value) -> CollectionOperation[tuple[str, int]]:
        """Return a new operation with different divisions"""
        raise ValueError(f"divisions cannot be modified for {type(self)}")

    @property
    def npartitions(self) -> int | None:
        """Return partition count"""
        if not self.divisions:
            return None
        return len(self.divisions) - 1

    @property
    def collection_keys(self) -> list[tuple[str, int]]:
        """Return list of all collection keys"""
        if self.npartitions is None:
            raise ValueError
        return [(self.name, i) for i in range(self.npartitions)]

    @property
    def dask(self) -> HighLevelGraph:
        """Return a HighLevelGraph representation of this operation

        This property provides temporary compatibility, and may
        be removed in the future.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class CompatFrameOperation(FrameOperation):
    """Pass-through FrameOperation

    This class acts as a container for the name, meta,
    divisions, and graph (HLG) of a "legacy" collection.
    Note that a ``CompatFrameOperation`` may not have any
    dependencies.
    """

    _dsk: dict | HighLevelGraph
    _name: str
    _meta: Any
    _divisions: tuple | None
    parent_meta: Any | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def meta(self) -> Any:
        return self._meta

    def replace_meta(self, value) -> CompatFrameOperation:
        return replace(self, _meta=value)

    @property
    def divisions(self) -> tuple | None:
        return tuple(self._divisions) if self._divisions is not None else None

    def replace_divisions(self, value) -> CompatFrameOperation:
        return replace(self, _divisions=value)

    @property
    def dependencies(self) -> frozenset[CollectionOperation[Any]]:
        return frozenset()

    @cached_property
    def dask(self) -> HighLevelGraph:
        return (
            HighLevelGraph.from_collections(self._name, self._dsk, dependencies=[])
            if not isinstance(self._dsk, HighLevelGraph)
            else self._dsk
        )

    def reinitialize(self, replace_dependencies, **changes) -> CompatFrameOperation:
        if replace_dependencies:
            raise ValueError(
                "CompatFrameOperation does not support replace_dependencies"
            )
        return replace(self, **changes)

    def subgraph(
        self, keys: list[tuple]
    ) -> tuple[
        dict[tuple[str, int], Any],
        dict[CollectionOperation[Any], list[tuple[Hashable]]],
    ]:
        # TODO: Maybe add optional HLG optimization pass?
        return self.dask.cull(keys).to_dict(), {}

    def copy(self) -> CompatFrameOperation:
        return replace(self, _meta=self.meta.copy())

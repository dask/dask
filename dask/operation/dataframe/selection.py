from __future__ import annotations

import operator
from dataclasses import dataclass, replace
from functools import cached_property
from typing import Any

from dask.base import tokenize
from dask.dataframe.core import _extract_meta
from dask.operation.core import _CollectionOperation
from dask.operation.dataframe.core import _PartitionwiseOperation

no_default = "__no_default__"


@dataclass(frozen=True)
class ColumnSelection(_PartitionwiseOperation):

    source: Any
    key: str
    _meta: Any = no_default
    _divisions: tuple | None = None

    @cached_property
    def name(self) -> str:
        token = tokenize(self.func, self.args, self.kwargs)
        return f"getitem-columns-{token}"

    @property
    def func(self):
        return operator.getitem

    @property
    def args(self):
        return [self.source, self.key]

    @property
    def kwargs(self):
        return {}

    @cached_property
    def default_meta(self) -> Any:
        return self.source.meta[_extract_meta(self.key)]

    @property
    def default_divisions(self) -> tuple:
        return self.source.divisions

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> ColumnSelection:
        args = [
            replace_dependencies[arg.name]
            if isinstance(arg, _CollectionOperation)
            else arg
            for arg in self.args
        ]
        _changes = {"source": args[0], **changes}
        return replace(self, **_changes)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class SeriesSelection(_PartitionwiseOperation):

    source: Any
    key: str
    _meta: Any = no_default
    _divisions: tuple | None = None

    @cached_property
    def name(self) -> str:
        token = tokenize(self.func, self.args, self.kwargs)
        return f"getitem-series-{token}"

    @property
    def func(self):
        return operator.getitem

    @property
    def args(self):
        return [self.source, self.key]

    @property
    def kwargs(self):
        return {}

    @cached_property
    def default_meta(self) -> Any:
        return self.source.meta

    @property
    def default_divisions(self) -> tuple:
        return self.source.divisions

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> SeriesSelection:
        args = [
            replace_dependencies[arg.name]
            if isinstance(arg, _CollectionOperation)
            else arg
            for arg in self.args
        ]
        _changes = {"source": args[0], "key": args[1], **changes}
        return replace(self, **_changes)

    def __hash__(self):
        return hash(self.name)

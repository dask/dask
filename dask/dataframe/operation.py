from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dask.base import tokenize
from dask.dataframe.utils import make_meta
from dask.highlevelgraph import HighLevelGraph
from dask.utils import apply


class CollectionOperation:

    # TODO: Move this out of dataframe module
    _dask: HighLevelGraph | None = None

    @property
    def dask(self) -> HighLevelGraph | None:
        if self._dask is None:
            self._dask = HighLevelGraph.from_collections(
                self.name,
                self.generate_graph(self.collection_keys),
                dependencies=[],
            )
        return self._dask

    def generate_graph(self, keys: list[tuple]) -> dict:
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @name.setter
    def name(self, value):
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[tuple]:
        raise NotImplementedError

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        raise NotImplementedError


class DataFrameOperation(CollectionOperation):

    _name: str
    _meta: Any
    _divisions: tuple

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def meta(self):
        return self._meta

    @meta.setter
    def meta(self, value):
        self._meta = value

    @property
    def divisions(self) -> tuple:
        return self._divisions

    @divisions.setter
    def divisions(self, value):
        self._divisions = value

    @property
    def npartitions(self) -> int:
        return len(self.divisions) - 1

    @property
    def collection_keys(self) -> list[tuple]:
        return [(self.name, i) for i in range(self.npartitions)]


class CompatFrameOperation(DataFrameOperation):
    def __init__(self, dsk, name, meta, divisions, parent_meta=None):
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[])
        self._dask = dsk
        self._name = name
        self._parent_meta = parent_meta
        self._meta = make_meta(meta, parent_meta=self._parent_meta)
        self._divisions = tuple(divisions) if divisions is not None else None

    @property
    def dask(self) -> HighLevelGraph | None:
        return self._dask

    def generate_graph(self, keys: list[tuple]) -> dict:
        return dict(self.dask)

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        return {}


class DataFrameCreation(DataFrameOperation):
    def __init__(
        self,
        io_func,
        meta,
        inputs,
        columns=None,
        divisions=None,
        label=None,
        token=None,
        creation_info=None,
    ):
        label = label or "create-frame"
        token = token or tokenize(
            io_func, meta, inputs, columns, divisions, creation_info
        )
        self._name = f"{label}-{token}"
        self.io_func = io_func
        self._meta = meta
        self.inputs = inputs
        self.columns = columns
        divisions = divisions or (None,) * (len(inputs) + 1)
        self._divisions = tuple(divisions)
        self.creation_info = creation_info

    def generate_graph(self, keys: list[tuple]) -> dict:
        dsk = {}
        for key in keys:
            name, index = key
            assert name == self.name
            dsk[key] = (self.io_func, self.inputs[index])
        return dsk

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        return {}


class DataFrameMapOperation(DataFrameOperation):
    def __init__(
        self,
        func,
        meta,
        *args,
        divisions=None,
        label=None,
        token=None,
        creation_info=None,
        **kwargs,
    ):
        label = label or "map-partitions"
        token = token or tokenize(func, meta, args, divisions, creation_info)
        self._name = f"{label}-{token}"
        self.func = func
        self._meta = meta
        assert len(args)
        self.args = args
        self._dependencies = {
            arg.name: arg for arg in self.args if isinstance(arg, CollectionOperation)
        }
        divisions = divisions or (None,) * (
            len(next(iter(self._dependencies.values()))) + 1
        )
        self._divisions = tuple(divisions)
        self.creation_info = creation_info
        self.kwargs = kwargs

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        return self._dependencies

    def generate_graph(self, keys: list[tuple]) -> dict:
        dsk = {}
        dependency_graph = {}
        for name, dep in self.dependencies.items():
            if isinstance(dep, (DataFrameMapOperation, DataFrameCreation)):
                dependency_graph.update(
                    dep.generate_graph([(name, key[1]) for key in keys])
                )
            else:
                # Not fusing these dependencies, so include them in dsk
                dsk.update(dep.generate_graph([(name, key[1]) for key in keys]))

        for key in keys:
            name, index = key
            assert name == self.name

            task = [self.func]
            for arg in self.args:
                if isinstance(arg, CollectionOperation):
                    dep_key = (arg.name, index)
                    task.append(dependency_graph.pop(dep_key, dep_key))
                else:
                    task.append(arg)

            if self.kwargs:
                dsk[key] = (
                    apply,
                    task[0],
                    task[1:],
                    self.kwargs,
                )
            else:
                dsk[key] = tuple(task)

        return dsk

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

import numpy as np

from dask.base import tokenize
from dask.dataframe.utils import make_meta
from dask.highlevelgraph import HighLevelGraph
from dask.utils import apply


class CollectionOperation:

    # TODO: Move this out of dataframe module
    _dask: HighLevelGraph | None = None

    @property
    def dask(self) -> HighLevelGraph | None:
        # TODO: We can wrap the operator in a new Layer type
        # to avoid materialization here once the HLG/Layer
        # serialization moves to Pickle (otherwise it will
        # be too much of a headache to wrap a general
        # CollectionOperation in an HLG Layer)
        if self._dask is None:
            self._dask = HighLevelGraph.from_collections(
                self.name,
                self.generate_graph(self.collection_keys),
                dependencies=[],
            )
        return self._dask

    @classmethod
    def _find_deps(cls, op, op_tree, all_ops):
        all_ops[op.name] = op
        for dep_name, dep in op.dependencies.items():
            op_tree[op.name].add(dep_name)
            cls._find_deps(dep, op_tree, all_ops)
        if not op.dependencies:
            op_tree[op.name] |= set()

    @property
    def operation_tree(self):
        op_tree = defaultdict(set)
        all_ops = {}
        self._find_deps(self, op_tree, all_ops)
        return op_tree, all_ops

    def visualize(self, filename="dask-operation.svg", format=None, **kwargs):
        from dask.dot import graphviz_to_file

        g = to_graphviz(self, **kwargs)
        graphviz_to_file(g, filename, format)
        return g

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

    def regenerate(self, new_dependencies: dict, new_kwargs: dict, cache: dict):
        """Regenerate ``self``"""
        raise NotImplementedError

    def regenerate_recursive(self, new_kwargs: dict = None, cache: dict = None):
        """Regenerate this CollectionOperation object (and any
        CollectionOperation dependencies) recursively.
        """
        new_kwargs = new_kwargs or {}
        cache = cache or {}

        if self.name in cache:
            return cache[self.name]

        _new_dependencies = {}
        for dep_name, dep in self.dependencies.items():
            if dep_name not in cache:
                cache[dep_name] = dep.regenerate_recursive(new_kwargs, cache)
            _new_dependencies[dep_name] = cache[dep_name]

        return self.regenerate(_new_dependencies, new_kwargs, cache)


class DataFrameOperation(CollectionOperation):

    _name: str
    _meta: Any
    _divisions: tuple
    _columns: set | None = None
    projected_columns: set | None = None

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

    @property
    def columns(self):
        return self._columns

    @classmethod
    def _required_columns(cls, op, columns=None):

        # First check if this is a creation operation
        creation_name = None
        if isinstance(op, DataFrameCreation):
            return columns, op.name

        if isinstance(op, DataFrameColumnSelection):
            # This IS a column selection, so we can
            # reset the current column projection
            columns = op.columns
        elif columns is None or op.columns is None:
            # Required columns unknown, cannot project
            columns = None
        else:
            # Required clumns known - Use set union
            columns |= op.columns

        # Discover required columns in dependencies
        for dep_name, dep in op.dependencies.items():
            columns, creation_name = cls._required_columns(dep, columns)

        return columns, creation_name

    def project_columns(self):

        _, all_ops = self.operation_tree
        creation_ops = {
            op_name
            for op_name, op in all_ops.items()
            if isinstance(op, DataFrameCreation)
        }
        if not creation_ops:
            return self

        columns, creation = self._required_columns(self)
        new_kwargs = {creation: {"columns": columns}}
        new = self.regenerate_recursive(new_kwargs)
        new.projected_columns = columns
        return new

    def optimize(self):
        new_operation = self
        new_operation = new_operation.project_columns()
        # TODO: Add other optimizations (e.g. predicate pushdown)
        return new_operation


class CompatFrameOperation(DataFrameOperation):
    """Pass-through DataFrameOperation

    This class simply acts as a container for a "legacy"
    collections name, meta, divisions, and graph (HLG).
    """

    def __init__(self, dsk, name, meta, divisions, parent_meta=None):
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[])
        self._dask = dsk
        self._name = name
        self._parent_meta = parent_meta
        self._meta = make_meta(meta, parent_meta=self._parent_meta)
        self._divisions = tuple(divisions) if divisions is not None else None

    def regenerate(self, new_dependencies: dict, new_kwargs: dict, cache: dict):
        if self.name not in cache:
            cache[self.name] = CompatFrameOperation(
                self._dask,
                self._name,
                self._meta,
                self._divisions,
                self._parent_meta,
            )
        return cache[self.name]

    @property
    def dask(self) -> HighLevelGraph | None:
        return self._dask

    def generate_graph(self, keys: list[tuple]) -> dict:
        if self.dask is None:
            raise ValueError("Graph is undefined")
        return self.dask.to_dict()

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
    ):
        from dask.dataframe.io.utils import DataFrameIOFunction

        if columns is not None and isinstance(io_func, DataFrameIOFunction):
            self.io_func = io_func.project_columns(columns)
            self._meta = meta[columns]
        else:
            self.io_func = io_func
            self._meta = meta
        self._columns = columns
        self.label = label or "create-frame"
        token = token or tokenize(self.io_func, meta, inputs, columns, divisions)
        self._name = f"{self.label}-{token}"
        self.inputs = inputs
        divisions = divisions or (None,) * (len(inputs) + 1)
        self._divisions = tuple(divisions)

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

    def regenerate(self, new_dependencies: dict, new_kwargs: dict, cache: dict):
        if self.name not in cache:
            kwargs = {
                "columns": self.columns,
                "divisions": self.divisions,
                "label": self.label,
            }
            kwargs.update(new_kwargs.get(self.name, {}))
            cache[self.name] = type(self)(
                self.io_func,
                self.meta,
                self.inputs,
                **kwargs,
            )
        return cache[self.name]


class DataFrameMapOperation(DataFrameOperation):
    def __init__(
        self,
        func,
        meta,
        *args,
        divisions=None,
        label=None,
        token=None,
        columns=None,
        **kwargs,
    ):
        self.label = label or "map-partitions"
        token = token or tokenize(func, meta, args, divisions)
        self._name = f"{self.label}-{token}"
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
        self._columns = columns
        self.kwargs = kwargs

    def regenerate(self, new_dependencies: dict, new_kwargs: dict, cache: dict):
        if self.name not in cache:
            kwargs = {
                "divisions": self.divisions,
                "label": self.label,
                "columns": self.columns,
                **self.kwargs,
            }
            kwargs.update(new_kwargs.get(self.name, {}))
            args = [
                new_dependencies[arg.name]
                if isinstance(arg, CollectionOperation)
                else arg
                for arg in self.args
            ]
            cache[self.name] = type(self)(
                self.func,
                self.meta,
                *args,
                **kwargs,
            )
        return cache[self.name]

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        return self._dependencies

    def generate_graph(self, keys: list[tuple]) -> dict:
        dsk = {}

        # Start by populating `dsk` with the graph needed for
        # dependencies (recursively). If the dependency is
        # "fusable", we add it to a seperate `fusable_graph`
        # dictionary.
        fusable_graph = {}
        for name, dep in self.dependencies.items():
            if isinstance(dep, (DataFrameMapOperation, DataFrameCreation)):
                # These dependencies are "fusable"
                fusable_graph.update(
                    dep.generate_graph([(name, key[1]) for key in keys])
                )
            else:
                # Not fusing these dependencies, so include them in dsk
                dsk.update(dep.generate_graph([(name, key[1]) for key in keys]))

        # Now we just need to update the graph with the
        # current DataFrameMapOperation tasks. If any dependency
        # keys are in `fusable_graph`, we use the corresponding
        # element from `fusable_graph` (rather than the task key).
        for key in keys:
            name, index = key
            assert name == self.name

            task = [self.func]
            for arg in self.args:
                if isinstance(arg, CollectionOperation):
                    dep_key = (arg.name, index)
                    task.append(fusable_graph.pop(dep_key, dep_key))
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

        dsk.update(fusable_graph)
        return dsk


class DataFrameColumnSelection(DataFrameMapOperation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        key = self.args[1]
        if np.isscalar(key) or isinstance(key, (tuple, str)):
            self._columns = {key}
        else:
            self._columns = set(key)


class DataFrameSeriesSelection(DataFrameMapOperation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._columns = set()


class DataFrameElementwise(DataFrameMapOperation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._columns = set()


def to_graphviz(
    operation,
    data_attributes=None,
    function_attributes=None,
    rankdir="BT",
    graph_attr=None,
    node_attr=None,
    edge_attr=None,
    **kwargs,
):
    from dask.dot import graphviz, label, name

    data_attributes = data_attributes or {}
    function_attributes = function_attributes or {}
    graph_attr = graph_attr or {}
    node_attr = node_attr or {}
    edge_attr = edge_attr or {}

    graph_attr["rankdir"] = rankdir
    node_attr["shape"] = "box"
    node_attr["fontname"] = "helvetica"

    graph_attr.update(kwargs)
    g = graphviz.Digraph(
        graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr
    )

    op_tree, all_ops = operation.operation_tree

    n_tasks = {}
    for op_name in op_tree:
        n_tasks[op_name] = all_ops[op_name].npartitions

    min_tasks = min(n_tasks.values())
    max_tasks = max(n_tasks.values())

    cache = {}

    for op in op_tree:
        op_name = name(op)
        attrs = data_attributes.get(op, {})

        node_label = label(op, cache=cache)
        node_size = (
            20
            if max_tasks == min_tasks
            else int(20 + ((n_tasks[op] - min_tasks) / (max_tasks - min_tasks)) * 20)
        )

        attrs.setdefault("label", str(node_label))
        attrs.setdefault("fontsize", str(node_size))

        g.node(op_name, **attrs)

    for op, deps in op_tree.items():
        op_name = name(op)
        for dep in deps:
            dep_name = name(dep)
            g.edge(dep_name, op_name)

    return g

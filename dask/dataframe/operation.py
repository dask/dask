from __future__ import annotations

from dataclasses import dataclass, replace
from functools import cached_property
from typing import Any, Callable, Tuple

from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.operation import (
    CollectionOperation,
    FusableOperation,
    FusedOperations,
    LiteralInputs,
    fuse_subgraph_callables,
    map_fusion,
)
from dask.optimization import SubgraphCallable
from dask.utils import apply

PartitionKey = Tuple[str, int]


class ScalarOperation(CollectionOperation[PartitionKey]):
    """Scalar-based CollectionOperation"""

    @property
    def meta(self) -> Any:
        """Return DataFrame metadata"""
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[PartitionKey]:
        """Return list of all collection keys"""
        return [(self.name, 0)]


@dataclass(frozen=True)
class CompatScalarOperation(ScalarOperation):
    """Pass-through ScalarOperation

    This class acts as a container for the name, meta,
    divisions, and graph (HLG) of a "legacy" collection.
    """

    _name: str
    _dsk: dict | HighLevelGraph
    _meta: Any
    divisions: tuple | None = None
    parent_meta: Any | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def meta(self) -> Any:
        return self._meta

    def replace_meta(self, value) -> CompatScalarOperation:
        return replace(self, _meta=value)

    @property
    def dependencies(self) -> frozenset[CollectionOperation]:
        return frozenset()

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> CompatScalarOperation:
        if replace_dependencies:
            raise ValueError(
                "CompatScalarOperation does not support replace_dependencies"
            )
        return replace(self, **changes)

    @cached_property
    def dask(self) -> HighLevelGraph:
        return (
            HighLevelGraph.from_collections(self.name, self._dsk, dependencies=[])
            if not isinstance(self._dsk, HighLevelGraph)
            else self._dsk
        )

    def subgraph(self, keys: list[PartitionKey]) -> tuple[dict, dict]:
        # TODO: Maybe add optional HLG optimization pass?
        return self.dask.cull(keys).to_dict(), {}

    def copy(self) -> CompatScalarOperation:
        return replace(self, _meta=self.meta.copy())

    def __hash__(self):
        return hash(tokenize(self.name, self.dask, self.meta, self.parent_meta))


class FrameOperation(CollectionOperation[PartitionKey]):
    """Abtract DataFrame-based CollectionOperation"""

    @property
    def meta(self) -> Any:
        """Return DataFrame metadata"""
        raise NotImplementedError

    def replace_meta(self, value) -> CollectionOperation[PartitionKey]:
        """Return a new operation with different meta"""
        raise ValueError(f"meta cannot be modified for {type(self)}")

    @property
    def divisions(self) -> tuple:
        """Return DataFrame divisions"""
        raise NotImplementedError

    def replace_divisions(self, value) -> CollectionOperation[PartitionKey]:
        """Return a new operation with different divisions"""
        raise ValueError(f"divisions cannot be modified for {type(self)}")

    @property
    def npartitions(self) -> int:
        """Return partition count"""
        return len(self.divisions) - 1

    @property
    def collection_keys(self) -> list[PartitionKey]:
        """Return list of all collection keys"""
        return [(self.name, i) for i in range(self.npartitions)]


@dataclass(frozen=True)
class CompatFrameOperation(FrameOperation):
    """Pass-through FrameOperation

    This class acts as a container for the name, meta,
    divisions, and graph (HLG) of a "legacy" collection.
    """

    _name: str
    _dsk: dict | HighLevelGraph
    _meta: Any
    _divisions: tuple

    @property
    def name(self) -> str:
        return self._name

    @property
    def meta(self) -> Any:
        return self._meta

    def replace_meta(self, value) -> CompatFrameOperation:
        return replace(self, _meta=value)

    @property
    def divisions(self) -> tuple:
        return self._divisions

    def replace_divisions(self, value) -> CompatFrameOperation:
        return replace(self, _divisions=value)

    @property
    def dependencies(self) -> frozenset[CollectionOperation]:
        return frozenset()

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> CompatFrameOperation:
        if replace_dependencies:
            raise ValueError(
                "CompatFrameOperation does not support replace_dependencies"
            )
        return replace(self, **changes)

    @cached_property
    def dask(self) -> HighLevelGraph:
        return (
            HighLevelGraph.from_collections(self.name, self._dsk, dependencies=[])
            if not isinstance(self._dsk, HighLevelGraph)
            else self._dsk
        )

    def subgraph(self, keys: list[PartitionKey]) -> tuple[dict, dict]:
        # TODO: Maybe add optional HLG optimization pass?
        return self.dask.cull(keys).to_dict(), {}

    def copy(self) -> CompatFrameOperation:
        return replace(self, _meta=self.meta.copy())

    def __hash__(self):
        return hash(tokenize(self.name, self.dask, self.meta, self.divisions))


@dataclass(frozen=True)
class FrameCreation(FusableOperation, FrameOperation):

    io_func: Callable
    inputs: list
    label: str
    _meta: Any
    _divisions: tuple

    @cached_property
    def name(self) -> str:
        token = tokenize(self.io_func, self._meta, self.inputs, self._divisions)
        return f"{self.label}-{token}"

    @cached_property
    def dependencies(self) -> frozenset[CollectionOperation]:
        _dep = LiteralInputs({(i,): val for i, val in enumerate(self.inputs)})
        return frozenset({_dep})

    @property
    def meta(self) -> Any:
        return self._meta

    @cached_property
    def divisions(self) -> tuple:
        return self._divisions

    @cached_property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[CollectionOperation]]:
        inkeys = [d.name for d in self.dependencies]
        subgraph = {self.name: (self.io_func, inkeys[-1])}
        return (
            SubgraphCallable(
                dsk=subgraph,
                outkey=self.name,
                inkeys=inkeys,
            ),
            self.dependencies,
        )

    def subgraph(self, keys: list[tuple]) -> tuple[dict, dict]:

        # Always fuse MapInput operations
        input_op = next(iter(self.dependencies))
        input_op_name = input_op.name
        input_op_keys = [(input_op_name,) + tuple(key[1:]) for key in keys]
        dep_subgraph, _ = input_op.subgraph(input_op_keys)

        # Build subgraph with LiteralInputs dependencies fused
        dsk = {}
        for key in keys:
            dep_key = (input_op_name,) + tuple(key[1:])
            dsk[key] = (self.io_func, dep_subgraph.get(dep_key, dep_key))
        return dsk, {}

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> FrameCreation:
        # TODO: Support column projection and predicate pushdown
        return replace(self, **changes)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class PartitionwiseOperation(FusableOperation, FrameOperation):

    func: Callable
    args: list[Any]
    label: str
    kwargs: dict
    _meta: Any
    _divisions: tuple

    @cached_property
    def name(self) -> str:
        token = tokenize(self.func, self._meta, self.args, self._divisions, self.kwargs)
        return f"{self.label}-{token}"

    @property
    def meta(self) -> Any:
        return self._meta

    def replace_meta(self, value) -> PartitionwiseOperation:
        return replace(self, _meta=value)

    @property
    def divisions(self) -> tuple:
        return self._divisions

    def replace_divisions(self, value) -> PartitionwiseOperation:
        return replace(self, _divisions=value)

    @cached_property
    def dependencies(self) -> frozenset[CollectionOperation]:
        return frozenset(
            arg for arg in self.args if isinstance(arg, CollectionOperation)
        )

    @cached_property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[CollectionOperation]]:
        task = [self.func]
        inkeys = []
        for arg in self.args:
            if isinstance(arg, CollectionOperation):
                inkeys.append(arg.name)
                task.append(inkeys[-1])
            else:
                task.append(arg)

        subgraph = {
            self.name: (
                apply,
                task[0],
                task[1:],
                self.kwargs,
            )
            if self.kwargs
            else tuple(task)
        }
        return (
            SubgraphCallable(
                dsk=subgraph,
                outkey=self.name,
                inkeys=inkeys,
            ),
            self.dependencies,
        )

    def subgraph(self, keys: list[tuple]) -> tuple[dict, dict]:

        # Check if we have MapInput dependencies to fuse
        dep_subgraphs = {}
        dep_keys = {}
        for dep in self.dependencies:
            dep_name = dep.name
            input_op_keys = [(dep_name,) + tuple(key[1:]) for key in keys]
            if isinstance(dep, LiteralInputs):
                dep_subgraphs.update(dep.subgraph(input_op_keys)[0])
            else:
                dep_keys[dep] = input_op_keys

        # Build subgraph with LiteralInputs dependencies fused
        dsk = {}
        for key in keys:
            task = [self.func]
            for arg in self.args:
                if isinstance(arg, CollectionOperation):
                    dep_key = (arg.name,) + tuple(key[1:])
                    task.append(dep_subgraphs.get(dep_key, dep_key))
                else:
                    task.append(arg)
            dsk[key] = tuple(task)
        return dsk, dep_keys

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> PartitionwiseOperation:
        args = [
            replace_dependencies[arg.name]
            if isinstance(arg, CollectionOperation)
            else arg
            for arg in self.args
        ]
        _changes = {"args": args, **changes}
        return replace(self, **_changes)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class FusedFrameOperations(FusedOperations, FrameOperation):

    func: SubgraphCallable
    inkey_mapping: dict[str, str]
    label: str
    _dependencies: frozenset
    _meta: Any
    _divisions: tuple

    @classmethod
    def from_operation(
        cls,
        operation: CollectionOperation,
        fusable: set | bool,
        label: str,
    ):
        # Check inputs
        if not isinstance(operation, PartitionwiseOperation):
            raise ValueError(
                f"FusedDataFrameOperation.from_operation only supports "
                f"PartitionwiseOperation. Got {type(operation)}"
            )
        elif not fusable:
            return operation

        # Build fused SubgraphCallable and extract dependencies
        _dependencies = set()
        inkey_mapping = {}
        _subgraph_callable, all_deps = fuse_subgraph_callables(operation, fusable)
        for dep in all_deps:
            key = dep.name
            _dependencies.add(dep)
            inkey_mapping[key] = key

        # Return new FusedOperations object
        return cls(
            _subgraph_callable,
            inkey_mapping,
            label,
            frozenset(_dependencies),
            operation.meta,
            operation.divisions,
        )

    @cached_property
    def name(self) -> str:
        token = tokenize(
            self.func,
            self.inkey_mapping,
            self._dependencies,
            self._meta,
            self._divisions,
        )
        return f"{self.label}-{token}"

    @property
    def meta(self) -> Any:
        return self._meta

    def replace_meta(self, value) -> FusedFrameOperations:
        return replace(self, _meta=value)

    @property
    def divisions(self) -> tuple:
        return self._divisions

    def replace_divisions(self, value) -> FusedFrameOperations:
        return replace(self, _divisions=value)

    @property
    def collection_keys(self) -> list[PartitionKey]:
        if self.npartitions is None:
            raise ValueError
        return [(self.name, i) for i in range(self.npartitions)]

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> FusedFrameOperations:
        # Update dependencies
        _dependencies = set()
        inkey_mapping = {}
        _dependency_dict = {d.name: d for d in self.dependencies}

        for inkey in self.func.inkeys:
            dep_name = self.inkey_mapping[inkey]
            dep = _dependency_dict[dep_name]
            _dep = replace_dependencies[dep.name]
            _dependencies.add(_dep)
            inkey_mapping[inkey] = _dep.name

        _changes = {
            "inkey_mapping": inkey_mapping,
            "_dependencies": _dependencies,
            **changes,
        }
        return replace(self, **_changes)

    def __hash__(self):
        return hash(self.name)


#
# Optimization
#


def optimize(
    operation,
    fuse_operations=True,
):
    if isinstance(operation, CompatFrameOperation):
        return operation
    new = operation

    # Apply map fusion
    if fuse_operations:
        new = map_fusion(new, FusedFrameOperations)

    # Return new operation
    return new

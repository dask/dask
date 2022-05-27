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
    def divisions(self) -> tuple | None:
        """Return DataFrame divisions"""
        raise NotImplementedError

    def replace_divisions(self, value) -> CollectionOperation[PartitionKey]:
        """Return a new operation with different divisions"""
        raise ValueError(f"divisions cannot be modified for {type(self)}")

    @property
    def npartitions(self) -> int | None:
        """Return partition count"""
        if not self.divisions:
            return None
        return len(self.divisions) - 1

    @property
    def collection_keys(self) -> list[PartitionKey]:
        """Return list of all collection keys"""
        if self.npartitions is None:
            raise ValueError
        return [(self.name, i) for i in range(self.npartitions)]


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
    def dependencies(self) -> frozenset[CollectionOperation]:
        return frozenset()

    @cached_property
    def dask(self) -> HighLevelGraph:
        return (
            HighLevelGraph.from_collections(self._name, self._dsk, dependencies=[])
            if not isinstance(self._dsk, HighLevelGraph)
            else self._dsk
        )

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> CompatFrameOperation:
        if replace_dependencies:
            raise ValueError(
                "CompatFrameOperation does not support replace_dependencies"
            )
        return replace(self, **changes)

    def subgraph(self, keys: list[PartitionKey]) -> tuple[dict, dict]:
        # TODO: Maybe add optional HLG optimization pass?
        return self.dask.cull(keys).to_dict(), {}

    def copy(self) -> CompatFrameOperation:
        return replace(self, _meta=self.meta.copy())

    def __hash__(self):
        return hash(tokenize(self.name, self.meta, self.divisions))


@dataclass(frozen=True)
class FrameCreation(FusableOperation, FrameOperation):

    _io_func: Callable
    _meta: Any
    _inputs: list
    _divisions: tuple
    _label: str

    @cached_property
    def name(self) -> str:
        token = tokenize(self._io_func, self._meta, self._inputs, self._divisions)
        return f"{self._label}-{token}"

    @cached_property
    def dependencies(self) -> frozenset[CollectionOperation]:
        _dep = LiteralInputs({(i,): val for i, val in enumerate(self._inputs)})
        return frozenset({_dep})

    @property
    def meta(self) -> Any:
        return self._meta

    @cached_property
    def divisions(self) -> tuple | None:
        return tuple(self._divisions or (None,) * (len(self._inputs) + 1))

    @cached_property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[CollectionOperation]]:
        inkeys = [d.name for d in self.dependencies]
        subgraph = {self.name: (self._io_func, inkeys[-1])}
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
            dsk[key] = (self._io_func, dep_subgraph.get(dep_key, dep_key))
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

    _func: Callable
    _meta: Any
    _args: list[Any]
    _divisions: tuple
    _label: str
    _kwargs: dict

    @cached_property
    def name(self) -> str:
        token = tokenize(
            self._func, self._meta, self._args, self._divisions, self._kwargs
        )
        return f"{self._label}-{token}"

    @property
    def meta(self) -> Any:
        return self._meta

    def replace_meta(self, value) -> PartitionwiseOperation:
        return replace(self, _meta=value)

    @property
    def divisions(self) -> tuple | None:
        return self._divisions

    def replace_divisions(self, value) -> PartitionwiseOperation:
        return replace(self, _divisions=value)

    @cached_property
    def dependencies(self) -> frozenset[CollectionOperation]:
        return frozenset(
            arg for arg in self._args if isinstance(arg, CollectionOperation)
        )

    @cached_property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[CollectionOperation]]:
        task = [self._func]
        inkeys = []
        for arg in self._args:
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
                self._kwargs,
            )
            if self._kwargs
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
            task = [self._func]
            for arg in self._args:
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
            for arg in self._args
        ]
        _changes = {"_args": args, **changes}
        return replace(self, **_changes)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class FusedFrameOperations(FusedOperations, FrameOperation):

    _func: SubgraphCallable
    _inkey_mapping: dict[str, str]
    _dependencies: frozenset
    _label: str
    _meta: Any
    _divisions: tuple | None

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
        _inkey_mapping = {}
        _subgraph_callable, all_deps = fuse_subgraph_callables(operation, fusable)
        for dep in all_deps:
            key = dep.name
            _dependencies.add(dep)
            _inkey_mapping[key] = key

        # Return new FusedOperations object
        return cls(
            _subgraph_callable,
            _inkey_mapping,
            frozenset(_dependencies),
            label,
            operation.meta,
            operation.divisions,
        )

    @cached_property
    def name(self) -> str:
        token = tokenize(
            self._func,
            self._inkey_mapping,
            self._dependencies,
            self._meta,
            self._divisions,
        )
        return f"{self._label}-{token}"

    @property
    def meta(self) -> Any:
        return self._meta

    def replace_meta(self, value) -> FusedFrameOperations:
        return replace(self, _meta=value)

    @property
    def divisions(self) -> tuple | None:
        return self._divisions or (None,) * (len(next(iter(self.dependencies))) + 1)

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
        _inkey_mapping = {}
        _dependency_dict = {d.name: d for d in self.dependencies}

        for inkey in self._func.inkeys:
            dep_name = self._inkey_mapping[inkey]
            dep = _dependency_dict[dep_name]
            _dep = replace_dependencies[dep.name]
            _dependencies.add(_dep)
            _inkey_mapping[inkey] = _dep.name

        _changes = {
            "_inkey_mapping": _inkey_mapping,
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

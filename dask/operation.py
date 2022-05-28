from __future__ import annotations

import copy
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Callable, Hashable, Protocol, TypeVar, runtime_checkable

from dask.base import tokenize
from dask.core import reverse_dict
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import SubgraphCallable

KeyType = TypeVar("KeyType", bound=Hashable)
CollectionOperationType = TypeVar(
    "CollectionOperationType", bound="CollectionOperation"
)


@runtime_checkable
class CollectionOperation(Hashable, Protocol[KeyType]):
    """CollectionOperation class

    Encapsulates the state and graph-creation logic for
    a generic Dask collection.
    """

    @property
    def name(self) -> str:
        """Return the name for this CollectionOperation

        This name should match the string label used in the
        output keys of a task grah terminating with this operation.
        """
        raise NotImplementedError

    @property
    def dependencies(self) -> frozenset[CollectionOperation]:
        """Return set of CollectionOperation dependencies"""
        raise NotImplementedError

    def subgraph(
        self, keys: list[KeyType]
    ) -> tuple[dict[KeyType, Any], dict[CollectionOperation, list[Hashable]]]:
        """Return the subgraph and key dependencies for this operation

        NOTE: Since this method returns a mapping between dependencies
        and required keys, the logic may include recursively fusion.

        Parameters
        ----------
        keys : list[KeyType]
            List of required output keys needed from this collection

        Returns
        -------
        graph : dict[tuple[KeyType], Any]
            The subgraph for the current operation
        dependencies : dict[CollectionOperation, list[Hashable]]
            A dictionary mapping ``CollectionOperation`` objects
            to required keys. This dictionary will be used by the
            global graph-generation algorithm to determine which
            operation-key combinations need to be materialized after
            this operation.
        """
        raise NotImplementedError

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> CollectionOperation:
        """Reinitialize this CollectionOperation

        Parameters
        ----------
        replace_dependencies : dict[str, CollectionOperation]
            Replaced dependencies for the new operation
        **changes : dict
            New fields to use when initializing the new operation
        """
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[KeyType]:
        """Get the collection keys for this operation"""
        raise NotImplementedError

    def copy(self: CollectionOperationType) -> CollectionOperationType:
        """Return a shallow copy of this operation"""
        return copy.copy(self)

    @cached_property
    def dask(self) -> HighLevelGraph:
        dsk = generate_graph(self)
        return HighLevelGraph.from_collections(
            self.name,
            dsk,
            dependencies=[],
        )


@dataclass(frozen=True)
class LiteralInputs(CollectionOperation):
    """LiteralInputs class

    Defines literal block/partition inputs to a CollectionOperation
    """

    inputs: Mapping[tuple, Any]
    label: str = "literals"

    @cached_property
    def name(self) -> str:
        return f"{self.label}-{tokenize(self.inputs)}"

    @property
    def collection_keys(self) -> list:
        keys = self.inputs.keys()  # This may not always work
        return [(self.name,) + key for key in keys]

    def subgraph(self, keys) -> tuple[dict, dict]:
        dsk: dict[tuple, Any] = {}
        for key in keys:
            index = key[1:]
            dsk[key] = self.inputs[index]
        return dsk, {}

    @property
    def dependencies(self) -> frozenset[CollectionOperation]:
        # LiteralInputs can not have dependencies
        return frozenset()

    def reinitialize(
        self, replace_dependencies: dict[str, CollectionOperation], **changes
    ) -> LiteralInputs:
        # Nothing to "reinitialize"
        return self

    def __hash__(self):
        return hash(self.name)


@runtime_checkable
class FusableOperation(CollectionOperation[KeyType], Protocol):
    """FusableOperation class

    A block/partition-wise collection operation which can
    be fused with other ``FusableOperation`` dependencies. In
    order to enable operation fusion, the subclass must
    define a ``subgraph_callable`` method.
    """

    @property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[CollectionOperation]]:
        """Return a SubgraphCallable representation of the
        function being mapped by this operation and its
        dependencies

        Returns
        -------
        func : SubgraphCallable
            The subgraph for the current operation
        dependencies : frozenset[CollectionOperation]
            The set of ``CollectionOperation`` objects
            required as inputs to ``func``.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class FusedOperations(FusableOperation[KeyType]):
    """FusedOperations class

    A specialized ``FusableOperation`` class corresponding
    to multiple 'fused' ``FusableOperation`` objects.
    """

    func: SubgraphCallable
    inkey_mapping: dict[str, str]
    label: str
    _dependencies: frozenset

    @classmethod
    def from_operation(
        cls,
        operation: CollectionOperation,
        fusable: set | bool,
        label: str,
    ):
        raise NotImplementedError

    @property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[CollectionOperation]]:
        return self.func, self.dependencies

    @property
    def dependencies(self):
        return self._dependencies

    def subgraph(self, keys) -> tuple[dict, dict]:
        func, deps = self.subgraph_callable

        # Check if we have MapInput dependencies to fuse
        dep_subgraphs = {}
        dep_keys = {}
        for dep in deps:
            dep_name = dep.name
            input_op_keys = [(dep_name,) + tuple(key[1:]) for key in keys]
            if isinstance(dep, LiteralInputs):
                dep_subgraphs.update(dep.subgraph(input_op_keys)[0])
            else:
                dep_keys[dep] = input_op_keys

        _dependencies_dict = {d.name: d for d in self.dependencies}

        # Build subgraph with LiteralInputs dependencies fused
        dsk = {}
        for key in keys:
            task = [func]
            for inkey in func.inkeys:
                dep_name = self.inkey_mapping[inkey]
                real_dep = _dependencies_dict[dep_name]
                dep_key = (real_dep.name,) + tuple(key[1:])
                task.append(dep_subgraphs.get(dep_key, dep_key))
            dsk[key] = tuple(task)
        return dsk, dep_keys

    def __hash__(self):
        return hash(self.name)


#
# Expression-Graph Traversal Logic
#


class MemoizingVisitor:
    """Helper class for memoized graph traversal"""

    def __init__(self, func: Callable, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.cache: dict = {}

    def __call__(self, operation, *args):
        key = tokenize(operation, *args)
        try:
            return self.cache[key]
        except KeyError:
            return self.cache.setdefault(
                key,
                self.func(
                    operation,
                    self,
                    *args,
                    **self.kwargs.get(operation.name, {}),
                ),
            )


def _generate_graph(operation, visitor, keys):
    dsk, dependency_keys = operation.subgraph(keys)
    for dep, dep_keys in dependency_keys.items():
        dsk.update(visitor(dep, dep_keys))
    return dsk


def generate_graph(operation, keys=None):
    keys = keys or operation.collection_keys
    return MemoizingVisitor(_generate_graph)(operation, keys)


def _replay(operation: CollectionOperation, visitor: MemoizingVisitor, **kwargs):
    # Helper function for ``replay``
    transformed_dependencies = {}
    operation = kwargs.pop("replace_with", operation)
    for dep in operation.dependencies:
        transformed_dependencies[dep.name] = visitor(dep)
    return operation.reinitialize(transformed_dependencies, **kwargs)


def replay(operation: CollectionOperation, operation_kwargs=None):
    """Replay the operation recursively"""
    visitor = MemoizingVisitor(_replay, **(operation_kwargs or {}))
    return visitor(operation)


def _operation_dag(operation: CollectionOperation, visitor: MemoizingVisitor):
    dag: dict = {operation.name: set()}
    for dep in operation.dependencies:
        dep_name = dep.name
        dag[operation.name].add(dep_name)
        dag.update(visitor(dep))
    return dag


def operation_dag(operation: CollectionOperation):
    return MemoizingVisitor(_operation_dag)(operation)


def _operations(operation: CollectionOperation, visitor: MemoizingVisitor):
    ops = {operation.name: operation}
    for dep in operation.dependencies:
        ops.update(visitor(dep))
    return ops


def operations(operation: CollectionOperation):
    return MemoizingVisitor(_operations)(operation)


def _fuse_subgraph_callables(
    operation: CollectionOperation, visitor: MemoizingVisitor, fusable: bool | set
) -> tuple[SubgraphCallable, set[CollectionOperation]]:
    if not isinstance(operation, FusableOperation):
        raise ValueError
    func, deps = operation.subgraph_callable
    all_deps: set = set(deps)

    dep_funcs = {}
    for dep in deps:
        if (
            fusable is True or (isinstance(fusable, set) and dep.name in fusable)
        ) and isinstance(dep, FusableOperation):
            _func, _deps = visitor(dep, fusable)
            dep_funcs[dep.name] = _func
            all_deps |= _deps

    if dep_funcs:
        new_dsk = func.dsk.copy()
        new_inkeys = []
        for key in func.inkeys:
            if key in dep_funcs:
                dep_func = dep_funcs[key]
                new_dsk.update(dep_func.dsk)
                new_inkeys.extend([k for k in dep_func.inkeys if k not in new_inkeys])
            elif key not in new_inkeys:
                new_inkeys.append(key)
        func = SubgraphCallable(
            dsk=new_dsk,
            outkey=operation.name,
            inkeys=new_inkeys,
        )

    return func, all_deps


def fuse_subgraph_callables(operation: CollectionOperation, fusable: bool | set = True):
    visitor = MemoizingVisitor(_fuse_subgraph_callables)
    return visitor(operation, fusable)


def map_fusion(operation: CollectionOperation, fused_op_cls: FusedOperations):

    all_ops = operations(operation)
    op_dag = operation_dag(operation)
    reverse_dag = reverse_dict(op_dag)

    fusable: dict[str, set] = defaultdict(set)
    fuse_operation: str | None = None

    work = [operation.name]
    while work:

        op_name = work.pop()
        op = all_ops[op_name]
        work.extend(op_dag[op_name])
        dependents = reverse_dag[op_name]

        if isinstance(op, FusableOperation):
            map_dependents = {
                dep_name
                for dep_name in dependents
                if isinstance(all_ops[dep_name], FusableOperation)
            }
            if not map_dependents:
                fuse_operation = op_name
            elif fuse_operation and (
                len(
                    {
                        map_dep
                        for map_dep in map_dependents
                        if (
                            fuse_operation is None
                            or map_dep not in fusable[fuse_operation]
                        )
                    }
                )
                <= 1
            ):
                fusable[fuse_operation].add(op_name)

    replaced = {}
    for op_name, fusable_set in fusable.items():
        # Define new fused operation
        new_op = fused_op_cls.from_operation(all_ops[op_name], fusable_set, "fused")
        replaced[op_name] = {"replace_with": new_op}

    return replay(operation, operation_kwargs=replaced)

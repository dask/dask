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
_CollectionOperationType = TypeVar(
    "_CollectionOperationType", bound="_CollectionOperation"
)


@runtime_checkable
class _CollectionOperation(Hashable, Protocol[KeyType]):
    """_CollectionOperation class

    Encapsulates the state and graph-creation logic for
    a generic Dask collection.
    """

    @property
    def name(self) -> str:
        """Return the name for this _CollectionOperation

        This name should match the string label used in the
        output keys of a task grah terminating with this operation.
        """
        raise NotImplementedError

    @property
    def dependencies(self) -> frozenset[_CollectionOperation]:
        """Return set of _CollectionOperation dependencies"""
        raise NotImplementedError

    def subgraph(
        self, keys: list[KeyType]
    ) -> tuple[dict[KeyType, Any], dict[_CollectionOperation, list[Hashable]]]:
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
        dependencies : dict[_CollectionOperation, list[Hashable]]
            A dictionary mapping ``_CollectionOperation`` objects
            to required keys. This dictionary will be used by the
            global graph-generation algorithm to determine which
            operation-key combinations need to be materialized after
            this operation.
        """
        raise NotImplementedError

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> _CollectionOperation:
        """Reinitialize this _CollectionOperation

        Parameters
        ----------
        replace_dependencies : dict[str, _CollectionOperation]
            Replaced dependencies for the new operation
        **changes : dict
            New fields to use when initializing the new operation
        """
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[KeyType]:
        """Get the collection keys for this operation"""
        raise NotImplementedError

    def copy(self: _CollectionOperationType) -> _CollectionOperationType:
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
class LiteralInputs(_CollectionOperation):
    """LiteralInputs class

    Defines literal block/partition inputs to a _CollectionOperation
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
    def dependencies(self) -> frozenset[_CollectionOperation]:
        # LiteralInputs can not have dependencies
        return frozenset()

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> LiteralInputs:
        # Nothing to "reinitialize"
        return self

    def __hash__(self):
        return hash(self.name)


@runtime_checkable
class _FusableOperation(_CollectionOperation[KeyType], Protocol):
    """_FusableOperation class

    A block/partition-wise collection operation which can
    be fused with other ``_FusableOperation`` dependencies. In
    order to enable operation fusion, the subclass must
    define a ``subgraph_callable`` method.
    """

    @property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[_CollectionOperation]]:
        """Return a SubgraphCallable representation of the
        function being mapped by this operation and its
        dependencies

        Returns
        -------
        func : SubgraphCallable
            The subgraph for the current operation
        dependencies : frozenset[_CollectionOperation]
            The set of ``_CollectionOperation`` objects
            required as inputs to ``func``.
        """
        raise NotImplementedError


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


def _replay(operation: _CollectionOperation, visitor: MemoizingVisitor, **kwargs):
    # Helper function for ``replay``
    transformed_dependencies = {}
    operation = kwargs.pop("replace_with", operation)
    for dep in operation.dependencies:
        transformed_dependencies[dep.name] = visitor(dep)
    return operation.reinitialize(transformed_dependencies, **kwargs)


def replay(operation: _CollectionOperation, operation_kwargs=None):
    """Replay the operation recursively"""
    visitor = MemoizingVisitor(_replay, **(operation_kwargs or {}))
    return visitor(operation)


def _operation_dag(operation: _CollectionOperation, visitor: MemoizingVisitor):
    dag: dict = {operation.name: set()}
    for dep in operation.dependencies:
        dep_name = dep.name
        dag[operation.name].add(dep_name)
        dag.update(visitor(dep))
    return dag


def operation_dag(operation: _CollectionOperation):
    return MemoizingVisitor(_operation_dag)(operation)


def _operations(operation: _CollectionOperation, visitor: MemoizingVisitor):
    ops = {operation.name: operation}
    for dep in operation.dependencies:
        ops.update(visitor(dep))
    return ops


def operations(operation: _CollectionOperation):
    return MemoizingVisitor(_operations)(operation)


def _fuse_subgraph_callables(
    operation: _CollectionOperation, visitor: MemoizingVisitor, fusable: bool | set
) -> tuple[SubgraphCallable, set[_CollectionOperation]]:
    if not isinstance(operation, _FusableOperation):
        raise ValueError
    func, deps = operation.subgraph_callable
    all_deps: set = set(deps)

    dep_funcs = {}
    for dep in deps:
        if (
            fusable is True or (isinstance(fusable, set) and dep.name in fusable)
        ) and isinstance(dep, _FusableOperation):
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


def fuse_subgraph_callables(
    operation: _CollectionOperation, fusable: bool | set = True
):
    visitor = MemoizingVisitor(_fuse_subgraph_callables)
    return visitor(operation, fusable)


def map_fusion(operation: _CollectionOperation, make_fused_op: Callable):

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

        if isinstance(op, _FusableOperation):
            map_dependents = {
                dep_name
                for dep_name in dependents
                if isinstance(all_ops[dep_name], _FusableOperation)
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
        new_op = make_fused_op(all_ops[op_name], fusable_set, "fused")
        replaced[op_name] = {"replace_with": new_op}

    return replay(operation, operation_kwargs=replaced)

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Hashable, TypeVar

from dask.base import tokenize

KeyType = TypeVar("KeyType", bound=Hashable)


@dataclass(frozen=True)
class CollectionOperation(Generic[KeyType]):
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
    def dependencies(self) -> frozenset[CollectionOperation[Any]]:
        """Return set of CollectionOperation dependencies"""
        raise NotImplementedError

    def subgraph(
        self, keys: list[tuple]
    ) -> tuple[
        dict[KeyType, Any], dict[CollectionOperation[Any], list[tuple[Hashable]]]
    ]:
        """Return the subgraph and key dependencies for this operation

        NOTE: Since this method returns a mapping between dependencies
        and required keys, the logic may include recursively fusion.

        Parameters
        ----------
        keys : list[tuple]
            List of required output keys needed from this collection

        Returns
        -------
        graph : dict
            The subgraph for the current operation
        dependencies : dict[CollectionOperation, list[tuple]]
            A dictionary mapping ``CollectionOperation`` objects
            to required keys. This dictionary will be used by the
            global graph-generation algorithm to determine which
            operation-key combinations need to be materialized after
            this operation.
        """
        raise NotImplementedError

    def reinitialize(
        self, replace_dependencies: dict, **changes
    ) -> CollectionOperation[Any]:
        """Reinitialize this CollectionOperation

        Parameters
        ----------
        replace_dependencies : dict[CollectionOperation, CollectionOperation]
            Replaced dependencies for the new operation
        **changes : dict
            New fields to use when initializing the new operation
        """
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[KeyType]:
        """Get the collection keys for this operation"""
        raise NotImplementedError

    def copy(self) -> CollectionOperation[Any]:
        """Return a shallow copy of this operation"""
        raise NotImplementedError


#
# Expression-Graph Traversal Logic
#


class MemoizingVisitor:
    """Helper class for memoized graph traversal"""

    def __init__(self, func, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.cache = {}

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


def _replay(operation: CollectionOperation[Any], visitor: MemoizingVisitor, **kwargs):
    # Helper function for ``replay``
    transformed_dependencies = {}
    operation = kwargs.pop("replace_with", operation)
    for dep in operation.dependencies:
        transformed_dependencies[dep.name] = visitor(dep)
    return operation.reinitialize(transformed_dependencies, **kwargs)


def replay(operation: CollectionOperation[Any], operation_kwargs=None):
    """Replay the operation recursively"""
    visitor = MemoizingVisitor(_replay, **(operation_kwargs or {}))
    return visitor(operation)

from __future__ import annotations

from typing import Hashable

from dask.base import tokenize


class CollectionOperation:
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
    def dependencies(self) -> set[CollectionOperation]:
        """Return set of CollectionOperation dependencies"""
        raise NotImplementedError

    def subgraph(self, keys: list[tuple]) -> tuple:
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

    def reinitialize(self, new_dependencies: dict, **new_kwargs) -> CollectionOperation:
        """Reinitialize this CollectionOperation

        Parameters
        ----------
        new_dependencies : dict[CollectionOperation, CollectionOperation]
            Dependencies for the new operation
        **new_kwargs : dict
            New kwargs to use when initializing the new operation
        """
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[Hashable]:
        """Get the collection keys for this operation"""
        raise NotImplementedError

    def copy(self) -> CollectionOperation:
        """Return a shallow copy of this operation"""
        raise NotImplementedError

    def __hash__(self) -> int:
        """Hash a CollectionOperation"""
        raise NotImplementedError

    def replay(self, **kwargs) -> CollectionOperation:
        """Replay this CollectionOperation

        This will recursively call ``reinitialize`` for all
        operations in the expression graph.

        Parameters
        ----------
        **kwargs : dict
            New kwargs to use when reinitializing this operation
        """
        _operation_kwargs = {self.name: kwargs} if kwargs else {}
        return replay(self, operation_kwargs=_operation_kwargs)


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


def _replay(operation, visitor, **kwargs):
    # Helper function for ``replay``
    transformed_dependencies = {}
    operation = kwargs.pop("replace_with", operation)
    for dep in operation.dependencies:
        transformed_dependencies[dep.name] = visitor(dep)
    return operation.reinitialize(transformed_dependencies, **kwargs)


def replay(operation, operation_kwargs=None):
    """Replay the operation by calling reinitialize recursively"""
    visitor = MemoizingVisitor(_replay, **(operation_kwargs or {}))
    return visitor(operation)

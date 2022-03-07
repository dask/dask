from __future__ import annotations

from typing import (
    Any,
    Hashable,
    Mapping,
    MutableMapping,
    Protocol,
    Sequence,
    TypeVar,
    runtime_checkable,
)

try:
    from IPython.display import DisplayObject
except ImportError:
    DisplayObject = object


T = TypeVar("T")
CollectionType = TypeVar("CollectionType", bound="DaskCollection")


class PostComputeCallable(Protocol):
    def __call__(self, partitions: Sequence[Any], *args) -> Any:
        """ """


class PostPersistCallable(Protocol):
    def __call__(
        self,
        dsk: Mapping,
        *args,
        rename: Mapping[str, str] | None = None,
    ) -> DaskCollection:
        """ """


@runtime_checkable
class DaskCollection(Protocol):
    """Protocal defining the interface of a Dask collection."""

    def __dask_graph__(self) -> Mapping | None:
        """The Dask task graph.

        The core Dask collections (Array, DatFrame, Bag, and Delayed)
        use a :py:class:`HighLevelGraph` to represent the collection
        task graph. It is also possible to represent the task graph as
        a low level graph using a Python dictionary.

        Returns
        -------
        Mapping or None
            The Dask task graph. If the task graph is ``None`` then
            the instance will not be interpreted as a Dask collection.
            If the instance returns a
            :py:class:`dask.highlevelgraph.HighLevelGraph` then the
            :py:func:`__dask_layers__` method must be defined.

        """

    def __dask_keys__(self) -> list[Hashable]:
        """The output keys of the task graph.

        Returns
        -------
        list
            The output keys of the collection's task graph.

        """

    def __dask_postcompute__(self) -> tuple[PostComputeCallable, tuple]:
        """Finalizer function and optional arguments to construct final result.

        Upon computation each key in the collection will have an in
        memory result, the postcompute function combines each key's
        result into a final in memory representation. For example,
        dask.array.Array concatenates the arrays at each chunk into a
        final in-memory array.

        Returns
        -------
        PostComputeCallable
            Function that takes the sequence of the results of each
            final key along with optional arguments. The signure must
            be ``finalize(results: Sequence[Any], *args)``.
        tuple[Any, ...]
            Optional arguments passed to the function following the
            key results (the `*args` part of the
            ``PostComputeCallable``. If no additional arguments are to
            be passed then this must be an empty tuple.

        """

    def __dask_postpersist__(self) -> tuple[PostPersistCallable, tuple]:
        """Rebuilder function and optional arguments to contruct a persisted collection.

        Returns
        -------
        PostPersistCallable
            Function that rebuilds the collection. The signature
            should be
            ``rebuild(dsk: Mapping, *args: Any, rename: Mapping[str, str] | None)``.
            The callable should return an equivalent Dask collection
            with the same keys as `self`, but with the results that
            are computed through a different graph. In the case of
            :py:func:`dask.persist`, the new graph will have just the
            output keys and the values already computed.
        tuple[Any, ...]
            Optional arugments passed toe the rebuild function. If no
            additional arguments are to be passed then this must be an
            empty tuple.

        """

    def __dask_tokenize__(self) -> Hashable:
        """Value that must fully represent the object."""

    @staticmethod
    def __dask_optimize__(dsk: MutableMapping, keys: Sequence[Hashable], **kwargs) -> MutableMapping:
        """Given a graph and keys, return a new optimized graph."""

    @staticmethod
    def __dask_scheduler__(dsk: Mapping, keys: Sequence[Hashable], **kwargs) -> Any:
        """The default scheduler get to use for this object."""

    def compute(self, **kwargs) -> Any:
        """TODO"""

    def persist(self: CollectionType, **kwargs) -> CollectionType:
        """TODO"""

    def visualize(self, **kwargs) -> DisplayObject:
        """TODO"""


class HLGDaskCollection(DaskCollection):
    def __dask_layers__(self) -> Sequence[str]:
        """TODO"""

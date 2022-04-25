from __future__ import annotations

import abc
from collections.abc import Hashable, Mapping, Sequence
from typing import Any, Protocol, TypeVar, runtime_checkable

try:
    from IPython.display import DisplayObject
except ImportError:
    DisplayObject = Any


T = TypeVar("T")
CollectionType = TypeVar("CollectionType", bound="DaskCollection")
CovariantCollection = TypeVar(
    "CovariantCollection", bound="DaskCollection", covariant=True
)


class SchedulerGetMethod(Protocol):
    """Protocol for defining the __dask_scheduler__ attribute."""

    @staticmethod
    def __call__(
        dsk: Mapping,
        keys: Sequence[Hashable] | Hashable,
        **kwargs: Any,
    ) -> Any:
        """Default dask.get method.

        Parameters
        ----------
        dsk : Mapping
            The task graph.
        keys : Sequence[Hashable] | Hashable
            The keys of the task graph to get.
        **kwargs : Any
            Arguments passed to the get method.

        Returns
        -------
        Any
            The result.

        """
        raise NotImplementedError("Inheriting class must implement this method.")


class PostComputeCallable(Protocol):
    """Protocol defining the signature of a __dask_postcompute__ method."""

    @staticmethod
    def __call__(parts: Sequence[Any], *args: Any) -> Any:
        """Method called after the keys of a collection are computed.

        Parameters
        ----------
        parts : Sequence[Any]
            The sequence of computed results on each partition (key).
        *args : Any
            Additional optional arguments If no extra arguments are
            necessary, it must be an empty tuple.

        Returns
        -------
        Any
            The final result of the computed collection. For example,
            the finalize function for ``dask.array.Array``
            concatenates all the individual array chunks into one
            large NumPy array, which is then the result of compute.

        """
        raise NotImplementedError("Inheriting class must implement this method.")


class PostPersistCallable(Protocol[CovariantCollection]):
    """Protocol defining the signature of a __dask_postpersist__ method."""

    @staticmethod
    def __call__(
        dsk: Mapping,
        *args: Any,
        rename: Mapping[str, str] | None = None,
    ) -> CovariantCollection:
        """Method called to rebuild a persisted collection.

        Parameters
        ----------
        dsk: Mapping
            A mapping which contains at least the output keys returned
            by __dask_keys__().
        *args : Any
            Additional optional arguments If no extra arguments are
            necessary, it must be an empty tuple.
        rename : Mapping[str, str], optional
            If defined, it indicates that output keys may be changing
            too; e.g. if the previous output of :meth:`__dask_keys__`
            was ``[("a", 0), ("a", 1)]``, after calling
            ``rebuild(dsk, *extra_args, rename={"a": "b"})``
            it must become ``[("b", 0), ("b", 1)]``.
            The ``rename`` mapping may not contain the collection
            name(s); in such case the associated keys do not change.
            It may contain replacements for unexpected names, which
            must be ignored.

        Returns
        -------
        Collection
            An equivalent Dask collection with the same keys as
            computed through a different graph.

        """
        raise NotImplementedError("Inheriting class must implement this method.")


@runtime_checkable
class DaskCollection(Protocol):
    """Protocal defining the interface of a Dask collection."""

    @abc.abstractmethod
    def __dask_graph__(self) -> Mapping:
        """The Dask task graph.

        The core Dask collections (Array, DatFrame, Bag, and Delayed)
        use a :py:class:`HighLevelGraph` to represent the collection
        task graph. It is also possible to represent the task graph as
        a low level graph using a Python dictionary.

        Returns
        -------
        Mapping
            The Dask task graph. If the instance returns a
            :py:class:`dask.highlevelgraph.HighLevelGraph` then the
            :py:func:`__dask_layers__` method must be defined.

        """
        raise NotImplementedError("Inheriting class must implement this method.")

    @abc.abstractmethod
    def __dask_keys__(self) -> list[Hashable]:
        """The output keys of the task graph.

        Returns
        -------
        list
            The output keys of the collection's task graph.

        """
        raise NotImplementedError("Inheriting class must implement this method.")

    @abc.abstractmethod
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
            Callable that recieves the sequence of the results of each
            final key along with optional arguments. The signure must
            be ``finalize(results: Sequence[Any], *args)``.
        tuple[Any, ...]
            Optional arguments passed to the function following the
            key results (the `*args` part of the
            ``PostComputeCallable``. If no additional arguments are to
            be passed then this must be an empty tuple.

        """
        raise NotImplementedError("Inheriting class must implement this method.")

    @abc.abstractmethod
    def __dask_postpersist__(self) -> tuple[PostPersistCallable, tuple]:
        """Rebuilder function and optional arguments to contruct a persisted collection.

        Returns
        -------
        PostPersistCallable
            Callable that rebuilds the collection. The signature
            should be
            ``rebuild(dsk: Mapping, *args: Any, rename: Mapping[str, str] | None)``.
            The callable should return an equivalent Dask collection
            with the same keys as `self`, but with results that are
            computed through a different graph. In the case of
            :py:func:`dask.persist`, the new graph will have just the
            output keys and the values already computed.
        tuple[Any, ...]
            Optional arugments passed to the rebuild callable. If no
            additional arguments are to be passed then this must be an
            empty tuple.

        """
        raise NotImplementedError("Inheriting class must implement this method.")

    @abc.abstractmethod
    def __dask_tokenize__(self) -> Hashable:
        """Value that must fully represent the object."""
        raise NotImplementedError("Inheriting class must implement this method.")

    __dask_optimize__: Any
    """Given a graph and keys, return a new optimized graph.

    This method can be either a ``staticmethod`` or a
    ``classmethod``, but not an ``instancemethod``.

    Note that graphs and keys are merged before calling
    ``__dask_optimize__``; as such, the graph and keys passed to
    this method may represent more than one collection sharing the
    same optimize method.

    Parameters
    ----------
    dsk : MutableMapping
        The merged graphs from all collections sharing the same
        ``__dask_optimize__`` method.
    keys : Sequence[Hashable]
        A list of the outputs from ``__dask_keys__`` from all
        collections sharing the same ``__dask_optimize__`` method.
    **kwargs : Any
        Extra keyword arguments forwarded from the call to
        ``compute`` or ``persist``. Can be used or ignored as
        needed.

    Returns
    -------
    MutableMapping
        The optimized Dask graph.

    """

    __dask_scheduler__: Any
    """The default scheduler ``get`` to use for this object.

    Usually attached to the class as a staticmethod, e.g.:

    >>> import dask.threaded
    >>> class MyCollection:
    ...     # Use the threaded scheduler by default
    ...     __dask_scheduler__ = staticmethod(dask.threaded.get)

    """

    @abc.abstractmethod
    def compute(self, **kwargs: Any) -> Any:
        """Compute this dask collection.

        This turns a lazy Dask collection into its in-memory
        equivalent. For example a Dask array turns into a NumPy array
        and a Dask dataframe turns into a Pandas dataframe. The entire
        dataset must fit into memory before calling this operation.

        Parameters
        ----------
        scheduler : string, optional
            Which scheduler to use like "threads", "synchronous" or
            "processes". If not provided, the default is to check the
            global settings first, and then fall back to the
            collection defaults.
        optimize_graph : bool, optional
            If True [default], the graph is optimized before
            computation. Otherwise the graph is run as is. This can be
            useful for debugging.
        kwargs
            Extra keywords to forward to the scheduler function.

        Returns
        -------
        The collection's computed result.

        See Also
        --------
        dask.base.compute

        """
        raise NotImplementedError("Inheriting class must implement this method.")

    @abc.abstractmethod
    def persist(self: CollectionType, **kwargs: Any) -> CollectionType:
        """Persist this dask collection into memory

        This turns a lazy Dask collection into a Dask collection with
        the same metadata, but now with the results fully computed or
        actively computing in the background.

        The action of function differs significantly depending on the
        active task scheduler. If the task scheduler supports
        asynchronous computing, such as is the case of the
        dask.distributed scheduler, then persist will return
        *immediately* and the return value's task graph will contain
        Dask Future objects. However if the task scheduler only
        supports blocking computation then the call to persist will
        *block* and the return value's task graph will contain
        concrete Python results.

        This function is particularly useful when using distributed
        systems, because the results will be kept in distributed
        memory, rather than returned to the local process as with
        compute.

        Parameters
        ----------
        scheduler : string, optional
            Which scheduler to use like "threads", "synchronous" or
            "processes". If not provided, the default is to check the
            global settings first, and then fall back to the
            collection defaults.
        optimize_graph : bool, optional
            If True [default], the graph is optimized before
            computation. Otherwise the graph is run as is. This can be
            useful for debugging.
        **kwargs
            Extra keywords to forward to the scheduler function.

        Returns
        -------
        New dask collections backed by in-memory data

        See Also
        --------
        dask.base.persist

        """
        raise NotImplementedError("Inheriting class must implement this method.")

    @abc.abstractmethod
    def visualize(
        self,
        filename: str = "mydask",
        format: str | None = None,
        optimize_graph: bool = False,
        **kwargs: Any,
    ) -> DisplayObject | None:
        """Render the computation of this object's task graph using graphviz.

        Requires ``graphviz`` to be installed.

        Parameters
        ----------
        filename : str or None, optional
            The name of the file to write to disk. If the provided
            `filename` doesn't include an extension, '.png' will be
            used by default. If `filename` is None, no file will be
            written, and we communicate with dot using only pipes.
        format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
            Format in which to write output file. Default is 'png'.
        optimize_graph : bool, optional
            If True, the graph is optimized before rendering.
            Otherwise, the graph is displayed as is. Default is False.
        color: {None, 'order'}, optional
            Options to color nodes. Provide ``cmap=`` keyword for
            additional colormap
        **kwargs
           Additional keyword arguments to forward to ``to_graphviz``.

        Examples
        --------
        >>> x.visualize(filename='dask.pdf')  # doctest: +SKIP
        >>> x.visualize(filename='dask.pdf', color='order')  # doctest: +SKIP

        Returns
        -------
        result : IPython.diplay.Image, IPython.display.SVG, or None
            See dask.dot.dot_graph for more information.

        See Also
        --------
        dask.base.visualize
        dask.dot.dot_graph

        Notes
        -----
        For more information on optimization see here:

        https://docs.dask.org/en/latest/optimize.html

        """
        raise NotImplementedError("Inheriting class must implement this method.")


@runtime_checkable
class HLGDaskCollection(DaskCollection, Protocol):
    """Protocal defining a Dask collection that uses HighLevelGraphs."""

    @abc.abstractmethod
    def __dask_layers__(self) -> Sequence[str]:
        """Names of the HighLevelGraph layers."""
        raise NotImplementedError("Inheriting class must implement this method.")

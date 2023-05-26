from __future__ import annotations

import dataclasses
import datetime
import hashlib
import inspect
import os
import pathlib
import pickle
import threading
import uuid
import warnings
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Executor
from contextlib import contextmanager
from enum import Enum
from functools import partial
from numbers import Integral, Number
from operator import getitem
from typing import TYPE_CHECKING, Literal, Protocol

from tlz import curry, groupby, identity, merge
from tlz.functoolz import Compose

from dask import config, local
from dask._compatibility import EMSCRIPTEN
from dask.core import flatten
from dask.core import get as simple_get
from dask.core import literal, quote
from dask.hashing import hash_buffer_hex
from dask.system import CPU_COUNT
from dask.typing import SchedulerGetCallable
from dask.utils import Dispatch, apply, ensure_dict, is_namedtuple_instance, key_split

__all__ = (
    "DaskMethodsMixin",
    "annotate",
    "is_dask_collection",
    "compute",
    "persist",
    "optimize",
    "visualize",
    "tokenize",
    "normalize_token",
    "get_collection_names",
    "get_name_from_key",
    "replace_name_in_key",
    "clone_key",
)

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer


@contextmanager
def annotate(**annotations):
    """Context Manager for setting HighLevelGraph Layer annotations.

    Annotations are metadata or soft constraints associated with
    tasks that dask schedulers may choose to respect: They signal intent
    without enforcing hard constraints. As such, they are
    primarily designed for use with the distributed scheduler.

    Almost any object can serve as an annotation, but small Python objects
    are preferred, while large objects such as NumPy arrays are discouraged.

    Callables supplied as an annotation should take a single *key* argument and
    produce the appropriate annotation. Individual task keys in the annotated collection
    are supplied to the callable.

    Parameters
    ----------
    **annotations : key-value pairs

    Examples
    --------

    All tasks within array A should have priority 100 and be retried 3 times
    on failure.

    >>> import dask
    >>> import dask.array as da
    >>> with dask.annotate(priority=100, retries=3):
    ...     A = da.ones((10000, 10000))

    Prioritise tasks within Array A on flattened block ID.

    >>> nblocks = (10, 10)
    >>> with dask.annotate(priority=lambda k: k[1]*nblocks[1] + k[2]):
    ...     A = da.ones((1000, 1000), chunks=(100, 100))

    Annotations may be nested.

    >>> with dask.annotate(priority=1):
    ...     with dask.annotate(retries=3):
    ...         A = da.ones((1000, 1000))
    ...     B = A + 1
    """

    # Sanity check annotations used in place of
    # legacy distributed Client.{submit, persist, compute} keywords
    if "workers" in annotations:
        if isinstance(annotations["workers"], (list, set, tuple)):
            annotations["workers"] = list(annotations["workers"])
        elif isinstance(annotations["workers"], str):
            annotations["workers"] = [annotations["workers"]]
        elif callable(annotations["workers"]):
            pass
        else:
            raise TypeError(
                "'workers' annotation must be a sequence of str, a str or a callable, but got %s."
                % annotations["workers"]
            )

    if (
        "priority" in annotations
        and not isinstance(annotations["priority"], Number)
        and not callable(annotations["priority"])
    ):
        raise TypeError(
            "'priority' annotation must be a Number or a callable, but got %s"
            % annotations["priority"]
        )

    if (
        "retries" in annotations
        and not isinstance(annotations["retries"], Number)
        and not callable(annotations["retries"])
    ):
        raise TypeError(
            "'retries' annotation must be a Number or a callable, but got %s"
            % annotations["retries"]
        )

    if (
        "resources" in annotations
        and not isinstance(annotations["resources"], dict)
        and not callable(annotations["resources"])
    ):
        raise TypeError(
            "'resources' annotation must be a dict, but got %s"
            % annotations["resources"]
        )

    if (
        "allow_other_workers" in annotations
        and not isinstance(annotations["allow_other_workers"], bool)
        and not callable(annotations["allow_other_workers"])
    ):
        raise TypeError(
            "'allow_other_workers' annotations must be a bool or a callable, but got %s"
            % annotations["allow_other_workers"]
        )

    with config.set({f"annotations.{k}": v for k, v in annotations.items()}):
        yield


def is_dask_collection(x) -> bool:
    """Returns ``True`` if ``x`` is a dask collection.

    Parameters
    ----------
    x : Any
        Object to test.

    Returns
    -------
    result : bool
        ``True`` if `x` is a Dask collection.

    Notes
    -----
    The DaskCollection typing.Protocol implementation defines a Dask
    collection as a class that returns a Mapping from the
    ``__dask_graph__`` method. This helper function existed before the
    implementation of the protocol.

    """
    try:
        return x.__dask_graph__() is not None
    except (AttributeError, TypeError):
        return False


class DaskMethodsMixin:
    """A mixin adding standard dask collection methods"""

    __slots__ = ()

    def visualize(self, filename="mydask", format=None, optimize_graph=False, **kwargs):
        """Render the computation of this object's task graph using graphviz.

        Requires ``graphviz`` to be installed.

        Parameters
        ----------
        filename : str or None, optional
            The name of the file to write to disk. If the provided `filename`
            doesn't include an extension, '.png' will be used by default.
            If `filename` is None, no file will be written, and we communicate
            with dot using only pipes.
        format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
            Format in which to write output file.  Default is 'png'.
        optimize_graph : bool, optional
            If True, the graph is optimized before rendering.  Otherwise,
            the graph is displayed as is. Default is False.
        color: {None, 'order'}, optional
            Options to color nodes.  Provide ``cmap=`` keyword for additional
            colormap
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
        dask.visualize
        dask.dot.dot_graph

        Notes
        -----
        For more information on optimization see here:

        https://docs.dask.org/en/latest/optimize.html
        """
        return visualize(
            self,
            filename=filename,
            format=format,
            optimize_graph=optimize_graph,
            **kwargs,
        )

    def persist(self, **kwargs):
        """Persist this dask collection into memory

        This turns a lazy Dask collection into a Dask collection with the same
        metadata, but now with the results fully computed or actively computing
        in the background.

        The action of function differs significantly depending on the active
        task scheduler.  If the task scheduler supports asynchronous computing,
        such as is the case of the dask.distributed scheduler, then persist
        will return *immediately* and the return value's task graph will
        contain Dask Future objects.  However if the task scheduler only
        supports blocking computation then the call to persist will *block*
        and the return value's task graph will contain concrete Python results.

        This function is particularly useful when using distributed systems,
        because the results will be kept in distributed memory, rather than
        returned to the local process as with compute.

        Parameters
        ----------
        scheduler : string, optional
            Which scheduler to use like "threads", "synchronous" or "processes".
            If not provided, the default is to check the global settings first,
            and then fall back to the collection defaults.
        optimize_graph : bool, optional
            If True [default], the graph is optimized before computation.
            Otherwise the graph is run as is. This can be useful for debugging.
        **kwargs
            Extra keywords to forward to the scheduler function.

        Returns
        -------
        New dask collections backed by in-memory data

        See Also
        --------
        dask.persist
        """
        (result,) = persist(self, traverse=False, **kwargs)
        return result

    def compute(self, **kwargs):
        """Compute this dask collection

        This turns a lazy Dask collection into its in-memory equivalent.
        For example a Dask array turns into a NumPy array and a Dask dataframe
        turns into a Pandas dataframe.  The entire dataset must fit into memory
        before calling this operation.

        Parameters
        ----------
        scheduler : string, optional
            Which scheduler to use like "threads", "synchronous" or "processes".
            If not provided, the default is to check the global settings first,
            and then fall back to the collection defaults.
        optimize_graph : bool, optional
            If True [default], the graph is optimized before computation.
            Otherwise the graph is run as is. This can be useful for debugging.
        kwargs
            Extra keywords to forward to the scheduler function.

        See Also
        --------
        dask.compute
        """
        (result,) = compute(self, traverse=False, **kwargs)
        return result

    def __await__(self):
        try:
            from distributed import futures_of, wait
        except ImportError as e:
            raise ImportError(
                "Using async/await with dask requires the `distributed` package"
            ) from e
        from tornado import gen

        @gen.coroutine
        def f():
            if futures_of(self):
                yield wait(self)
            raise gen.Return(self)

        return f().__await__()


def compute_as_if_collection(cls, dsk, keys, scheduler=None, get=None, **kwargs):
    """Compute a graph as if it were of type cls.

    Allows for applying the same optimizations and default scheduler."""
    schedule = get_scheduler(scheduler=scheduler, cls=cls, get=get)
    dsk2 = optimization_function(cls)(dsk, keys, **kwargs)
    return schedule(dsk2, keys, **kwargs)


def dont_optimize(dsk, keys, **kwargs):
    return dsk


def optimization_function(x):
    return getattr(x, "__dask_optimize__", dont_optimize)


def collections_to_dsk(collections, optimize_graph=True, optimizations=(), **kwargs):
    """
    Convert many collections into a single dask graph, after optimization
    """
    from dask.highlevelgraph import HighLevelGraph

    optimizations = tuple(optimizations) + tuple(config.get("optimizations", ()))

    if optimize_graph:
        groups = groupby(optimization_function, collections)

        graphs = []
        for opt, val in groups.items():
            dsk, keys = _extract_graph_and_keys(val)
            dsk = opt(dsk, keys, **kwargs)

            for opt_inner in optimizations:
                dsk = opt_inner(dsk, keys, **kwargs)

            graphs.append(dsk)

        # Merge all graphs
        if any(isinstance(graph, HighLevelGraph) for graph in graphs):
            dsk = HighLevelGraph.merge(*graphs)
        else:
            dsk = merge(*map(ensure_dict, graphs))
    else:
        dsk, _ = _extract_graph_and_keys(collections)

    return dsk


def _extract_graph_and_keys(vals):
    """Given a list of dask vals, return a single graph and a list of keys such
    that ``get(dsk, keys)`` is equivalent to ``[v.compute() for v in vals]``."""
    from dask.highlevelgraph import HighLevelGraph

    graphs, keys = [], []
    for v in vals:
        graphs.append(v.__dask_graph__())
        keys.append(v.__dask_keys__())

    if any(isinstance(graph, HighLevelGraph) for graph in graphs):
        graph = HighLevelGraph.merge(*graphs)
    else:
        graph = merge(*map(ensure_dict, graphs))

    return graph, keys


def unpack_collections(*args, traverse=True):
    """Extract collections in preparation for compute/persist/etc...

    Intended use is to find all collections in a set of (possibly nested)
    python objects, do something to them (compute, etc...), then repackage them
    in equivalent python objects.

    Parameters
    ----------
    *args
        Any number of objects. If it is a dask collection, it's extracted and
        added to the list of collections returned. By default, python builtin
        collections are also traversed to look for dask collections (for more
        information see the ``traverse`` keyword).
    traverse : bool, optional
        If True (default), builtin python collections are traversed looking for
        any dask collections they might contain.

    Returns
    -------
    collections : list
        A list of all dask collections contained in ``args``
    repack : callable
        A function to call on the transformed collections to repackage them as
        they were in the original ``args``.
    """

    collections = []
    repack_dsk = {}

    collections_token = uuid.uuid4().hex

    def _unpack(expr):
        if is_dask_collection(expr):
            tok = tokenize(expr)
            if tok not in repack_dsk:
                repack_dsk[tok] = (getitem, collections_token, len(collections))
                collections.append(expr)
            return tok

        tok = uuid.uuid4().hex
        if not traverse:
            tsk = quote(expr)
        else:
            # Treat iterators like lists
            typ = list if isinstance(expr, Iterator) else type(expr)
            if typ in (list, tuple, set):
                tsk = (typ, [_unpack(i) for i in expr])
            elif typ in (dict, OrderedDict):
                tsk = (typ, [[_unpack(k), _unpack(v)] for k, v in expr.items()])
            elif dataclasses.is_dataclass(expr) and not isinstance(expr, type):
                tsk = (
                    apply,
                    typ,
                    (),
                    (
                        dict,
                        [
                            [f.name, _unpack(getattr(expr, f.name))]
                            for f in dataclasses.fields(expr)
                        ],
                    ),
                )
            elif is_namedtuple_instance(expr):
                tsk = (typ, *[_unpack(i) for i in expr])
            else:
                return expr

        repack_dsk[tok] = tsk
        return tok

    out = uuid.uuid4().hex
    repack_dsk[out] = (tuple, [_unpack(i) for i in args])

    def repack(results):
        dsk = repack_dsk.copy()
        dsk[collections_token] = quote(results)
        return simple_get(dsk, out)

    return collections, repack


def optimize(*args, traverse=True, **kwargs):
    """Optimize several dask collections at once.

    Returns equivalent dask collections that all share the same merged and
    optimized underlying graph. This can be useful if converting multiple
    collections to delayed objects, or to manually apply the optimizations at
    strategic points.

    Note that in most cases you shouldn't need to call this method directly.

    Parameters
    ----------
    *args : objects
        Any number of objects. If a dask object, its graph is optimized and
        merged with all those of all other dask objects before returning an
        equivalent dask collection. Non-dask arguments are passed through
        unchanged.
    traverse : bool, optional
        By default dask traverses builtin python collections looking for dask
        objects passed to ``optimize``. For large collections this can be
        expensive. If none of the arguments contain any dask objects, set
        ``traverse=False`` to avoid doing this traversal.
    optimizations : list of callables, optional
        Additional optimization passes to perform.
    **kwargs
        Extra keyword arguments to forward to the optimization passes.

    Examples
    --------
    >>> import dask
    >>> import dask.array as da
    >>> a = da.arange(10, chunks=2).sum()
    >>> b = da.arange(10, chunks=2).mean()
    >>> a2, b2 = dask.optimize(a, b)

    >>> a2.compute() == a.compute()
    True
    >>> b2.compute() == b.compute()
    True
    """
    collections, repack = unpack_collections(*args, traverse=traverse)
    if not collections:
        return args

    dsk = collections_to_dsk(collections, **kwargs)

    postpersists = []
    for a in collections:
        r, s = a.__dask_postpersist__()
        postpersists.append(r(dsk, *s))

    return repack(postpersists)


def compute(
    *args, traverse=True, optimize_graph=True, scheduler=None, get=None, **kwargs
):
    """Compute several dask collections at once.

    Parameters
    ----------
    args : object
        Any number of objects. If it is a dask object, it's computed and the
        result is returned. By default, python builtin collections are also
        traversed to look for dask objects (for more information see the
        ``traverse`` keyword). Non-dask arguments are passed through unchanged.
    traverse : bool, optional
        By default dask traverses builtin python collections looking for dask
        objects passed to ``compute``. For large collections this can be
        expensive. If none of the arguments contain any dask objects, set
        ``traverse=False`` to avoid doing this traversal.
    scheduler : string, optional
        Which scheduler to use like "threads", "synchronous" or "processes".
        If not provided, the default is to check the global settings first,
        and then fall back to the collection defaults.
    optimize_graph : bool, optional
        If True [default], the optimizations for each collection are applied
        before computation. Otherwise the graph is run as is. This can be
        useful for debugging.
    get : ``None``
        Should be left to ``None`` The get= keyword has been removed.
    kwargs
        Extra keywords to forward to the scheduler function.

    Examples
    --------
    >>> import dask
    >>> import dask.array as da
    >>> a = da.arange(10, chunks=2).sum()
    >>> b = da.arange(10, chunks=2).mean()
    >>> dask.compute(a, b)
    (45, 4.5)

    By default, dask objects inside python collections will also be computed:

    >>> dask.compute({'a': a, 'b': b, 'c': 1})
    ({'a': 45, 'b': 4.5, 'c': 1},)
    """

    collections, repack = unpack_collections(*args, traverse=traverse)
    if not collections:
        return args

    schedule = get_scheduler(
        scheduler=scheduler,
        collections=collections,
        get=get,
    )

    dsk = collections_to_dsk(collections, optimize_graph, **kwargs)
    keys, postcomputes = [], []
    for x in collections:
        keys.append(x.__dask_keys__())
        postcomputes.append(x.__dask_postcompute__())

    results = schedule(dsk, keys, **kwargs)
    return repack([f(r, *a) for r, (f, a) in zip(results, postcomputes)])


def visualize(
    *args,
    filename="mydask",
    traverse=True,
    optimize_graph=False,
    maxval=None,
    engine: Literal["cytoscape", "ipycytoscape", "graphviz"] | None = None,
    **kwargs,
):
    """
    Visualize several dask graphs simultaneously.

    Requires ``graphviz`` to be installed. All options that are not the dask
    graph(s) should be passed as keyword arguments.

    Parameters
    ----------
    args : object
        Any number of objects. If it is a dask collection (for example, a
        dask DataFrame, Array, Bag, or Delayed), its associated graph
        will be included in the output of visualize. By default, python builtin
        collections are also traversed to look for dask objects (for more
        information see the ``traverse`` keyword). Arguments lacking an
        associated graph will be ignored.
    filename : str or None, optional
        The name of the file to write to disk. If the provided `filename`
        doesn't include an extension, '.png' will be used by default.
        If `filename` is None, no file will be written, and we communicate
        with dot using only pipes.
    format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
        Format in which to write output file.  Default is 'png'.
    traverse : bool, optional
        By default, dask traverses builtin python collections looking for dask
        objects passed to ``visualize``. For large collections this can be
        expensive. If none of the arguments contain any dask objects, set
        ``traverse=False`` to avoid doing this traversal.
    optimize_graph : bool, optional
        If True, the graph is optimized before rendering.  Otherwise,
        the graph is displayed as is. Default is False.
    color : {None, 'order', 'ages', 'freed', 'memoryincreases', 'memorydecreases', 'memorypressure'}, optional
        Options to color nodes. colormap:

        - None, the default, no colors.
        - 'order', colors the nodes' border based on the order they appear in the graph.
        - 'ages', how long the data of a node is held.
        - 'freed', the number of dependencies released after running a node.
        - 'memoryincreases', how many more outputs are held after the lifetime of a node.
          Large values may indicate nodes that should have run later.
        - 'memorydecreases', how many fewer outputs are held after the lifetime of a node.
          Large values may indicate nodes that should have run sooner.
        - 'memorypressure', the number of data held when the node is run (circle), or
          the data is released (rectangle).
    maxval : {int, float}, optional
        Maximum value for colormap to normalize form 0 to 1.0. Default is ``None``
        will make it the max number of values
    collapse_outputs : bool, optional
        Whether to collapse output boxes, which often have empty labels.
        Default is False.
    verbose : bool, optional
        Whether to label output and input boxes even if the data aren't chunked.
        Beware: these labels can get very long. Default is False.
    engine : {"graphviz", "ipycytoscape", "cytoscape"}, optional.
        The visualization engine to use. If not provided, this checks the dask config
        value "visualization.engine". If that is not set, it tries to import ``graphviz``
        and ``ipycytoscape``, using the first one to succeed.
    **kwargs
       Additional keyword arguments to forward to the visualization engine.

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
    dask.dot.dot_graph

    Notes
    -----
    For more information on optimization see here:

    https://docs.dask.org/en/latest/optimize.html
    """
    args, _ = unpack_collections(*args, traverse=traverse)

    dsk = dict(collections_to_dsk(args, optimize_graph=optimize_graph))

    color = kwargs.get("color")

    if color in {
        "order",
        "order-age",
        "order-freed",
        "order-memoryincreases",
        "order-memorydecreases",
        "order-memorypressure",
        "age",
        "freed",
        "memoryincreases",
        "memorydecreases",
        "memorypressure",
    }:
        import matplotlib.pyplot as plt

        from dask.order import diagnostics, order

        o = order(dsk)
        try:
            cmap = kwargs.pop("cmap")
        except KeyError:
            cmap = plt.cm.plasma
        if isinstance(cmap, str):
            import matplotlib.pyplot as plt

            cmap = getattr(plt.cm, cmap)

        def label(x):
            return str(values[x])

        data_values = None
        if color != "order":
            info = diagnostics(dsk, o)[0]
            if color.endswith("age"):
                values = {key: val.age for key, val in info.items()}
            elif color.endswith("freed"):
                values = {key: val.num_dependencies_freed for key, val in info.items()}
            elif color.endswith("memorypressure"):
                values = {key: val.num_data_when_run for key, val in info.items()}
                data_values = {
                    key: val.num_data_when_released for key, val in info.items()
                }
            elif color.endswith("memoryincreases"):
                values = {
                    key: max(0, val.num_data_when_released - val.num_data_when_run)
                    for key, val in info.items()
                }
            else:  # memorydecreases
                values = {
                    key: max(0, val.num_data_when_run - val.num_data_when_released)
                    for key, val in info.items()
                }

            if color.startswith("order-"):

                def label(x):
                    return str(o[x]) + "-" + str(values[x])

        else:
            values = o
        if maxval is None:
            maxval = max(1, max(values.values()))
        colors = {k: _colorize(cmap(v / maxval, bytes=True)) for k, v in values.items()}
        if data_values is None:
            data_values = values
            data_colors = colors
        else:
            data_colors = {
                k: _colorize(cmap(v / maxval, bytes=True))
                for k, v in data_values.items()
            }

        kwargs["function_attributes"] = {
            k: {"color": v, "label": label(k)} for k, v in colors.items()
        }
        kwargs["data_attributes"] = {k: {"color": v} for k, v in data_colors.items()}
    elif color:
        raise NotImplementedError("Unknown value color=%s" % color)

    # Determine which engine to dispatch to, first checking the kwarg, then config,
    # then whichever of graphviz or ipycytoscape are installed, in that order.
    engine = engine or config.get("visualization.engine", None)

    if not engine:
        try:
            import graphviz  # noqa: F401

            engine = "graphviz"
        except ImportError:
            try:
                import ipycytoscape  # noqa: F401

                engine = "cytoscape"
            except ImportError:
                pass

    if engine == "graphviz":
        from dask.dot import dot_graph

        return dot_graph(dsk, filename=filename, **kwargs)
    elif engine in ("cytoscape", "ipycytoscape"):
        from dask.dot import cytoscape_graph

        return cytoscape_graph(dsk, filename=filename, **kwargs)
    elif engine is None:
        raise RuntimeError(
            "No visualization engine detected, please install graphviz or ipycytoscape"
        )
    else:
        raise ValueError(f"Visualization engine {engine} not recognized")


def persist(*args, traverse=True, optimize_graph=True, scheduler=None, **kwargs):
    """Persist multiple Dask collections into memory

    This turns lazy Dask collections into Dask collections with the same
    metadata, but now with their results fully computed or actively computing
    in the background.

    For example a lazy dask.array built up from many lazy calls will now be a
    dask.array of the same shape, dtype, chunks, etc., but now with all of
    those previously lazy tasks either computed in memory as many small :class:`numpy.array`
    (in the single-machine case) or asynchronously running in the
    background on a cluster (in the distributed case).

    This function operates differently if a ``dask.distributed.Client`` exists
    and is connected to a distributed scheduler.  In this case this function
    will return as soon as the task graph has been submitted to the cluster,
    but before the computations have completed.  Computations will continue
    asynchronously in the background.  When using this function with the single
    machine scheduler it blocks until the computations have finished.

    When using Dask on a single machine you should ensure that the dataset fits
    entirely within memory.

    Examples
    --------
    >>> df = dd.read_csv('/path/to/*.csv')  # doctest: +SKIP
    >>> df = df[df.name == 'Alice']  # doctest: +SKIP
    >>> df['in-debt'] = df.balance < 0  # doctest: +SKIP
    >>> df = df.persist()  # triggers computation  # doctest: +SKIP

    >>> df.value().min()  # future computations are now fast  # doctest: +SKIP
    -10
    >>> df.value().max()  # doctest: +SKIP
    100

    >>> from dask import persist  # use persist function on multiple collections
    >>> a, b = persist(a, b)  # doctest: +SKIP

    Parameters
    ----------
    *args: Dask collections
    scheduler : string, optional
        Which scheduler to use like "threads", "synchronous" or "processes".
        If not provided, the default is to check the global settings first,
        and then fall back to the collection defaults.
    traverse : bool, optional
        By default dask traverses builtin python collections looking for dask
        objects passed to ``persist``. For large collections this can be
        expensive. If none of the arguments contain any dask objects, set
        ``traverse=False`` to avoid doing this traversal.
    optimize_graph : bool, optional
        If True [default], the graph is optimized before computation.
        Otherwise the graph is run as is. This can be useful for debugging.
    **kwargs
        Extra keywords to forward to the scheduler function.

    Returns
    -------
    New dask collections backed by in-memory data
    """
    collections, repack = unpack_collections(*args, traverse=traverse)
    if not collections:
        return args

    schedule = get_scheduler(scheduler=scheduler, collections=collections)

    if inspect.ismethod(schedule):
        try:
            from distributed.client import default_client
        except ImportError:
            pass
        else:
            try:
                client = default_client()
            except ValueError:
                pass
            else:
                if client.get == schedule:
                    results = client.persist(
                        collections, optimize_graph=optimize_graph, **kwargs
                    )
                    return repack(results)

    dsk = collections_to_dsk(collections, optimize_graph, **kwargs)
    keys, postpersists = [], []
    for a in collections:
        a_keys = list(flatten(a.__dask_keys__()))
        rebuild, state = a.__dask_postpersist__()
        keys.extend(a_keys)
        postpersists.append((rebuild, a_keys, state))

    results = schedule(dsk, keys, **kwargs)
    d = dict(zip(keys, results))
    results2 = [r({k: d[k] for k in ks}, *s) for r, ks, s in postpersists]
    return repack(results2)


############
# Tokenize #
############


class _HashFactory(Protocol):
    def __call__(
        self, string: ReadableBuffer = b"", *, usedforsecurity: bool = True
    ) -> hashlib._Hash:
        ...


# Pass `usedforsecurity=False` to support FIPS builds of Python
def _md5(x: ReadableBuffer, _hashlib_md5: _HashFactory = hashlib.md5) -> hashlib._Hash:
    return _hashlib_md5(x, usedforsecurity=False)


def tokenize(*args, **kwargs):
    """Deterministic token

    >>> tokenize([1, 2, '3'])
    '7d6a880cd9ec03506eee6973ff551339'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    hasher = _md5(str(tuple(map(normalize_token, args))).encode())
    if kwargs:
        hasher.update(str(normalize_token(kwargs)).encode())
    return hasher.hexdigest()


normalize_token = Dispatch()
normalize_token.register(
    (
        int,
        float,
        str,
        bytes,
        type(None),
        type,
        slice,
        complex,
        type(Ellipsis),
        datetime.date,
        datetime.time,
        datetime.datetime,
        datetime.timedelta,
        pathlib.PurePath,
    ),
    identity,
)


@normalize_token.register(dict)
def normalize_dict(d):
    return normalize_token(sorted(d.items(), key=str))


@normalize_token.register(OrderedDict)
def normalize_ordered_dict(d):
    return type(d).__name__, normalize_token(list(d.items()))


@normalize_token.register(set)
def normalize_set(s):
    return normalize_token(sorted(s, key=str))


def _normalize_seq_func(seq):
    # Defined outside normalize_seq to avoid unnecessary redefinitions and
    # therefore improving computation times.
    try:
        return list(map(normalize_token, seq))
    except RecursionError:
        if not config.get("tokenize.ensure-deterministic"):
            return uuid.uuid4().hex

        raise RuntimeError(
            f"Sequence {str(seq)} cannot be deterministically hashed. Please, see "
            "https://docs.dask.org/en/latest/custom-collections.html#implementing-deterministic-hashing "
            "for more information"
        )


@normalize_token.register((tuple, list))
def normalize_seq(seq):
    return type(seq).__name__, _normalize_seq_func(seq)


@normalize_token.register(literal)
def normalize_literal(lit):
    return "literal", normalize_token(lit())


@normalize_token.register(range)
def normalize_range(r):
    return list(map(normalize_token, [r.start, r.stop, r.step]))


@normalize_token.register(Enum)
def normalize_enum(e):
    return type(e).__name__, e.name, e.value


@normalize_token.register(object)
def normalize_object(o):
    method = getattr(o, "__dask_tokenize__", None)
    if method is not None:
        return method()

    if callable(o):
        return normalize_function(o)

    if dataclasses.is_dataclass(o):
        return normalize_dataclass(o)

    if not config.get("tokenize.ensure-deterministic"):
        return uuid.uuid4().hex

    raise RuntimeError(
        f"Object {str(o)} cannot be deterministically hashed. Please, see "
        "https://docs.dask.org/en/latest/custom-collections.html#implementing-deterministic-hashing "
        "for more information"
    )


function_cache: dict[Callable, Callable | tuple | str | bytes] = {}
function_cache_lock = threading.Lock()


def normalize_function(func: Callable) -> Callable | tuple | str | bytes:
    try:
        return function_cache[func]
    except KeyError:
        result = _normalize_function(func)
        if len(function_cache) >= 500:  # clear half of cache if full
            with function_cache_lock:
                if len(function_cache) >= 500:
                    for k in list(function_cache)[::2]:
                        del function_cache[k]
        function_cache[func] = result
        return result
    except TypeError:  # not hashable
        return _normalize_function(func)


def _normalize_function(func: Callable) -> tuple | str | bytes:
    if isinstance(func, Compose):
        first = getattr(func, "first", None)
        funcs = reversed((first,) + func.funcs) if first else func.funcs
        return tuple(normalize_function(f) for f in funcs)
    elif isinstance(func, (partial, curry)):
        args = tuple(normalize_token(i) for i in func.args)
        if func.keywords:
            kws = tuple(
                (k, normalize_token(v)) for k, v in sorted(func.keywords.items())
            )
        else:
            kws = None
        return (normalize_function(func.func), args, kws)
    else:
        try:
            result = pickle.dumps(func, protocol=4)
            if b"__main__" not in result:  # abort on dynamic functions
                return result
        except Exception:
            pass
        if not config.get("tokenize.ensure-deterministic"):
            try:
                import cloudpickle

                return cloudpickle.dumps(func, protocol=4)
            except Exception:
                return str(func)
        else:
            raise RuntimeError(
                f"Function {str(func)} may not be deterministically hashed by "
                "cloudpickle. See: https://github.com/cloudpipe/cloudpickle/issues/385 "
                "for more information."
            )


def normalize_dataclass(obj):
    fields = [
        (field.name, getattr(obj, field.name)) for field in dataclasses.fields(obj)
    ]
    return (
        normalize_function(type(obj)),
        _normalize_seq_func(fields),
    )


@normalize_token.register_lazy("pandas")
def register_pandas():
    import pandas as pd

    @normalize_token.register(pd.Index)
    def normalize_index(ind):
        values = ind.array
        return [ind.name, normalize_token(values)]

    @normalize_token.register(pd.MultiIndex)
    def normalize_index(ind):
        codes = ind.codes
        return (
            [ind.name]
            + [normalize_token(x) for x in ind.levels]
            + [normalize_token(x) for x in codes]
        )

    @normalize_token.register(pd.Categorical)
    def normalize_categorical(cat):
        return [normalize_token(cat.codes), normalize_token(cat.dtype)]

    @normalize_token.register(pd.arrays.PeriodArray)
    @normalize_token.register(pd.arrays.DatetimeArray)
    @normalize_token.register(pd.arrays.TimedeltaArray)
    def normalize_period_array(arr):
        return [normalize_token(arr.asi8), normalize_token(arr.dtype)]

    @normalize_token.register(pd.arrays.IntervalArray)
    def normalize_interval_array(arr):
        return [
            normalize_token(arr.left),
            normalize_token(arr.right),
            normalize_token(arr.closed),
        ]

    @normalize_token.register(pd.Series)
    def normalize_series(s):
        return [
            s.name,
            s.dtype,
            normalize_token(s._values),
            normalize_token(s.index),
        ]

    @normalize_token.register(pd.DataFrame)
    def normalize_dataframe(df):
        mgr = df._mgr
        data = list(mgr.arrays) + [df.columns, df.index]
        return list(map(normalize_token, data))

    @normalize_token.register(pd.api.extensions.ExtensionArray)
    def normalize_extension_array(arr):
        import numpy as np

        return normalize_token(np.asarray(arr))

    # Dtypes
    @normalize_token.register(pd.api.types.CategoricalDtype)
    def normalize_categorical_dtype(dtype):
        return [normalize_token(dtype.categories), normalize_token(dtype.ordered)]

    @normalize_token.register(pd.api.extensions.ExtensionDtype)
    def normalize_period_dtype(dtype):
        return normalize_token(dtype.name)


@normalize_token.register_lazy("numpy")
def register_numpy():
    import numpy as np

    @normalize_token.register(np.ndarray)
    def normalize_array(x):
        if not x.shape:
            return (x.item(), x.dtype)
        if hasattr(x, "mode") and getattr(x, "filename", None):
            if hasattr(x.base, "ctypes"):
                offset = (
                    x.ctypes._as_parameter_.value - x.base.ctypes._as_parameter_.value
                )
            else:
                offset = 0  # root memmap's have mmap object as base
            if hasattr(
                x, "offset"
            ):  # offset numpy used while opening, and not the offset to the beginning of file
                offset += x.offset
            return (
                x.filename,
                os.path.getmtime(x.filename),
                x.dtype,
                x.shape,
                x.strides,
                offset,
            )
        if x.dtype.hasobject:
            try:
                try:
                    # string fast-path
                    data = hash_buffer_hex(
                        "-".join(x.flat).encode(
                            encoding="utf-8", errors="surrogatepass"
                        )
                    )
                except UnicodeDecodeError:
                    # bytes fast-path
                    data = hash_buffer_hex(b"-".join(x.flat))
            except (TypeError, UnicodeDecodeError):
                try:
                    data = hash_buffer_hex(pickle.dumps(x, pickle.HIGHEST_PROTOCOL))
                except Exception:
                    # pickling not supported, use UUID4-based fallback
                    if not config.get("tokenize.ensure-deterministic"):
                        data = uuid.uuid4().hex
                    else:
                        raise RuntimeError(
                            f"``np.ndarray`` with object ``dtype`` {str(x)} cannot "
                            "be deterministically hashed. Please, see "
                            "https://docs.dask.org/en/latest/custom-collections.html#implementing-deterministic-hashing "  # noqa: E501
                            "for more information"
                        )
        else:
            try:
                data = hash_buffer_hex(x.ravel(order="K").view("i1"))
            except (BufferError, AttributeError, ValueError):
                data = hash_buffer_hex(x.copy().ravel(order="K").view("i1"))
        return (data, x.dtype, x.shape, x.strides)

    @normalize_token.register(np.matrix)
    def normalize_matrix(x):
        return type(x).__name__, normalize_array(x.view(type=np.ndarray))

    normalize_token.register(np.dtype, repr)
    normalize_token.register(np.generic, repr)

    @normalize_token.register(np.ufunc)
    def normalize_ufunc(x):
        try:
            name = x.__name__
            if getattr(np, name) is x:
                return "np." + name
        except AttributeError:
            return normalize_function(x)

    @normalize_token.register(np.random.BitGenerator)
    def normalize_bit_generator(bg):
        return normalize_token(bg.state)


@normalize_token.register_lazy("scipy")
def register_scipy():
    import scipy.sparse as sp

    def normalize_sparse_matrix(x, attrs):
        return (
            type(x).__name__,
            normalize_seq(normalize_token(getattr(x, key)) for key in attrs),
        )

    for cls, attrs in [
        (sp.dia_matrix, ("data", "offsets", "shape")),
        (sp.bsr_matrix, ("data", "indices", "indptr", "blocksize", "shape")),
        (sp.coo_matrix, ("data", "row", "col", "shape")),
        (sp.csr_matrix, ("data", "indices", "indptr", "shape")),
        (sp.csc_matrix, ("data", "indices", "indptr", "shape")),
        (sp.lil_matrix, ("data", "rows", "shape")),
    ]:
        normalize_token.register(cls, partial(normalize_sparse_matrix, attrs=attrs))

    @normalize_token.register(sp.dok_matrix)
    def normalize_dok_matrix(x):
        return type(x).__name__, normalize_token(sorted(x.items()))


def _colorize(t):
    """Convert (r, g, b) triple to "#RRGGBB" string

    For use with ``visualize(color=...)``

    Examples
    --------
    >>> _colorize((255, 255, 255))
    '#FFFFFF'
    >>> _colorize((0, 32, 128))
    '#002080'
    """
    t = t[:3]
    i = sum(v * 256 ** (len(t) - i - 1) for i, v in enumerate(t))
    h = hex(int(i))[2:].upper()
    h = "0" * (6 - len(h)) + h
    return "#" + h


named_schedulers: dict[str, SchedulerGetCallable] = {
    "sync": local.get_sync,
    "synchronous": local.get_sync,
    "single-threaded": local.get_sync,
}

if not EMSCRIPTEN:
    from dask import threaded

    named_schedulers.update(
        {
            "threads": threaded.get,
            "threading": threaded.get,
        }
    )

    from dask import multiprocessing as dask_multiprocessing

    named_schedulers.update(
        {
            "processes": dask_multiprocessing.get,
            "multiprocessing": dask_multiprocessing.get,
        }
    )


get_err_msg = """
The get= keyword has been removed.

Please use the scheduler= keyword instead with the name of
the desired scheduler like 'threads' or 'processes'

    x.compute(scheduler='single-threaded')
    x.compute(scheduler='threads')
    x.compute(scheduler='processes')

or with a function that takes the graph and keys

    x.compute(scheduler=my_scheduler_function)

or with a Dask client

    x.compute(scheduler=client)
""".strip()


def get_scheduler(get=None, scheduler=None, collections=None, cls=None):
    """Get scheduler function

    There are various ways to specify the scheduler to use:

    1.  Passing in scheduler= parameters
    2.  Passing these into global configuration
    3.  Using a dask.distributed default Client
    4.  Using defaults of a dask collection

    This function centralizes the logic to determine the right scheduler to use
    from those many options
    """
    if get:
        raise TypeError(get_err_msg)

    if scheduler is not None:
        if callable(scheduler):
            return scheduler
        elif "Client" in type(scheduler).__name__ and hasattr(scheduler, "get"):
            return scheduler.get
        elif isinstance(scheduler, str):
            scheduler = scheduler.lower()

            try:
                from distributed import Client

                Client.current(allow_global=True)
                client_available = True
            except (ImportError, ValueError):
                client_available = False
            if scheduler in named_schedulers:
                if client_available:
                    warnings.warn(
                        "Running on a single-machine scheduler when a distributed client "
                        "is active might lead to unexpected results."
                    )
                return named_schedulers[scheduler]
            elif scheduler in ("dask.distributed", "distributed"):
                if not client_available:
                    raise RuntimeError(
                        f"Requested {scheduler} scheduler but no Client active."
                    )
                from distributed.worker import get_client

                return get_client().get
            else:
                raise ValueError(
                    "Expected one of [distributed, %s]"
                    % ", ".join(sorted(named_schedulers))
                )
        elif isinstance(scheduler, Executor):
            # Get `num_workers` from `Executor`'s `_max_workers` attribute.
            # If undefined, fallback to `config` or worst case CPU_COUNT.
            num_workers = getattr(scheduler, "_max_workers", None)
            if num_workers is None:
                num_workers = config.get("num_workers", CPU_COUNT)
            assert isinstance(num_workers, Integral) and num_workers > 0
            return partial(local.get_async, scheduler.submit, num_workers)
        else:
            raise ValueError("Unexpected scheduler: %s" % repr(scheduler))
        # else:  # try to connect to remote scheduler with this name
        #     return get_client(scheduler).get

    if config.get("scheduler", None):
        return get_scheduler(scheduler=config.get("scheduler", None))

    if config.get("get", None):
        raise ValueError(get_err_msg)

    try:
        from distributed import get_client

        return get_client().get
    except (ImportError, ValueError):
        pass

    if cls is not None:
        return cls.__dask_scheduler__

    if collections:
        collections = [c for c in collections if c is not None]
    if collections:
        get = collections[0].__dask_scheduler__
        if not all(c.__dask_scheduler__ == get for c in collections):
            raise ValueError(
                "Compute called on multiple collections with "
                "differing default schedulers. Please specify a "
                "scheduler=` parameter explicitly in compute or "
                "globally with `dask.config.set`."
            )
        return get

    return None


def wait(x, timeout=None, return_when="ALL_COMPLETED"):
    """Wait until computation has finished

    This is a compatibility alias for ``dask.distributed.wait``.
    If it is applied onto Dask collections without Dask Futures or if Dask
    distributed is not installed then it is a no-op
    """
    try:
        from distributed import wait

        return wait(x, timeout=timeout, return_when=return_when)
    except (ImportError, ValueError):
        return x


def get_collection_names(collection) -> set[str]:
    """Infer the collection names from the dask keys, under the assumption that all keys
    are either tuples with matching first element, and that element is a string, or
    there is exactly one key and it is a string.

    Examples
    --------
    >>> a.__dask_keys__()  # doctest: +SKIP
    ["foo", "bar"]
    >>> get_collection_names(a)  # doctest: +SKIP
    {"foo", "bar"}
    >>> b.__dask_keys__()  # doctest: +SKIP
    [[("foo-123", 0, 0), ("foo-123", 0, 1)], [("foo-123", 1, 0), ("foo-123", 1, 1)]]
    >>> get_collection_names(b)  # doctest: +SKIP
    {"foo-123"}
    """
    if not is_dask_collection(collection):
        raise TypeError(f"Expected Dask collection; got {type(collection)}")
    return {get_name_from_key(k) for k in flatten(collection.__dask_keys__())}


def get_name_from_key(key) -> str:
    """Given a dask collection's key, extract the collection name.

    Parameters
    ----------
    key: string or tuple
        Dask collection's key, which must be either a single string or a tuple whose
        first element is a string (commonly referred to as a collection's 'name'),

    Examples
    --------
    >>> get_name_from_key("foo")
    'foo'
    >>> get_name_from_key(("foo-123", 1, 2))
    'foo-123'
    """
    if isinstance(key, tuple) and key and isinstance(key[0], str):
        return key[0]
    if isinstance(key, str):
        return key
    raise TypeError(f"Expected str or tuple[str, Hashable, ...]; got {key}")


def replace_name_in_key(key, rename: Mapping[str, str]):
    """Given a dask collection's key, replace the collection name with a new one.

    Parameters
    ----------
    key: string or tuple
        Dask collection's key, which must be either a single string or a tuple whose
        first element is a string (commonly referred to as a collection's 'name'),
    rename:
        Mapping of zero or more names from : to. Extraneous names will be ignored.
        Names not found in this mapping won't be replaced.

    Examples
    --------
    >>> replace_name_in_key("foo", {})
    'foo'
    >>> replace_name_in_key("foo", {"foo": "bar"})
    'bar'
    >>> replace_name_in_key(("foo-123", 1, 2), {"foo-123": "bar-456"})
    ('bar-456', 1, 2)
    """
    if isinstance(key, tuple) and key and isinstance(key[0], str):
        return (rename.get(key[0], key[0]),) + key[1:]
    if isinstance(key, str):
        return rename.get(key, key)
    raise TypeError(f"Expected str or tuple[str, Hashable, ...]; got {key}")


def clone_key(key, seed):
    """Clone a key from a Dask collection, producing a new key with the same prefix and
    indices and a token which is a deterministic function of the previous key and seed.

    Examples
    --------
    >>> clone_key("x", 123)
    'x-dc2b8d1c184c72c19faa81c797f8c6b0'
    >>> clone_key("inc-cbb1eca3bafafbb3e8b2419c4eebb387", 123)
    'inc-f81b5a88038a2132882aa29a9fcfec06'
    >>> clone_key(("sum-cbb1eca3bafafbb3e8b2419c4eebb387", 4, 3), 123)
    ('sum-fd6be9e9fe07fc232ad576fa997255e8', 4, 3)
    """
    if isinstance(key, tuple) and key and isinstance(key[0], str):
        return (clone_key(key[0], seed),) + key[1:]
    if isinstance(key, str):
        prefix = key_split(key)
        return prefix + "-" + tokenize(key, seed)
    raise TypeError(f"Expected str or tuple[str, Hashable, ...]; got {key}")

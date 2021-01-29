"""Tools to modify already existing dask graphs. Unlike in :mod:`dask.optimization`, the
output collections produced by this module are typically not functionally equivalent to
their inputs.
"""
import uuid
from typing import Callable, Hashable, Optional, Set, Tuple, TypeVar

from .array import Array
from .bag import Bag
from .base import (
    clone_key,
    get_collection_name,
    replace_name_in_key,
    tokenize,
    unpack_collections,
)
from .blockwise import blockwise
from .core import flatten
from .dataframe import DataFrame, Series
from .delayed import Delayed, delayed
from .highlevelgraph import BasicLayer, HighLevelGraph, Layer
from .utils import ensure_dict

__all__ = ("bind", "checkpoint", "clone", "drop")

T = TypeVar("T")


def drop(*collections) -> Delayed:
    """Build a :doc:`delayed` which waits until all chunks of the input collection(s)
    have been computed before returning None.

    Arguments
    ---------
    collections
        Zero or more Dask collections or nested data structures containing zero or more
        collections

    Returns
    -------
    :doc:`delayed` yielding None
    """
    collections, _ = unpack_collections(*collections)
    if len(collections) != 1:
        return delayed(chunks.drop)(*(drop(c) for c in collections))
    (collection,) = collections

    tok = tokenize(collection)
    name = "drop-" + tok
    map_name = "drop_map-" + tok

    keys_iter = flatten(collection.__dask_keys__())
    try:
        next(keys_iter)
        next(keys_iter)
    except StopIteration:
        # Collection has 0 or 1 keys; no need for a map step
        layer = {name: (chunks.drop, collection.__dask_keys__())}
        dsk = HighLevelGraph.from_collections(name, layer, dependencies=(collection,))
    else:
        # Collection has 2+ keys; apply a two-step map->reduce algorithm so that we
        # transfer over the network and store in RAM only a handful of None's instead of
        # the full computed collection's contents
        map_layer = _build_map_layer(chunks.drop, map_name, collection)
        map_dsk = HighLevelGraph.from_collections(
            map_name, map_layer, dependencies=(collection,)
        )
        reduce_layer = {name: (chunks.drop, list(map_layer.get_output_keys()))}
        reduce_dsk = HighLevelGraph(
            {name: reduce_layer}, dependencies={name: {map_name}}
        )
        dsk = HighLevelGraph.merge(map_dsk, reduce_dsk)

    return Delayed(name, dsk)


def _build_map_layer(
    func: Callable, name: str, collection, dependencies: Tuple[Delayed, ...] = ()
) -> Layer:
    """Apply func to all keys of collection. Create a Blockwise layer whenever possible;
    fall back to BasicLayer otherwise.

    Arguments
    ---------
    func
        Callable to be invoked on the graph node
    name : str
        Layer name
    collection
        Arbitrary dask collection
    dependencies
        Zero or more Delayed objects, which will be passed as arbitrary variadic args to
        func after the collection's chunk
    """
    if isinstance(collection, (Array, DataFrame, Series, Bag)):
        # Use a Blockwise layer
        if isinstance(collection, Array):
            numblocks = collection.numblocks
        else:
            numblocks = (collection.npartitions,)
        ndim = getattr(collection, "ndim", 1)
        indices = tuple(range(ndim))
        kwargs = {"_deps": dependencies} if dependencies else {}
        prev_name = get_collection_name(collection)
        return blockwise(
            func,
            name,
            indices,
            prev_name,
            indices,
            numblocks={prev_name: numblocks},
            dependencies=dependencies,
            **kwargs,
        )
    else:
        # Delayed, bag.Item, dataframe.core.Scalar, or third-party collection;
        # fall back to BasicLayer
        dep_keys = tuple(d.key for d in dependencies)
        return BasicLayer(
            {
                replace_name_in_key(k, name): (func, k) + dep_keys
                for k in flatten(collection.__dask_keys__())
            }
        )


def bind(
    children: T,
    parents,
    *,
    omit=None,
    seed: Hashable = None,
    assume_layers: bool = True
) -> T:
    """
    Make ``children`` collection(s), optionally omitting sub-collections, dependent on
    ``parents`` collection(s). Two examples follow.

    The first example creates an array ``b2`` whose computation first computes an array
    ``a`` completely and then computes ``b`` completely, recomputing ``a`` in the
    process:

    >>> from dask import array as da
    >>> a = da.ones(4, chunks=2)
    >>> b = a + 1
    >>> b2 = bind(b, a)
    >>> len(b2.dask)
    11
    >>> b2.compute()
    array([2., 2., 2., 2.])

    The second example creates arrays ``b3`` and ``c3``, whose computation first
    computes an array ``a`` and then computes the additions, this time not
    recomputing ``a`` in the process:

    >>> c = a + 2
    >>> b3, c3 = bind((b, c), a, omit=a)
    >>> len(b3.dask), len(c3.dask)
    (9, 9)
    >>> dask.compute(b3, c3)
    (array([2., 2., 2., 2.]), array([3., 3., 3., 3.]))

    Arguments
    ---------
    children
        Dask collection or nested structure of Dask collections
    parents
        Dask collection or nested structure of Dask collections
    omit
        Dask collection or nested structure of Dask collections
    seed
        Hashable used to seed the key regeneration. Omit to default to a random number
        that will produce different keys at every call.
    assume_layers
        True
            Use a fast algorithm that works at layer level, which assumes that all
            collections in ``children`` and ``omit``

            #. use :class:`~dask.highlevelgraph.HighLevelGraph`,
            #. define the ``__dask_layers__()`` method, and
            #. never had their graphs squashed and rebuilt between the creation of the
               ``omit`` collections and the ``children`` collections; in other words if
               the keys of the ``omit`` collections can be found among the keys of the
               ``children`` collections, then the same must also hold true for the
               layers.
        False
            Use a slower algorithm that works at keys level, which makes none of the
            above assumptions.

    Returns
    -------
    Dask collection or structure of dask collection equivalent as ``children``, which
    compute to the same values.
    All keys of ``children`` will be regenerated, up to and excluding the keys of
    ``omit``. Non-constant nodes immediately above ``omit``, or the leaf non-constant
    nodes if the collections in ``omit`` are not found, are prevented from computing
    until all collections in ``parents`` have been fully computed.
    """
    if seed is None:
        seed = uuid.uuid4().bytes

    # parents=None is a special case invoked by the one-liner wrapper clone() below
    blocker = drop(parents) if parents is not None else None

    omit, _ = unpack_collections(omit)
    if assume_layers:
        # Set of all the top-level layers of the collections in omit
        omit_layers = {layer for coll in omit for layer in coll.__dask_layers__()}
        omit_keys = set()
    else:
        omit_layers = set()
        # Set of *all* the keys, not just the top-level ones, of the collections in omit
        omit_keys = {key for coll in omit for key in coll.__dask_graph__()}

    unpacked_children, repack = unpack_collections(children)
    return repack(
        [
            _bind_one(child, blocker, omit_layers, omit_keys, seed)
            for child in unpacked_children
        ]
    )[0]


def _bind_one(
    child: T,
    blocker: Optional[Delayed],
    omit_layers: Set[str],
    omit_keys: Set[Hashable],
    seed: Hashable,
) -> T:
    try:
        prev_coll_name = get_collection_name(child)
    except KeyError:
        # Collection with no keys; this is a legitimate use case but, at the moment of
        # writing, can only happen with third-party collections
        return child

    dsk = child.__dask_graph__()
    new_layers = {}
    new_deps = {}
    if isinstance(dsk, HighLevelGraph):
        prev_layers = dsk.layers
        prev_deps = dsk.dependencies
        all_keys = dsk.get_all_external_keys()
    else:
        dsk = ensure_dict(dsk)
        prev_layers = {prev_coll_name: dsk}
        prev_deps = {prev_coll_name: set()}
        all_keys = dsk.keys()

    clone_keys = all_keys - omit_keys
    for layer_name in omit_layers:
        try:
            layer = prev_layers[layer_name]
        except KeyError:
            continue
        clone_keys -= layer.get_output_keys()
    # Note: when assume_layers=True, clone_keys can contain keys of the omit collections
    # that are not top-level. This is OK, as they will never be encountered inside the
    # values of their dependent layers.

    if blocker is not None:
        blocker_key = blocker.key
        blocker_dsk = blocker.__dask_graph__()
        assert isinstance(blocker_dsk, HighLevelGraph)
        new_layers.update(blocker_dsk.layers)
        new_deps.update(blocker_dsk.dependencies)
    else:
        blocker_key = None

    try:
        layers_to_clone = set(child.__dask_layers__())
    except AttributeError:
        layers_to_clone = {prev_coll_name}
    layers_to_copy_verbatim = set()

    while layers_to_clone:
        prev_layer_name = layers_to_clone.pop()
        new_layer_name = clone_key(prev_layer_name, seed=seed)
        if new_layer_name in new_layers:
            continue

        layer = prev_layers[prev_layer_name]
        layer_deps = prev_deps[prev_layer_name]
        layer_deps_to_clone = layer_deps - omit_layers
        layer_deps_to_omit = layer_deps & omit_layers
        layers_to_clone |= layer_deps_to_clone
        layers_to_copy_verbatim |= layer_deps_to_omit

        new_layers[new_layer_name], is_bound = layer.clone(
            keys=clone_keys, seed=seed, bind_to=blocker_key
        )
        new_deps[new_layer_name] = {
            clone_key(dep, seed=seed) for dep in layer_deps_to_clone
        } | layer_deps_to_omit
        if is_bound:
            new_deps[new_layer_name].add(blocker_key)

    # Add the layers of the collections from omit from child.dsk. Note that, when
    # assume_layers=False, it would be unsafe to simply do HighLevelGraph.merge(dsk,
    # omit[i].dsk). Also, collections in omit may or may not be parents of this specific
    # child, or of any children at all.
    while layers_to_copy_verbatim:
        layer_name = layers_to_copy_verbatim.pop()
        if layer_name in new_layers:
            continue
        layer_deps = prev_deps[layer_name]
        layers_to_copy_verbatim |= layer_deps
        new_deps[layer_name] = layer_deps
        new_layers[layer_name] = prev_layers[layer_name]

    rebuild, args = child.__dask_postpersist__()
    return rebuild(
        HighLevelGraph(new_layers, new_deps),
        *args,
        name=clone_key(prev_coll_name, seed),
    )


def clone(*collections, omit=None, seed: Hashable = None, assume_layers: bool = True):
    """Clone dask collections, returning equivalent collections that are generated from
    independent calculations.

    Example
    ------
    (tokens have been simplified for the sake of brevity)

    >>> from dask import array as da
    >>> x_i = da.asarray([1, 1, 1, 1], chunks=2)
    >>> y_i = x_i + 1
    >>> z_i = y_i + 2
    >>> dict(z_i.dask)
    {('array-1', 0): array([1, 1]),
     ('array-1', 1): array([1, 1]),
     ('add-2', 0): (<function operator.add>, ('array-1', 0), 1),
     ('add-2', 1): (<function operator.add>, ('array-1', 1), 1),
     ('add-3', 0): (<function operator.add>, ('add-2', 0), 1),
     ('add-3', 1): (<function operator.add>, ('add-2', 1), 1)}
    >>> w_i = clone(z_i, omit=x_i)
    >>> w_i.compute()
    array([4., 4., 4., 4.])
    >>> dict(w_i.dask)
    {('array-1', 0): array([1, 1]),
     ('array-1', 1): array([1, 1]),
     ('add-4', 0): (<function operator.add>, ('array-1', 0), 1),
     ('add-4', 1): (<function operator.add>, ('array-1', 1), 1),
     ('add-5', 0): (<function operator.add>, ('add-4', 0), 1),
     ('add-5', 1): (<function operator.add>, ('add-4', 1), 1)}

    Arguments
    ---------
    collections
        One or more Dask collections or nested structures of Dask collections
    omit
        Dask collection or nested structure of Dask collections which will not be cloned
    seed
        See :func:`bind`
    assume_layers
        See :func:`bind`

    Returns
    -------
    Dask collections of the same type as the inputs, which compute to the same value, or
    nested structures equivalent to the inputs, where the original collections have been
    replaced.
    """
    out = bind(
        collections, parents=None, omit=omit, seed=seed, assume_layers=assume_layers
    )
    return out[0] if len(collections) == 1 else out


def checkpoint(*collections):
    """Ensure that all chunks of all input collections have been computed before
    continuing.

    The following example creates a dask array ``u`` that, when used in a computation,
    will only proceed when all chunks of the array ``x`` have been computed, but
    otherwise matches ``x``:

    >>> from dask import array as da
    >>> x = da.ones(10, chunks=5)
    >>> u = checkpoint(x)

    The following example will create two arrays ``u`` and ``v`` that, when used in a
    computation, will only proceed when all chunks of the arrays ``x`` and ``y`` have
    been computed but otherwise match ``x`` and ``y``:

    >>> x = da.ones(10, chunks=5)
    >>> y = da.zeros(10, chunks=5)
    >>> u, v = checkpoint(x, y)

    Arguments
    ---------
    collections
        Zero or more Dask collections or nested structures of Dask collections

    Returns
    -------
    Dask collection of the same type as the input, which computes to the same value, or
    a nested structure equivalent to the input where the original collections have been
    replaced.
    """
    blocker = drop(*collections)

    def _checkpoint_one(coll):
        name = "checkpoint-" + tokenize(coll, blocker)
        layer = _build_map_layer(chunks.bind, name, coll, dependencies=(blocker,))
        dsk = HighLevelGraph.from_collections(name, layer, dependencies=(coll, blocker))
        rebuild, args = coll.__dask_postpersist__()
        return rebuild(dsk, *args, name=name)

    unpacked, repack = unpack_collections(*collections)
    out = repack([_checkpoint_one(coll) for coll in unpacked])
    return out[0] if len(collections) == 1 else out


class chunks:
    """Callables to be inserted in the Dask graph"""

    @staticmethod
    def bind(node: T, *args, **kwargs) -> T:
        """Dummy graph node of :func:`bind` and :func:`checkpoint`.
        Wait for both node and all variadic args to complete; then return node.
        """
        return node

    @staticmethod
    def drop(*args, **kwargs) -> None:
        """Dummy graph node of :func:`drop`.
        Wait for all variadic args to complete; then return None.
        """
        pass

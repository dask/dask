import abc
import collections.abc
import warnings
from typing import (
    AbstractSet,
    Any,
    Dict,
    Hashable,
    MutableMapping,
    Optional,
    Mapping,
    Iterable,
    Tuple,
)
import copy

import tlz as toolz

from . import config
from .utils import ensure_dict, ignoring, stringify
from .base import clone_key, flatten, is_dask_collection
from .core import reverse_dict, keys_in_tasks
from .utils_test import add, inc  # noqa: F401


def compute_layer_dependencies(layers):
    """Returns the dependencies between layers"""

    def _find_layer_containing_key(key):
        for k, v in layers.items():
            if key in v:
                return k
        raise RuntimeError(f"{repr(key)} not found")

    all_keys = set(key for layer in layers.values() for key in layer)
    ret = {k: set() for k in layers}
    for k, v in layers.items():
        for key in keys_in_tasks(all_keys - v.keys(), v.values()):
            ret[k].add(_find_layer_containing_key(key))
    return ret


class Layer(collections.abc.Mapping):
    """High level graph layer

    This abstract class establish a protocol for high level graph layers.

    The main motivation of a layer is to represent a collection of tasks
    symbolically in order to speedup a series of operations significantly.
    Ideally, a layer should stay in this symbolic state until execution
    but in practice some operations will force the layer to generate all
    its internal tasks. We say that the layer has been materialized.

    Most of the default implementations in this class will materialize the
    layer. It is up to derived classes to implement non-materializing
    implementations.
    """

    annotations: Optional[Mapping[str, Any]]

    def __init__(self, annotations: Mapping[str, Any] = None):
        if annotations:
            self.annotations = annotations
        else:
            self.annotations = copy.copy(config.get("annotations", None))

    @abc.abstractmethod
    def is_materialized(self) -> bool:
        """Return whether the layer is materialized or not"""
        return True

    def get_output_keys(self) -> AbstractSet:
        """Return a set of all output keys

        Output keys are all keys in the layer that might be referenced by
        other layers.

        Classes overriding this implementation should not cause the layer
        to be materialized.

        Returns
        -------
        keys: AbstractSet
            All output keys
        """
        return self.keys()

    def cull(
        self, keys: set, all_hlg_keys: Iterable
    ) -> Tuple["Layer", Mapping[Hashable, set]]:
        """Return a new Layer with only the tasks required to calculate `keys` and
        a map of external key dependencies.

        In other words, remove unnecessary tasks from the layer.

        Examples
        --------
        >>> d = Layer({'x': 1, 'y': (inc, 'x'), 'out': (add, 'x', 10)})  # doctest: +SKIP
        >>> d.cull({'out'})  # doctest: +SKIP
        {'x': 1, 'out': (add, 'x', 10)}

        Returns
        -------
        layer: Layer
            Culled layer
        deps: Map
            Map of external key dependencies
        """

        if len(keys) == len(self):
            # Nothing to cull if preserving all existing keys
            return (
                self,
                {k: self.get_dependencies(k, all_hlg_keys) for k in self.keys()},
            )

        ret_deps = {}
        seen = set()
        out = {}
        work = keys.copy()
        while work:
            k = work.pop()
            out[k] = self[k]
            ret_deps[k] = self.get_dependencies(k, all_hlg_keys)
            for d in ret_deps[k]:
                if d not in seen:
                    if d in self:
                        seen.add(d)
                        work.add(d)

        return MaterializedLayer(out), ret_deps

    def get_dependencies(self, key: Hashable, all_hlg_keys: Iterable) -> set:
        """Get dependencies of `key` in the layer

        Parameters
        ----------
        key: Hashable
            The key to find dependencies of
        all_hlg_keys: Iterable
            All keys in the high level graph.

        Returns
        -------
        deps: set
            A set of dependencies
        """
        return keys_in_tasks(all_hlg_keys, [self[key]])

    def __dask_distributed_anno_pack__(self) -> Optional[Mapping[str, Any]]:
        """Packs Layer annotations for transmission to scheduler

        Callables annotations are fully expanded over Layer keys, while
        other values are simply transmitted as is

        Returns
        -------
        packed_annotations : dict
            Packed annotations.
        """
        if self.annotations is None:
            return None

        packed = {}

        for a, v in self.annotations.items():
            if callable(v):
                packed[a] = {stringify(k): v(k) for k in self}
                packed[a]["__expanded_annotations__"] = True
            else:
                packed[a] = v

        return packed

    @staticmethod
    def __dask_distributed_annotations_unpack__(
        annotations: MutableMapping[str, Any],
        new_annotations: Optional[Mapping[str, Any]],
        keys: Iterable[Hashable],
    ) -> None:
        """
        Unpack a set of layer annotations across a set of keys, then merge those
        expanded annotations for the layer into an existing annotations mapping.

        This is not a simple shallow merge because some annotations like retries,
        priority, workers, etc need to be able to retain keys from different layers.

        Parameters
        ----------
        annotations: MutableMapping[str, Any], input/output
            Already unpacked annotations, which are to be updated with the new
            unpacked annotations
        new_annotations: Mapping[str, Any], optional
            New annotations to be unpacked into `annotations`
        keys: Iterable
            All keys in the layer.
        """
        if new_annotations is None:
            return

        expanded = {}
        keys_stringified = False

        # Expand the new annotations across the keyset
        for a, v in new_annotations.items():
            if type(v) is dict and "__expanded_annotations__" in v:
                # Maybe do a destructive update for efficiency?
                v = v.copy()
                del v["__expanded_annotations__"]
                expanded[a] = v
            else:
                if not keys_stringified:
                    keys = [stringify(k) for k in keys]
                    keys_stringified = True

                expanded[a] = dict.fromkeys(keys, v)

        # Merge the expanded annotations with the existing annotations mapping
        for k, v in expanded.items():
            v.update(annotations.get(k, {}))
        annotations.update(expanded)

    def clone(
        self,
        keys: set,
        seed: Hashable,
        bind_to: Hashable = None,
    ) -> "tuple[Layer, bool]":
        """Clone selected keys in the layer, as well as references to keys in other
        layers

        Parameters
        ----------
        keys
            Keys to be replaced. This never includes keys not listed by
            :meth:`get_output_keys`. It must also include any keys that are outside
            of this layer that may be referenced by it.
        seed
            Common hashable used to alter the keys; see :func:`dask.base.clone_key`
        bind_to
            Optional key to bind the leaf nodes to. A leaf node here is one that does
            not reference any replaced keys; in other words it's a node where the
            replacement graph traversal stops; it may still have dependencies on
            non-replaced nodes.
            A bound node will not be computed until after ``bind_to`` has been computed.

        Returns
        -------
        - New layer
        - True if the ``bind_to`` key was injected anywhere; False otherwise

        Notes
        -----
        This method should be overridden by subclasses to avoid materializing the layer.
        """
        from .graph_manipulation import chunks

        is_leaf: bool

        def clone_value(o):
            """Variant of distributed.utils_comm.subs_multiple, which allows injecting
            bind_to
            """
            nonlocal is_leaf

            typ = type(o)
            if typ is tuple and o and callable(o[0]):
                return (o[0],) + tuple(clone_value(i) for i in o[1:])
            elif typ is list:
                return [clone_value(i) for i in o]
            elif typ is dict:
                return {k: clone_value(v) for k, v in o.items()}
            else:
                try:
                    if o not in keys:
                        return o
                except TypeError:
                    return o
                is_leaf = False
                return clone_key(o, seed)

        dsk_new = {}
        bound = False

        for key, value in self.items():
            if key in keys:
                key = clone_key(key, seed)
                is_leaf = True
                value = clone_value(value)
                if bind_to is not None and is_leaf:
                    value = (chunks.bind, value, bind_to)
                    bound = True

            dsk_new[key] = value

        return MaterializedLayer(dsk_new), bound

    def __dask_distributed_pack__(
        self,
        all_hlg_keys: Iterable[Hashable],
        known_key_dependencies: Mapping[Hashable, set],
        client,
        client_keys: Iterable[Hashable],
    ) -> Any:
        """Pack the layer for scheduler communication in Distributed

        This method should pack its current state and is called by the Client when
        communicating with the Scheduler.
        The Scheduler will then use .__dask_distributed_unpack__(data, ...) to unpack
        the state, materialize the layer, and merge it into the global task graph.

        The returned state must be compatible with Distributed's scheduler, which
        means it must obey the following:
          - Serializable by msgpack (notice, msgpack converts lists to tuples)
          - All remote data must be unpacked (see unpack_remotedata())
          - All keys must be converted to strings now or when unpacking
          - All tasks must be serialized (see dumps_task())

        The default implementation materialize the layer thus layers such as Blockwise
        and ShuffleLayer should implement a specialized pack and unpack function in
        order to avoid materialization.

        Parameters
        ----------
        all_hlg_keys: Iterable[Hashable]
            All keys in the high level graph
        known_key_dependencies: Mapping[Hashable, set]
            Already known dependencies
        client: distributed.Client
            The client calling this function.
        client_keys : Iterable[Hashable]
            List of keys requested by the client.

        Returns
        -------
        state: Object serializable by msgpack
            Scheduler compatible state of the layer
        """
        from distributed.client import Future
        from distributed.utils_comm import unpack_remotedata, subs_multiple
        from distributed.worker import dumps_task
        from distributed.utils import CancelledError

        dsk = dict(self)

        # Find aliases not in `client_keys` and substitute all matching keys
        # with its Future
        values = {
            k: v
            for k, v in dsk.items()
            if isinstance(v, Future) and k not in client_keys
        }
        if values:
            dsk = subs_multiple(dsk, values)

        # Unpack remote data and record its dependencies
        dsk = {k: unpack_remotedata(v, byte_keys=True) for k, v in dsk.items()}
        unpacked_futures = set.union(*[v[1] for v in dsk.values()]) if dsk else set()
        for future in unpacked_futures:
            if future.client is not client:
                raise ValueError(
                    "Inputs contain futures that were created by another client."
                )
            if stringify(future.key) not in client.futures:
                raise CancelledError(stringify(future.key))
        unpacked_futures_deps = {}
        for k, v in dsk.items():
            if len(v[1]):
                unpacked_futures_deps[k] = {f.key for f in v[1]}
        dsk = {k: v[0] for k, v in dsk.items()}

        # Calculate dependencies without re-calculating already known dependencies
        missing_keys = dsk.keys() - known_key_dependencies.keys()
        dependencies = {
            k: keys_in_tasks(all_hlg_keys, [dsk[k]], as_list=False)
            for k in missing_keys
        }
        for k, v in unpacked_futures_deps.items():
            dependencies[k] = set(dependencies.get(k, ())) | v

        # The scheduler expect all keys to be strings
        dependencies = {
            stringify(k): {stringify(dep) for dep in deps}
            for k, deps in dependencies.items()
        }

        merged_hlg_keys = all_hlg_keys | dsk.keys()
        dsk = {
            stringify(k): stringify(v, exclusive=merged_hlg_keys)
            for k, v in dsk.items()
        }
        dsk = toolz.valmap(dumps_task, dsk)
        return {"dsk": dsk, "dependencies": dependencies}

    @classmethod
    def __dask_distributed_unpack__(
        cls,
        state: Any,
        dsk: Mapping[str, Any],
        dependencies: Mapping[str, set],
    ) -> Dict:
        """Unpack the state of a layer previously packed by __dask_distributed_pack__()

        This method is called by the scheduler in Distributed in order to unpack
        the state of a layer and merge it into its global task graph. The method
        should update `dsk` and `dependencies`, which are the already materialized
        state of the preceding layers in the high level graph. The layers of the
        high level graph are unpacked in topological order.

        See Layer.__dask_distributed_pack__() for packing detail.

        Parameters
        ----------
        state: Any
            The state returned by Layer.__dask_distributed_pack__()
        dsk: Mapping, read-only
            The materialized low level graph of the already unpacked layers
        dependencies: Mapping, read-only
            The dependencies of each key in `dsk`

        Returns
        -------
        unpacked-layer: dict
            layer_dsk: Mapping[str, Any]
                Materialized (stringified) graph of the layer
            layer_deps: Mapping[str, set]
                Dependencies of each key in `layer_dsk`
        """
        return {"dsk": state["dsk"], "deps": state["dependencies"]}

    def __reduce__(self):
        """Default serialization implementation, which materializes the Layer"""
        return (MaterializedLayer, (dict(self),))

    def __copy__(self):
        """Default shallow copy implementation"""
        obj = type(self).__new__(self.__class__)
        obj.__dict__.update(self.__dict__)
        return obj


class MaterializedLayer(Layer):
    """Fully materialized layer of `Layer`

    Parameters
    ----------
    mapping: Mapping
        The mapping between keys and tasks, typically a dask graph.
    """

    def __init__(self, mapping: Mapping, annotations=None):
        super().__init__(annotations=annotations)
        self.mapping = mapping

    def __contains__(self, k):
        return k in self.mapping

    def __getitem__(self, k):
        return self.mapping[k]

    def __iter__(self):
        return iter(self.mapping)

    def __len__(self):
        return len(self.mapping)

    def is_materialized(self):
        return True


class HighLevelGraph(Mapping):
    """Task graph composed of layers of dependent subgraphs

    This object encodes a Dask task graph that is composed of layers of
    dependent subgraphs, such as commonly occurs when building task graphs
    using high level collections like Dask array, bag, or dataframe.

    Typically each high level array, bag, or dataframe operation takes the task
    graphs of the input collections, merges them, and then adds one or more new
    layers of tasks for the new operation.  These layers typically have at
    least as many tasks as there are partitions or chunks in the collection.
    The HighLevelGraph object stores the subgraphs for each operation
    separately in sub-graphs, and also stores the dependency structure between
    them.

    Parameters
    ----------
    layers : Mapping[str, Mapping]
        The subgraph layers, keyed by a unique name
    dependencies : Mapping[str, set[str]]
        The set of layers on which each layer depends
    key_dependencies : Mapping[Hashable, set], optional
        Mapping (some) keys in the high level graph to their dependencies. If
        a key is missing, its dependencies will be calculated on-the-fly.

    Examples
    --------
    Here is an idealized example that shows the internal state of a
    HighLevelGraph

    >>> import dask.dataframe as dd

    >>> df = dd.read_csv('myfile.*.csv')  # doctest: +SKIP
    >>> df = df + 100  # doctest: +SKIP
    >>> df = df[df.name == 'Alice']  # doctest: +SKIP

    >>> graph = df.__dask_graph__()  # doctest: +SKIP
    >>> graph.layers  # doctest: +SKIP
    {
     'read-csv': {('read-csv', 0): (pandas.read_csv, 'myfile.0.csv'),
                  ('read-csv', 1): (pandas.read_csv, 'myfile.1.csv'),
                  ('read-csv', 2): (pandas.read_csv, 'myfile.2.csv'),
                  ('read-csv', 3): (pandas.read_csv, 'myfile.3.csv')},
     'add': {('add', 0): (operator.add, ('read-csv', 0), 100),
             ('add', 1): (operator.add, ('read-csv', 1), 100),
             ('add', 2): (operator.add, ('read-csv', 2), 100),
             ('add', 3): (operator.add, ('read-csv', 3), 100)}
     'filter': {('filter', 0): (lambda part: part[part.name == 'Alice'], ('add', 0)),
                ('filter', 1): (lambda part: part[part.name == 'Alice'], ('add', 1)),
                ('filter', 2): (lambda part: part[part.name == 'Alice'], ('add', 2)),
                ('filter', 3): (lambda part: part[part.name == 'Alice'], ('add', 3))}
    }

    >>> graph.dependencies  # doctest: +SKIP
    {
     'read-csv': set(),
     'add': {'read-csv'},
     'filter': {'add'}
    }

    See Also
    --------
    HighLevelGraph.from_collections :
        typically used by developers to make new HighLevelGraphs
    """

    layers: Mapping[str, Layer]
    dependencies: Mapping[str, AbstractSet]
    key_dependencies: Dict[Hashable, AbstractSet]
    _to_dict: dict
    _all_external_keys: set

    def __init__(
        self,
        layers: Mapping[str, Mapping],
        dependencies: Mapping[str, AbstractSet],
        key_dependencies: Optional[Dict[Hashable, AbstractSet]] = None,
    ):
        self.dependencies = dependencies
        self.key_dependencies = key_dependencies or {}
        # Makes sure that all layers are `Layer`
        self.layers = {
            k: v if isinstance(v, Layer) else MaterializedLayer(v)
            for k, v in layers.items()
        }

    @classmethod
    def _from_collection(cls, name, layer, collection):
        """ `from_collections` optimized for a single collection """
        if is_dask_collection(collection):
            graph = collection.__dask_graph__()
            if isinstance(graph, HighLevelGraph):
                layers = ensure_dict(graph.layers, copy=True)
                layers.update({name: layer})
                deps = ensure_dict(graph.dependencies, copy=True)
                with ignoring(AttributeError):
                    deps.update({name: set(collection.__dask_layers__())})
            else:
                key = _get_some_layer_name(collection)
                layers = {name: layer, key: graph}
                deps = {name: {key}, key: set()}
        else:
            raise TypeError(type(collection))

        return cls(layers, deps)

    @classmethod
    def from_collections(cls, name, layer, dependencies=()):
        """Construct a HighLevelGraph from a new layer and a set of collections

        This constructs a HighLevelGraph in the common case where we have a single
        new layer and a set of old collections on which we want to depend.

        This pulls out the ``__dask_layers__()`` method of the collections if
        they exist, and adds them to the dependencies for this new layer.  It
        also merges all of the layers from all of the dependent collections
        together into the new layers for this graph.

        Parameters
        ----------
        name : str
            The name of the new layer
        layer : Mapping
            The graph layer itself
        dependencies : List of Dask collections
            A lit of other dask collections (like arrays or dataframes) that
            have graphs themselves

        Examples
        --------

        In typical usage we make a new task layer, and then pass that layer
        along with all dependent collections to this method.

        >>> def add(self, other):
        ...     name = 'add-' + tokenize(self, other)
        ...     layer = {(name, i): (add, input_key, other)
        ...              for i, input_key in enumerate(self.__dask_keys__())}
        ...     graph = HighLevelGraph.from_collections(name, layer, dependencies=[self])
        ...     return new_collection(name, graph)
        """
        if len(dependencies) == 1:
            return cls._from_collection(name, layer, dependencies[0])
        layers = {name: layer}
        deps = {name: set()}
        for collection in toolz.unique(dependencies, key=id):
            if is_dask_collection(collection):
                graph = collection.__dask_graph__()
                if isinstance(graph, HighLevelGraph):
                    layers.update(graph.layers)
                    deps.update(graph.dependencies)
                    with ignoring(AttributeError):
                        deps[name] |= set(collection.__dask_layers__())
                else:
                    key = _get_some_layer_name(collection)
                    layers[key] = graph
                    deps[name].add(key)
                    deps[key] = set()
            else:
                raise TypeError(type(collection))

        return cls(layers, deps)

    def __getitem__(self, key):
        # Attempt O(1) direct access first, under the assumption that layer names match
        # either the keys (Scalar, Item, Delayed) or the first element of the key tuples
        # (Array, Bag, DataFrame, Series). This assumption is not always true.
        try:
            return self.layers[key][key]
        except KeyError:
            pass
        try:
            return self.layers[key[0]][key]
        except (KeyError, IndexError, TypeError):
            pass

        # Fall back to O(n) access
        for d in self.layers.values():
            try:
                return d[key]
            except KeyError:
                pass

        raise KeyError(key)

    def __len__(self):
        return len(self.to_dict())

    def __iter__(self):
        return iter(self.to_dict())

    def to_dict(self) -> dict:
        """Efficiently convert to plain dict. This method is faster than dict(self)."""
        try:
            return self._to_dict
        except AttributeError:
            out = self._to_dict = ensure_dict(self)
            return out

    def keys(self) -> AbstractSet:
        """Get all keys of all the layers.

        This will in many cases materialize layers, which makes it a relatively
        expensive operation. See :meth:`get_all_external_keys` for a faster alternative.
        """
        return self.to_dict().keys()

    def keyset(self) -> AbstractSet:
        # Backwards compatibility for now
        warnings.warn(
            "'keyset' method of HighLevelGraph is deprecated now and will be removed "
            "in a future version. To silence this warning, use '.keys' instead.",
            FutureWarning,
        )
        return self.keys()

    def get_all_external_keys(self) -> set:
        """Get all output keys of all layers

        This will in most cases _not_ materialize any layers, which makes
        it a relative cheap operation.

        Returns
        -------
        keys: set
            A set of all external keys
        """
        try:
            return self._all_external_keys
        except AttributeError:
            keys: set = set()
            for layer in self.layers.values():
                keys |= layer.get_output_keys()
            self._all_external_keys = keys
            return keys

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()

    def get_all_dependencies(self) -> Dict[Hashable, AbstractSet]:
        """Get dependencies of all keys

        This will in most cases materialize all layers, which makes
        it an expensive operation.

        Returns
        -------
        map: Mapping
            A map that maps each key to its dependencies
        """
        all_keys = self.keys()
        missing_keys = all_keys - self.key_dependencies.keys()
        if missing_keys:
            for layer in self.layers.values():
                for k in missing_keys & layer.keys():
                    self.key_dependencies[k] = layer.get_dependencies(k, all_keys)
        return self.key_dependencies

    @property
    def dependents(self):
        return reverse_dict(self.dependencies)

    @property
    def dicts(self):
        # Backwards compatibility for now
        warnings.warn(
            "'dicts' property of HighLevelGraph is deprecated now and will be "
            "removed in a future version. To silence this warning, "
            "use '.layers' instead.",
            FutureWarning,
        )
        return self.layers

    def copy(self):
        return HighLevelGraph(
            ensure_dict(self.layers, copy=True),
            ensure_dict(self.dependencies, copy=True),
            self.key_dependencies.copy(),
        )

    @classmethod
    def merge(cls, *graphs):
        layers = {}
        dependencies = {}
        for g in graphs:
            if isinstance(g, HighLevelGraph):
                layers.update(g.layers)
                dependencies.update(g.dependencies)
            elif isinstance(g, Mapping):
                layers[id(g)] = g
                dependencies[id(g)] = set()
            else:
                raise TypeError(g)
        return cls(layers, dependencies)

    def visualize(self, filename="dask.pdf", format=None, **kwargs):
        from .dot import graphviz_to_file

        g = to_graphviz(self, **kwargs)
        return graphviz_to_file(g, filename, format)

    def _toposort_layers(self):
        """Sort the layers in a high level graph topologically

        Parameters
        ----------
        hlg : HighLevelGraph
            The high level graph's layers to sort

        Returns
        -------
        sorted: list
            List of layer names sorted topologically
        """
        dependencies = copy.deepcopy(self.dependencies)
        ready = {k for k, v in dependencies.items() if len(v) == 0}
        ret = []
        while len(ready) > 0:
            layer = ready.pop()
            ret.append(layer)
            del dependencies[layer]
            for k, v in dependencies.items():
                v.discard(layer)
                if len(v) == 0:
                    ready.add(k)
        return ret

    def cull(self, keys: Iterable) -> "HighLevelGraph":
        """Return new HighLevelGraph with only the tasks required to calculate keys.

        In other words, remove unnecessary tasks from dask.

        Parameters
        ----------
        keys
            iterable of keys or nested list of keys such as the output of
            ``__dask_keys__()``

        Returns
        -------
        hlg: HighLevelGraph
            Culled high level graph
        """
        keys_set = set(flatten(keys))

        all_ext_keys = self.get_all_external_keys()
        ret_layers = {}
        ret_key_deps = {}
        for layer_name in reversed(self._toposort_layers()):
            layer = self.layers[layer_name]
            # Let's cull the layer to produce its part of `keys`
            output_keys = keys_set & layer.get_output_keys()
            if output_keys:
                culled_layer, culled_deps = layer.cull(output_keys, all_ext_keys)
                # Update `keys` with all layer's external key dependencies, which
                # are all the layer's dependencies (`culled_deps`) excluding
                # the layer's output keys.
                external_deps = set()
                for d in culled_deps.values():
                    external_deps |= d
                external_deps -= culled_layer.get_output_keys()
                keys_set |= external_deps

                # Save the culled layer and its key dependencies
                ret_layers[layer_name] = culled_layer
                ret_key_deps.update(culled_deps)

        ret_dependencies = {
            layer_name: self.dependencies[layer_name] & ret_layers.keys()
            for layer_name in ret_layers
        }

        return HighLevelGraph(ret_layers, ret_dependencies, ret_key_deps)

    def cull_layers(self, layers: Iterable[str]) -> "HighLevelGraph":
        """Return a new HighLevelGraph with only the given layers and their
        dependencies. Internally, layers are not modified.

        This is a variant of :meth:`HighLevelGraph.cull` which is much faster and does
        not risk creating a collision between two layers with the same name and
        different content when two culled graphs are merged later on.

        Returns
        -------
        hlg: HighLevelGraph
            Culled high level graph
        """
        to_visit = set(layers)
        ret_layers = {}
        ret_dependencies = {}
        while to_visit:
            k = to_visit.pop()
            ret_layers[k] = self.layers[k]
            ret_dependencies[k] = self.dependencies[k]
            to_visit |= ret_dependencies[k] - ret_dependencies.keys()

        return HighLevelGraph(ret_layers, ret_dependencies)

    def validate(self):
        # Check dependencies
        for layer_name, deps in self.dependencies.items():
            if layer_name not in self.layers:
                raise ValueError(
                    f"dependencies[{repr(layer_name)}] not found in layers"
                )
            for dep in deps:
                if dep not in self.dependencies:
                    raise ValueError(f"{repr(dep)} not found in dependencies")

        for layer in self.layers.values():
            assert hasattr(layer, "annotations")

        # Re-calculate all layer dependencies
        dependencies = compute_layer_dependencies(self.layers)

        # Check keys
        dep_key1 = self.dependencies.keys()
        dep_key2 = dependencies.keys()
        if dep_key1 != dep_key2:
            raise ValueError(
                f"incorrect dependencies keys {set(dep_key1)!r} "
                f"expected {set(dep_key2)!r}"
            )

        # Check values
        for k in dep_key1:
            if self.dependencies[k] != dependencies[k]:
                raise ValueError(
                    f"incorrect dependencies[{repr(k)}]: {repr(self.dependencies[k])} "
                    f"expected {repr(dependencies[k])}"
                )

    def __dask_distributed_pack__(self, client, client_keys: Iterable[Hashable]) -> Any:
        """Pack the high level graph for Scheduler -> Worker communication

        The approach is to delegate the packaging to each layer in the high level graph
        by calling .__dask_distributed_pack__() and .__dask_distributed_anno_pack__()
        on each layer. If the layer doesn't implement packaging, we materialize the
        layer and pack it.

        Parameters
        ----------
        client : distributed.Client
            The client calling this function.
        client_keys : Iterable
            List of keys requested by the client.

        Returns
        -------
        data: list of header and payload
            Packed high level graph serialized by dumps_msgpack
        """
        from distributed.protocol.core import dumps_msgpack

        # Dump each layer (in topological order)
        layers = []
        for layer in (self.layers[name] for name in self._toposort_layers()):
            layers.append(
                {
                    "__module__": layer.__module__,
                    "__name__": type(layer).__name__,
                    "state": layer.__dask_distributed_pack__(
                        self.get_all_external_keys(),
                        self.key_dependencies,
                        client,
                        client_keys,
                    ),
                    "annotations": layer.__dask_distributed_anno_pack__(),
                }
            )
        return dumps_msgpack({"layers": layers})

    @staticmethod
    def __dask_distributed_unpack__(packed_hlg, annotations: Mapping[str, Any]) -> Dict:
        """Unpack the high level graph for Scheduler -> Worker communication

        The approach is to delegate the unpackaging to each layer in the high level graph
        by calling ..._unpack__() and ..._annotations_unpack__()
        on each layer.

        Parameters
        ----------
        packed_hlg : list of header and payload
            Packed high level graph serialized by dumps_msgpack
        annotations : dict
            A top-level annotations object which may be partially populated,
            and which may be further filled by annotations from the layers
            of the packed_hlg.

        Returns
        -------
        unpacked-graph: dict
            dsk: Dict[str, Any]
                Materialized (stringified) graph of all nodes in the high level graph
            deps: Dict[str, set]
                Dependencies of each key in `dsk`
            annotations: Dict[str, Any]
                Annotations for `dsk`
        """
        from distributed.protocol.core import loads_msgpack
        from distributed.protocol.serialize import import_allowed_module

        hlg = loads_msgpack(*packed_hlg)
        dsk = {}
        deps = {}
        anno = {}

        # Unpack each layer (in topological order)
        for layer in hlg["layers"]:
            # Find the unpack functions
            if layer["__module__"] is None:  # Default implementation
                unpack_state = Layer.__dask_distributed_unpack__
                unpack_anno = Layer.__dask_distributed_annotations_unpack__
            else:
                mod = import_allowed_module(layer["__module__"])
                cls = getattr(mod, layer["__name__"])
                unpack_state = cls.__dask_distributed_unpack__
                unpack_anno = cls.__dask_distributed_annotations_unpack__

            # Unpack state into a graph and key dependencies
            unpacked_layer = unpack_state(layer["state"], dsk, deps)
            dsk.update(unpacked_layer["dsk"])
            for k, v in unpacked_layer["deps"].items():
                deps[k] = deps.get(k, set()) | v

            # Unpack the annotations
            if annotations and layer["annotations"]:
                layer_annotations = {**layer["annotations"], **annotations}
            else:
                layer_annotations = annotations or layer["annotations"] or None
            unpack_anno(anno, layer_annotations, unpacked_layer["dsk"].keys())

        return {"dsk": dsk, "deps": deps, "annotations": anno}


def to_graphviz(
    hg,
    data_attributes=None,
    function_attributes=None,
    rankdir="BT",
    graph_attr={},
    node_attr=None,
    edge_attr=None,
    **kwargs,
):
    from .dot import graphviz, name, label

    if data_attributes is None:
        data_attributes = {}
    if function_attributes is None:
        function_attributes = {}

    graph_attr = graph_attr or {}
    graph_attr["rankdir"] = rankdir
    graph_attr.update(kwargs)
    g = graphviz.Digraph(
        graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr
    )

    cache = {}

    for k in hg.dependencies:
        k_name = name(k)
        attrs = data_attributes.get(k, {})
        attrs.setdefault("label", label(k, cache=cache))
        attrs.setdefault("shape", "box")
        g.node(k_name, **attrs)

    for k, deps in hg.dependencies.items():
        k_name = name(k)
        for dep in deps:
            dep_name = name(dep)
            g.edge(dep_name, k_name)
    return g


def _get_some_layer_name(collection) -> str:
    """Somehow get a unique name for a Layer from a non-HighLevelGraph dask mapping"""
    try:
        (name,) = collection.__dask_layers__()
        return name
    except (AttributeError, ValueError):
        # collection does not define the optional __dask_layers__ method
        # or it spuriously returns more than one layer
        return str(id(collection))

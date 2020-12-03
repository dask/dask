import abc
import collections.abc
from typing import (
    Any,
    Dict,
    Hashable,
    Optional,
    Set,
    Mapping,
    Iterable,
    Tuple,
)
import copy

import tlz as toolz

from . import config
from .utils import ignoring, stringify
from .base import is_dask_collection
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
    ret = {k: set() for k in layers.keys()}
    for k, v in layers.items():
        for key in keys_in_tasks(all_keys.difference(v.keys()), v.values()):
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

    def __init__(self, annotations=None):
        if annotations:
            self.annotations = annotations
        else:
            self.annotations = copy.copy(config.get("annotations", None))

    @abc.abstractmethod
    def is_materialized(self) -> bool:
        """Return whether the layer is materialized or not"""
        return True

    def get_output_keys(self) -> Set:
        """Return a set of all output keys

        Output keys are all keys in the layer that might be referenced by
        other layers.

        Classes overriding this implementation should not cause the layer
        to be materialized.

        Returns
        -------
        keys: Set
            All output keys
        """
        return self.keys()

    def pack_annotations(self) -> Mapping[str, Any]:
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
    def expand_annotations(annotations, keys) -> Mapping[str, Any]:
        if annotations is None:
            return None

        expanded = {}
        keys_stringified = False

        for a, v in annotations.items():
            if type(v) is dict and "__expanded_annotations__" in v:
                # Maybe do a destructive update for efficiency?
                v = v.copy()
                del v["__expanded_annotations__"]
                expanded[a] = v
            else:
                if not keys_stringified:
                    keys = [stringify(k) for k in keys]
                    keys_stringified = True

                expanded[a] = {k: v for k in keys}

        return expanded

    def cull(
        self, keys: Set, all_hlg_keys: Iterable
    ) -> Tuple["Layer", Mapping[Hashable, Set]]:
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

        return BasicLayer(out), ret_deps

    def get_dependencies(self, key: Hashable, all_hlg_keys: Iterable) -> Set:
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

    def __dask_distributed_pack__(self, client) -> Optional[Any]:
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

        Alternatively, the method can return None, which will make Distributed
        materialize the layer and use a default packing method.

        Parameters
        ----------
        client: distributed.Client
            The client calling this function.

        Returns
        -------
        state: Object serializable by msgpack
            Scheduler compatible state of the layer or None
        """
        return None

    @classmethod
    def __dask_distributed_unpack__(
        cls,
        state: Any,
        dsk: Dict[str, Any],
        dependencies: Mapping[Hashable, Set],
        annotations: Dict[str, Any],
    ) -> None:
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
        dsk: dict
            The materialized low level graph of the already unpacked layers
        dependencies: Mapping
            The dependencies of each key in `dsk`
        annotations: dict
            The materialized task annotations
        """
        raise NotImplementedError(
            f"{type(cls)} doesn't implement __dask_distributed_unpack__()"
        )

    def __reduce__(self):
        """Default serialization implementation, which materializes the Layer

        This should follow the standard pickle protocol[1] but must always return
        a tuple and the arguments for the callable object must be compatible with
        msgpack. This is because Distributed uses msgpack to send Layers to the
        scheduler.

        [1] <https://docs.python.org/3/library/pickle.html#object.__reduce__>
        """
        return (BasicLayer, (dict(self),))

    def __copy__(self):
        """Default shallow copy implementation"""
        obj = type(self).__new__(self.__class__)
        obj.__dict__.update(self.__dict__)
        return obj


class BasicLayer(Layer):
    """Basic implementation of `Layer`

    Fully materialized layer implemented by a mapping

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
    dependencies : Mapping[str, Set[str]]
        The set of layers on which each layer depends
    key_dependencies : Mapping[Hashable, Set], optional
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

    def __init__(
        self,
        layers: Mapping[str, Layer],
        dependencies: Mapping[str, Set],
        key_dependencies: Optional[Mapping[Hashable, Set]] = None,
    ):
        self._keys = None
        self._all_external_keys = None
        self.layers = layers
        self.dependencies = dependencies
        self.key_dependencies = key_dependencies if key_dependencies else {}

        # Makes sure that all layers are `Layer`
        self.layers = {
            k: v if isinstance(v, Layer) else BasicLayer(v)
            for k, v in self.layers.items()
        }

    @classmethod
    def _from_collection(cls, name, layer, collection):
        """ `from_collections` optimized for a single collection """
        if is_dask_collection(collection):
            graph = collection.__dask_graph__()
            if isinstance(graph, HighLevelGraph):
                layers = graph.layers.copy()
                layers.update({name: layer})
                deps = graph.dependencies.copy()
                with ignoring(AttributeError):
                    deps.update({name: set(collection.__dask_layers__())})
            else:
                try:
                    [key] = collection.__dask_layers__()
                except AttributeError:
                    key = id(graph)
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
                    try:
                        [key] = collection.__dask_layers__()
                    except AttributeError:
                        key = id(graph)
                    layers[key] = graph
                    deps[name].add(key)
                    deps[key] = set()
            else:
                raise TypeError(type(collection))

        return cls(layers, deps)

    def __getitem__(self, key):
        for d in self.layers.values():
            if key in d:
                return d[key]
        raise KeyError(key)

    def __len__(self):
        return len(self.keyset())

    def __iter__(self):
        return toolz.unique(toolz.concat(self.layers.values()))

    def keyset(self) -> Set:
        """Get all keys of all the layers

        This will in many cases materialize layers, which makes it
        a relative cheap operation. See `get_all_external_keys()`
        for a faster alternative.

        Returns
        -------
        keys: Set
            A set of all keys
        """
        if self._keys is None:
            self._keys = set()
            for layer in self.layers.values():
                self._keys.update(layer.keys())
        return self._keys

    def get_all_external_keys(self) -> Set:
        """Get all output keys of all layers

        This will in most cases _not_ materialize any layers, which makes
        it a relative cheap operation.

        Returns
        -------
        keys: Set
            A set of all external keys
        """
        if self._all_external_keys is None:
            self._all_external_keys = set()
            for layer in self.layers.values():
                self._all_external_keys.update(layer.get_output_keys())
        return self._all_external_keys

    def get_all_dependencies(self) -> Mapping[Hashable, Set]:
        """Get dependencies of all keys

        This will in most cases materialize all layers, which makes
        it an expensive operation.

        Returns
        -------
        map: Mapping
            A map that maps each key to its dependencies
        """
        all_keys = self.keyset()
        missing_keys = all_keys.difference(self.key_dependencies.keys())
        if missing_keys:
            for layer in self.layers.values():
                for k in missing_keys.intersection(layer.keys()):
                    self.key_dependencies[k] = layer.get_dependencies(k, all_keys)
        return self.key_dependencies

    @property
    def dependents(self):
        return reverse_dict(self.dependencies)

    @property
    def dicts(self):
        # Backwards compatibility for now
        return self.layers

    def items(self):
        items = []
        seen = set()
        for d in self.layers.values():
            for key in d:
                if key not in seen:
                    seen.add(key)
                    items.append((key, d[key]))
        return items

    def keys(self):
        return [key for key, _ in self.items()]

    def values(self):
        return [value for _, value in self.items()]

    def copy(self):
        return HighLevelGraph(self.layers.copy(), self.dependencies.copy())

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

    def cull(self, keys: Set) -> "HighLevelGraph":
        """Return new high level graph with only the tasks required to calculate keys.

        In other words, remove unnecessary tasks from dask.
        ``keys`` may be a single key or list of keys.

        Returns
        -------
        hlg: HighLevelGraph
            Culled high level graph
        """

        all_ext_keys = self.get_all_external_keys()
        ret_layers = {}
        ret_key_deps = {}
        for layer_name in reversed(self._toposort_layers()):
            layer = self.layers[layer_name]
            # Let's cull the layer to produce its part of `keys`
            output_keys = keys.intersection(layer.get_output_keys())
            if len(output_keys) > 0:
                culled_layer, culled_deps = layer.cull(output_keys, all_ext_keys)
                # Update `keys` with all layer's external key dependencies, which
                # are all the layer's dependencies (`culled_deps`) excluding
                # the layer's output keys.
                external_deps = set()
                for d in culled_deps.values():
                    external_deps |= d
                external_deps.difference_update(culled_layer.get_output_keys())
                keys.update(external_deps)

                # Save the culled layer and its key dependencies
                ret_layers[layer_name] = culled_layer
                ret_key_deps.update(culled_deps)

        ret_dependencies = {}
        for layer_name in ret_layers:
            ret_dependencies[layer_name] = {
                d for d in self.dependencies[layer_name] if d in ret_layers
            }

        return HighLevelGraph(ret_layers, ret_dependencies, ret_key_deps)

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
        dep_key1 = set(self.dependencies.keys())
        dep_key2 = set(dependencies.keys())
        if dep_key1 != dep_key2:
            raise ValueError(
                f"incorrect dependencies keys {repr(dep_key1)} "
                f"expected {repr(dep_key2)}"
            )

        # Check values
        for k in dep_key1:
            if self.dependencies[k] != dependencies[k]:
                raise ValueError(
                    f"incorrect dependencies[{repr(k)}]: {repr(self.dependencies[k])} "
                    f"expected {repr(dependencies[k])}"
                )


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

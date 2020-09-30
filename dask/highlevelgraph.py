import collections.abc
from typing import Callable, Hashable, Optional, Set, Mapping, Iterable, Tuple
import copy

import tlz as toolz

from .utils import ignoring
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
    """

    def cull(
        self, keys: Set, all_hlg_keys: Iterable
    ) -> Tuple["Layer", Mapping[Hashable, Set]]:
        """Return a new Layer with only the tasks required to calculate `keys`.

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
        """
        deps = self.get_dependencies(all_hlg_keys)

        if len(keys) == len(self):
            return self, deps  # Nothing to cull if preserving all existing keys

        ret_deps = {}
        seen = set()
        out = {}
        work = keys.copy()
        while work:
            k = work.pop()
            out[k] = self[k]
            ret_deps[k] = deps[k]
            for d in deps[k]:
                if d not in seen:
                    if d in self:
                        seen.add(d)
                        work.add(d)

        return BasicLayer(out), ret_deps

    def get_dependencies(self, all_hlg_keys: Iterable) -> Mapping[Hashable, Set]:
        """Get dependencies of all keys in the layer

        Parameters
        ----------
        all_hlg_keys : Iterable
            All keys in the high level graph.

        Returns
        -------
        map: Mapping
            A map that maps each key in the layer to its dependencies
        """
        return {k: keys_in_tasks(all_hlg_keys, [v]) for k, v in self.items()}


class BasicLayer(Layer):
    """Basic implementation of `Layer`

    Parameters
    ----------
    mapping : Mapping
        The mapping between keys and tasks, typically a dask graph.
    dependencies : Mapping[Hashable, Set], optional
        Mapping between keys and their dependencies
    global_dependencies: Set, optional
        Set of dependencies that all keys in the layer depend on. Notice,
        the set might also contain literals that will be ignored.
    """

    def __init__(self, mapping, dependencies=None, global_dependencies=None):
        self.mapping = mapping
        self.dependencies = dependencies
        self.global_dependencies = global_dependencies

    def __contains__(self, k):
        return k in self.mapping

    def __getitem__(self, k):
        return self.mapping[k]

    def __iter__(self):
        return iter(self.mapping)

    def __len__(self):
        return len(self.mapping)

    def get_dependencies(self, all_hlg_keys):
        if self.dependencies is None or self.global_dependencies is None:
            return super().get_dependencies(all_hlg_keys)

        global_deps = self.global_dependencies.intersection(all_hlg_keys)
        ret = self.dependencies.copy()
        for v in ret.values():
            v |= global_deps
        return ret


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
        Mapping all keys in the high level graph to their dependencies

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
        layers: Mapping[str, Mapping],
        dependencies: Mapping[str, Set],
        key_dependencies: Optional[Mapping[Hashable, Set]] = None,
    ):
        self._keys = None
        self.layers = layers
        self.dependencies = dependencies
        self.key_dependencies = key_dependencies

        # Makes sure that all layers are `Layer`
        self.layers = {
            k: v if isinstance(v, Layer) else BasicLayer(v)
            for k, v in self.layers.items()
        }

    def keyset(self):
        if self._keys is None:
            self._keys = set()
            for layer in self.layers.values():
                self._keys.update(layer.keys())
        return self._keys

    @property
    def dependents(self):
        return reverse_dict(self.dependencies)

    @property
    def dicts(self):
        # Backwards compatibility for now
        return self.layers

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

    def get_dependencies(self) -> Mapping[Hashable, Set]:
        """Get dependencies of all keys in the HLG

        Returns
        -------
        map: Mapping
            A map that maps each key to its dependencies
        """
        if self.key_dependencies is None:
            all_keys = self.keyset()
            self.key_dependencies = {}
            for layer in self.layers.values():
                self.key_dependencies.update(layer.get_dependencies(all_keys))

        return self.key_dependencies

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

        layers = self._toposort_layers()
        all_keys = self.keyset()

        ret_layers = {}
        ret_key_deps = {}
        for layer_name in reversed(layers):
            layer = self.layers[layer_name]
            key_deps = keys.intersection(layer)
            if len(key_deps) > 0:
                culled_layer, culled_deps = layer.cull(key_deps, all_keys)

                external_deps = set()
                for k in culled_layer.keys():
                    external_deps |= culled_deps[k]
                external_deps.difference_update(culled_layer.keys())

                keys.update(external_deps)
                ret_layers[layer_name] = culled_layer
                ret_key_deps.update(culled_deps)

        ret_dependencies = {}
        for layer_name in ret_layers:
            ret_dependencies[layer_name] = {
                d for d in self.dependencies[layer_name] if d in ret_layers
            }

        return HighLevelGraph(ret_layers, ret_dependencies, ret_key_deps)

    def map_basic_layers(
        self, func: Callable[[BasicLayer], Mapping]
    ) -> "HighLevelGraph":
        """Map `func` on each basic layer and returns a new high level graph.

        `func` should take a BasicLayer as input and return a new Mapping as output
        and **cannot** change the dependencies between Layers.

        If `func` returns a non-BasicLayer type, it will be wrapped in a `BasicLayer`
        object automatically.

        Parameters
        ----------
        func : callable
            The function to call on each BasicLayer

        Returns
        -------
        hlg : HighLevelGraph
            A high level graph containing the transformed BasicLayers and the other
            Layers untouched
        """
        layers = {}
        for k, v in self.layers.items():
            if isinstance(v, BasicLayer):
                layer = func(v)
                if not isinstance(layer, BasicLayer):
                    layer = BasicLayer(layer)
                layers[k] = layer
            else:
                layers[k] = v
        return HighLevelGraph(layers, self.dependencies)

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

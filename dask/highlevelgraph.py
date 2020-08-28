from collections.abc import Mapping
from typing import Set, Dict
import copy

import tlz as toolz

from .utils import ignoring
from .base import is_dask_collection
from .core import reverse_dict, get_dependencies, flatten, keys_in_tasks


class Layer(Mapping):
    """ High level graph layer

    This abstract class establish a protocol for high level graph layers.
    """

    def cull(self, keys: Set) -> "Layer":
        """ Return a new Layer with only the tasks required to calculate `keys`.

        In other words, remove unnecessary tasks from the layer.

        Examples
        --------
        >>> d = Layer({'x': 1, 'y': (inc, 'x'), 'out': (add, 'x', 10)})
        >>> dsk = d.cull({'out'})
        >>> dsk  # doctest: +SKIP
        {'x': 1, 'out': (add, 'x', 10)}

        Returns
        -------
        layer: Layer
            Culled layer
        """

        seen = set()
        out = {}
        work = set(flatten(keys))
        while work:
            new_work = set()
            for k in work:
                out[k] = self[k]
                for d in get_dependencies(self, k):
                    if d not in seen:
                        seen.add(d)
                        new_work.add(d)
            work = new_work

        return BasicLayer(out)

    def get_external_dependencies(self, known_keys: Set) -> Set:
        """Get external dependencies

        Parameters
        ----------
        known_keys : set
            Set of known keys (typically all keys in a HighLevelGraph)

        Returns
        -------
        deps: set
            Set of dependencies
        """

        all_deps = keys_in_tasks(known_keys, self.values())
        return all_deps.difference(self.keys())


class BasicLayer(Layer):
    """ Basic implementation of `Layer` that takes a mapping """

    def __init__(self, mapping):
        self.__mapping = mapping

    def __getitem__(self, k):
        return self.__mapping[k]

    def __iter__(self):
        return iter(self.__mapping)

    def __len__(self):
        return len(self.__mapping)


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
    layers : Dict[str, Mapping]
        The subgraph layers, keyed by a unique name
    dependencies : Dict[str, Set[str]]
        The set of layers on which each layer depends

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

    def __init__(self, layers: Dict[str, Mapping], dependencies: Dict[str, Set]):
        self.layers = layers
        self.dependencies = dependencies

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
        deps = {}
        deps[name] = set()
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
        return sum(1 for _ in self)

    def items(self):
        items = []
        seen = set()
        for d in self.layers.values():
            for key in d:
                if key not in seen:
                    seen.add(key)
                    items.append((key, d[key]))
        return items

    def __iter__(self):
        return toolz.unique(toolz.concat(self.layers.values()))

    def keys(self):
        return [key for key, _ in self.items()]

    def values(self):
        return [value for _, value in self.items()]

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

    def _fix_hlg_layers_inplace(self):
        """Makes sure that all layers in hlg are `Layer`"""
        new_layers = {}
        for k, v in self.layers.items():
            if not isinstance(v, Layer):
                new_layers[k] = BasicLayer(v)
        self.layers.update(new_layers)

    def _find_layer_containing_key(self, key):
        for k, v in self.layers.items():
            if key in v:
                return k
        raise RuntimeError(f"{repr(key)} not found")

    def _fix_hlg_dependencies_inplace(self):
        """Makes sure that self.dependencies is correct"""
        all_keys = set(self.keys())
        self.dependencies = {k: set() for k in self.layers.keys()}
        for k, v in self.layers.items():
            for key in v.get_external_dependencies(all_keys):
                self.dependencies[k].add(self._find_layer_containing_key(key))

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

    def cull(self, keys):
        """ Return new high level graph with only the tasks required to calculate keys.

        In other words, remove unnecessary tasks from dask.
        ``keys`` may be a single key or list of keys.

        Returns
        -------
        hlg: HighLevelGraph
            Culled high level graph
        """

        self._fix_hlg_layers_inplace()

        # TODO: remove this when <https://github.com/dask/dask/pull/6509> passes
        self._fix_hlg_dependencies_inplace()

        if not isinstance(keys, (list, set)):
            keys = [keys]
        keys = set(flatten(keys))

        layers = self._toposort_layers()
        ret_layers = {}
        known_keys = set(self.keys())

        for layer_name in reversed(layers):
            layer = self.layers[layer_name]
            key_deps = keys.intersection(layer)
            if len(key_deps) > 0:
                culled_layer = layer.cull(key_deps)
                keys.update(culled_layer.get_external_dependencies(known_keys))
                ret_layers[layer_name] = culled_layer

        ret_dependencies = {}
        for layer_name in ret_layers:
            ret_dependencies[layer_name] = {
                d for d in self.dependencies[layer_name] if d in ret_layers
            }

        return HighLevelGraph(ret_layers, ret_dependencies)


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

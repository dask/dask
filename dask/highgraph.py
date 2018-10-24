import toolz

from . import sharedict
from .utils import ignoring
from .base import is_dask_collection
from .compatibility import Mapping


class HighLevelGraph(sharedict.ShareDict):
    def __init__(self, layers, dependencies):
        for v in layers.values():
            assert not isinstance(v, sharedict.ShareDict)
        assert all(layers)
        self.layers = layers
        self.dependencies = dependencies
        assert set(dependencies) == set(layers)

    @property
    def dicts(self):
        # Backwards compatibility for now
        return self.layers

    @classmethod
    def from_collections(cls, name, layer, dependencies=()):
        """ Construct a HighLevelGraph from a new layer and a set of collections

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
        """
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
        seen = set()
        for d in self.layers.values():
            for key in d:
                if key not in seen:
                    seen.add(key)
                    yield (key, d[key])

    def __iter__(self):
        return toolz.unique(toolz.concat(self.layers.values()))

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

    def visualize(self, filename='dask.pdf', format=None, **kwargs):
        from .dot import graphviz_to_file
        g = to_graphviz(self, **kwargs)
        return graphviz_to_file(g, filename, format)


def to_graphviz(hg, data_attributes=None, function_attributes=None,
                rankdir='BT', graph_attr={}, node_attr=None, edge_attr=None, **kwargs):
    from .dot import graphviz, name, label

    if data_attributes is None:
        data_attributes = {}
    if function_attributes is None:
        function_attributes = {}

    graph_attr = graph_attr or {}
    graph_attr['rankdir'] = rankdir
    graph_attr.update(kwargs)
    g = graphviz.Digraph(graph_attr=graph_attr,
                         node_attr=node_attr,
                         edge_attr=edge_attr)

    seen = set()
    cache = {}

    for k in hg.dependencies:
        k_name = name(k)
        attrs = data_attributes.get(k, {})
        attrs.setdefault('label', label(k, cache=cache))
        attrs.setdefault('shape', 'box')
        g.node(k_name, **attrs)

    for k, deps in hg.dependencies.items():
        k_name = name(k)
        for dep in deps:
            dep_name = name(dep)
            g.edge(dep_name, k_name)
    return g

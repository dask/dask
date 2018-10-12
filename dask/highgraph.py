from . import sharedict
from .utils import ignoring
from .base import is_dask_collection

import toolz

class HighGraph(sharedict.ShareDict):
    def __init__(self, layers, dependencies):
        for v in layers.values():
            assert not isinstance(v, sharedict.ShareDict)
        self.layers = layers
        self.dependencies = dependencies
        assert set(dependencies) == set(layers)

    @property
    def dicts(self):
        # Backwards compatibility for now
        return self.layers

    @classmethod
    def from_collections(cls, name, layer, dependencies=()):
        layers = {name: layer}
        deps = {}
        deps[name] = set()
        for collection in toolz.unique(dependencies, key=id):
            if is_dask_collection(collection):
                graph = collection.__dask_graph__()
                layers.update(graph.layers)
                deps.update(graph.dependencies)
                with ignoring(AttributeError):
                    deps[name] |= set(collection.__dask_layers__())
            else:
                raise TypeError(type(dep))

        return HighGraph(layers, deps)

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

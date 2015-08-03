from operator import attrgetter

from toolz import merge, groupby, concat

from .context import _globals

class Base(object):
    """Base class for dask collections"""

    def visualize(self, optimize_graph=False):
        from dask.dot import dot_graph
        if optimize_graph:
            return dot_graph(self._optimize(self.dask, self._keys()))
        else:
            return dot_graph(self.dask)

    def compute(self, **kwargs):
        return compute(self, **kwargs)[0]


def compute(*args, **kwargs):
    get = kwargs.pop('get', None) or _globals['get']
    if get:
        return _compute(args, get, **kwargs)
    else:
        # May have multiple different default schedulers
        groups = groupby(attrgetter('_get'), args).items()
        return tuple(concat([_compute(a, g, **kwargs) for g, a in groups]))


def _compute(args, get, **kwargs):
    keys = [a._keys() for a in args]
    dsk = merge(*[a._optimize(a.dask, a._keys()) for a in args])
    results = get(dsk, keys, **kwargs)
    return [a._finalize(r) for a, r in zip(args, results)]

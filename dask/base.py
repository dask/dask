import warnings
from operator import attrgetter

from toolz import merge, groupby, unique, first

from .context import _globals


class Base(object):
    """Base class for dask collections"""

    def visualize(self, optimize_graph=False):
        from dask.dot import dot_graph
        if optimize_graph:
            return dot_graph(self._config.optimize(self.dask, self._keys()))
        else:
            return dot_graph(self.dask)

    def _visualize(self, optimize_graph=False):
        warn = DeprecationWarning("``_visualize`` is deprecated, use "
                                  "``visualize`` instead.")
        warnings.warn(warn)
        return self.visualize(optimize_graph)

    def compute(self, **kwargs):
        return compute(self, **kwargs)[0]

    @classmethod
    def _get(cls, dsk, keys, get=None, **kwargs):
        get = get or _globals['get'] or cls._default_get
        dsk2 = cls._optimize(dsk, keys)
        return get(dsk2, keys, **kwargs)


def compute(*args, **kwargs):
    """Compute several dask collections at once.

    Examples
    --------
    >>> import dask.array as da
    >>> a = da.arange(10, chunks=2).sum()
    >>> b = da.arange(10, chunks=2).mean()
    >>> compute(a, b)
    (45, 4.5)
    """
    groups = groupby(type, args)
    get = kwargs.pop('get', None) or _globals['get']

    if not get:
        get = first(first(groups.values()))._default_get
        if not all(val[0]._default_get == get for val in groups.values()):
            raise ValueError("Compute called on multiple collections with "
                             "differing default schedulers. Please specify a "
                             "scheduler `get` function using either "
                             "the `get` kwarg or globally with `set_options`.")

    dsk = merge(*[typ._optimize(merge([v.dask for v in val]),
                                [v._keys() for v in val])
                  for typ, val in groups.items()])

    keys = [arg._keys() for arg in args]
    results = get(dsk, keys, **kwargs)

    return tuple(a._finalize(a, r) for a, r in zip(args, results))

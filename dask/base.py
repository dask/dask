import warnings
from operator import attrgetter

from toolz import merge, groupby, unique

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


class Config(object):
    """Specifies a configuration for a dask collection.

    This holds functions that are used to specialize dask schedulers for a
    specific collection.

    Parameters
    ----------
    optimize : callable
        Takes a dask and iterable of keys, and returns an optimized dask.
    default_get : callable
        The default scheduler to use (e.g. ``dask.threaded.get``)
    finalize : callable
        Takes a dask collection and an iterable of results, and converts them
        into the desired python type.
    """
    def __init__(self, optimize, default_get, finalize):
        self.optimize = optimize
        self.default_get = default_get
        self.finalize = finalize

    def get(self, dsk, keys, get=None, **kwargs):
        get = get or _globals['get'] or self.default_get
        dsk2 = self.optimize(dsk, keys)
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
    groups = groupby(attrgetter('_config'), args)
    if len(list(unique(groups, attrgetter('default_get')))) > 1:
        raise ValueError("Compute called on multiple collections with "
                         "differing default schedulers. Please specify a "
                         "scheduler `get` function using either "
                         "the `get` kwarg or globally with `set_options`.")
    get = kwargs.pop('get', None) or _globals['get']
    if not get:
        get = args[0]._config.default_get
    keys = [a._keys() for a in args]
    dsk = merge(*[_combine(k, v) for k, v in groups.items()])
    results = get(dsk, keys, **kwargs)
    return tuple(a._config.finalize(a, r) for a, r in zip(args, results))


def _combine(config, args):
    keys = [a._keys() for a in args]
    dsk = merge(*[a.dask for a in args])
    return config.optimize(dsk, keys)

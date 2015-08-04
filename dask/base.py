import warnings
from operator import attrgetter

from toolz import merge, groupby

from .context import _globals


class Base(object):
    """Base class for dask collections"""

    def visualize(self, filename=None, optimize_graph=False):
        from dask.dot import dot_graph
        if optimize_graph:
            dsk = self._optimize(self.dask, self._keys())
        else:
            dsk = self.dask
        return dot_graph(dsk, filename=filename)

    def _visualize(self, filename=None, optimize_graph=False):
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
    groups = groupby(attrgetter('_optimize'), args)
    get = kwargs.pop('get', None) or _globals['get']

    if not get:
        get = args[0]._default_get
        if not all(a._default_get == get for a in args):
            raise ValueError("Compute called on multiple collections with "
                             "differing default schedulers. Please specify a "
                             "scheduler `get` function using either "
                             "the `get` kwarg or globally with `set_options`.")

    dsk = merge([opt(merge([v.dask for v in val]), [v._keys() for v in val])
                for opt, val in groups.items()])
    keys = [arg._keys() for arg in args]
    results = get(dsk, keys, **kwargs)
    return tuple(a._finalize(a, r) for a, r in zip(args, results))

import warnings
from operator import attrgetter
from hashlib import md5

from toolz import merge, groupby

from .context import _globals
from .utils import Dispatch, ignoring


class Base(object):
    """Base class for dask collections"""

    def visualize(self, filename='mydask', optimize_graph=False):
        from dask.dot import dot_graph
        if optimize_graph:
            dsk = self._optimize(self.dask, self._keys())
        else:
            dsk = self.dask
        return dot_graph(dsk, filename=filename)

    def _visualize(self, filename='mydask', optimize_graph=False):
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


normalize = Dispatch()
normalize.register(object, lambda a: a)
normalize.register(dict, lambda a: tuple(sorted(a.items())))
with ignoring(ImportError):
    import pandas as pd
    normalize.register(pd.DataFrame, lambda a: (id(a), len(a), list(a.columns)))
    normalize.register(pd.Series, lambda a: (id(a), len(a), a.name))
with ignoring(ImportError):
    import numpy as np
    normalize.register(np.ndarray, lambda a: (id(a), a.dtype, a.shape))


def tokenize(*args):
    """ Deterministic token

    >>> tokenize([1, 2, '3'])
    'b9e8c0d38fb40e66dc4fd00adc3c6553'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    return md5(str(tuple(normalize(a) for a in args)).encode()).hexdigest()

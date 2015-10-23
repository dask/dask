from __future__ import absolute_import, division, print_function

import warnings
from operator import attrgetter
from hashlib import md5
from functools import partial

from toolz import merge, groupby, curry
from toolz.functoolz import Compose

from .compatibility import bind_method
from .context import _globals
from .utils import Dispatch, ignoring

__all__ = ("Base", "compute", "normalize_token", "tokenize", "visualize")


class Base(object):
    """Base class for dask collections"""

    def visualize(self, filename='mydask', format=None, optimize_graph=False,
                  **kwargs):
        return visualize(self, filename=filename, format=format,
                         optimize_graph=optimize_graph, **kwargs)

    def _visualize(self, filename='mydask', format=None, optimize_graph=False):
        warn = DeprecationWarning("``_visualize`` is deprecated, use "
                                  "``visualize`` instead.")
        warnings.warn(warn)
        return self.visualize(filename=filename, format=format,
                              optimize_graph=optimize_graph)

    def compute(self, **kwargs):
        return compute(self, **kwargs)[0]

    @classmethod
    def _get(cls, dsk, keys, get=None, **kwargs):
        get = get or _globals['get'] or cls._default_get
        dsk2 = cls._optimize(dsk, keys)
        return get(dsk2, keys, **kwargs)

    @classmethod
    def _bind_operator(cls, op):
        """ bind operator to this class """
        name = op.__name__

        if name.endswith('_'):
            # for and_ and or_
            name = name[:-1]
        elif name == 'inv':
            name = 'invert'

        meth = '__{0}__'.format(name)

        if name in ('abs', 'invert', 'neg'):
            bind_method(cls, meth, cls._get_unary_operator(op))
        else:
            bind_method(cls, meth, cls._get_binary_operator(op))

            if name in ('eq', 'gt', 'ge', 'lt', 'le', 'ne'):
                return

            rmeth = '__r{0}__'.format(name)
            bind_method(cls, rmeth, cls._get_binary_operator(op, inv=True))

    @classmethod
    def _get_unary_operator(cls, op):
        """ Must return a method used by unary operator """
        raise NotImplementedError

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        """ Must return a method used by binary operator """
        raise NotImplementedError


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


def visualize(*args, **kwargs):
    filename = kwargs.pop('filename', 'mydask')
    optimize_graph = kwargs.pop('optimize_graph', False)
    from dask.dot import dot_graph
    if optimize_graph:
        dsks = [arg._optimize(arg.dask, arg._keys()) for arg in args]
    else:
        dsks = [arg.dask for arg in args]
    dsk = merge(dsks)

    return dot_graph(dsk, filename=filename, **kwargs)


def normalize_function(func):
    if isinstance(func, curry):
        func = func._partial
    if isinstance(func, Compose):
        first = getattr(func, 'first', None)
        funcs = reversed((first,) + func.funcs) if first else func.funcs
        return tuple(normalize_function(f) for f in funcs)
    elif isinstance(func, partial):
        kws = tuple(sorted(func.keywords.items())) if func.keywords else ()
        return (normalize_function(func.func), func.args, kws)
    else:
        return str(func)


normalize_token = Dispatch()
normalize_token.register((int, float, str, tuple, list), lambda a: a)
normalize_token.register(object,
        lambda a: normalize_function(a) if callable(a) else a)
normalize_token.register(dict, lambda a: tuple(sorted(a.items())))

with ignoring(ImportError):
    import pandas as pd

    @partial(normalize_token.register, pd.Index)
    def normalize_index(ind):
        return [ind.name, normalize_token(ind.values)]

    @partial(normalize_token.register, pd.Categorical)
    def normalize_categorical(cat):
        return [normalize_token(cat.codes),
                normalize_token(cat.categories),
                cat.ordered]

    @partial(normalize_token.register, pd.Series)
    def normalize_series(s):
        return [s.name, s.dtype,
                normalize_token(s._data.blocks[0].values),
                normalize_token(s.index)]

    @partial(normalize_token.register, pd.DataFrame)
    def normalize_dataframe(df):
        data = [block.values for block in df._data.blocks]
        data += [df.columns, df.index]
        return list(map(normalize_token, data))


with ignoring(ImportError):
    import numpy as np
    @partial(normalize_token.register, np.ndarray)
    def normalize_array(x):
        if not x.shape:
            return (str(x), x.dtype)
        if x.dtype.hasobject:
            try:
                data = md5('-'.join(x.flat)).hexdigest()
            except TypeError:
                data = md5(b'-'.join([str(item).encode() for item in x.flat])).hexdigest()
        else:
            try:
                data = md5(x.ravel().view('i1').data).hexdigest()
            except (BufferError, AttributeError, ValueError):
                data = md5(x.copy().ravel().view('i1').data).hexdigest()
        return (data, x.dtype, x.shape, x.strides)


def tokenize(*args):
    """ Deterministic token

    >>> tokenize([1, 2, '3'])
    '9d71491b50023b06fc76928e6eddb952'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    return md5(str(tuple(map(normalize_token, args))).encode()).hexdigest()

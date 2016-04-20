from __future__ import absolute_import, division, print_function

from functools import partial
from hashlib import md5
from operator import attrgetter
import os
import uuid
import warnings

from toolz import merge, groupby, curry, identity
from toolz.functoolz import Compose

from .compatibility import bind_method, unicode
from .context import _globals
from .utils import Dispatch, ignoring

__all__ = ("Base", "is_dask_collection", "compute", "normalize_token",
           "tokenize", "visualize")


class DaskInterface(object):
    """Minimal interface required for dask-like duck typing"""
    def _dask_optimize_(self, dsk, keys, **kwargs):
        """Return an optimized dask graph"""
        raise NotImplementedError()

    def _dask_finalize_(self, results):
        """Finalize the computed keys into an output object"""
        raise NotImplementedError()

    def _dask_default_get_(self, dsk, key, **kwargs):
        """The default scheduler `get` to use for this object"""
        raise NotImplementedError()

    def _dask_keys_(self):
        """The keys for the dask graph"""
        raise NotImplementedError()

    def _dask_graph_(self):
        """The dask graph"""
        raise NotImplementedError()


def is_dask_collection(x):
    """Return if the object is a dask collection"""
    return isinstance(x, DaskInterface) or (hasattr(x, '_dask_optimize_') and
                                            hasattr(x, '_dask_finalize_') and
                                            hasattr(x, '_dask_default_get_') and
                                            hasattr(x, '_dask_keys_') and
                                            hasattr(x, '_dask_graph_'))


class Base(DaskInterface):
    """Base class for dask collections"""

    def _dask_graph_(self):
        return self.dask

    def _dask_keys_(self):
        return self._keys()

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
        get = get or _globals['get'] or cls._dask_default_get_
        dsk2 = cls._dask_optimize_(dsk, keys, **kwargs)
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
    is_collection = list(map(is_dask_collection, args))
    variables = [a for a, p in zip(args, is_collection) if p]
    if not variables:
        return args
    groups = groupby(attrgetter('_dask_optimize_'), variables)

    get = kwargs.pop('get', _globals['get'])

    if not get:
        get = variables[0]._dask_default_get_
        if not all(a._dask_default_get_ == get for a in variables):
            raise ValueError("Compute called on multiple collections with "
                             "differing default schedulers. Please specify a "
                             "scheduler `get` function using either "
                             "the `get` kwarg or globally with `set_options`.")

    dsk = merge([opt(merge([v._dask_graph_() for v in val]),
                     [v._dask_keys_() for v in val], **kwargs)
                for opt, val in groups.items()])
    keys = [var._dask_keys_() for var in variables]
    results = get(dsk, keys, **kwargs)

    results = iter(results)
    return tuple(a if not p else a._dask_finalize_(next(results))
                 for a, p in zip(args, is_collection))


def visualize(*args, **kwargs):
    dsks = [arg for arg in args if isinstance(arg, dict)]
    args = [arg for arg in args if is_dask_collection(arg)]
    filename = kwargs.pop('filename', 'mydask')
    optimize_graph = kwargs.pop('optimize_graph', False)
    from dask.dot import dot_graph
    if optimize_graph:
        dsks.extend([arg._dask_optimize_(arg._dask_graph_(), arg._dask_keys_())
                     for arg in args])
    else:
        dsks.extend([arg._dask_graph_() for arg in args])
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
normalize_token.register((int, float, str, unicode, bytes, type(None), type,
                          slice),
                         identity)


@partial(normalize_token.register, dict)
def normalize_dict(d):
    return normalize_token(sorted(d.items()))


@partial(normalize_token.register, (tuple, list, set))
def normalize_seq(seq):
    return type(seq).__name__, list(map(normalize_token, seq))


@partial(normalize_token.register, object)
def normalize_object(o):
    keys = getattr(o, '_dask_keys_', None)
    if keys is not None:
        return type(o).__name__, keys()
    if callable(o):
        return normalize_function(o)
    else:
        return uuid.uuid4().hex


@partial(normalize_token.register, Base)
def normalize_base(b):
    return type(b).__name__, b._dask_keys_()


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
        if hasattr(x, 'mode') and hasattr(x, 'filename'):
            return x.filename, os.path.getmtime(x.filename), x.dtype, x.shape
        if x.dtype.hasobject:
            try:
                data = md5('-'.join(x.flat).encode('utf-8')).hexdigest()
            except TypeError:
                data = md5(b'-'.join([str(item).encode() for item in x.flat])).hexdigest()
        else:
            try:
                data = md5(x.ravel().view('i1').data).hexdigest()
            except (BufferError, AttributeError, ValueError):
                data = md5(x.copy().ravel().view('i1').data).hexdigest()
        return (data, x.dtype, x.shape, x.strides)

    normalize_token.register(np.dtype, repr)
    normalize_token.register(np.generic, repr)


with ignoring(ImportError):
    from collections import OrderedDict
    @partial(normalize_token.register, OrderedDict)
    def normalize_ordered_dict(d):
        return type(d).__name__, normalize_token(list(d.items()))


def tokenize(*args, **kwargs):
    """ Deterministic token

    >>> tokenize([1, 2, '3'])
    '7d6a880cd9ec03506eee6973ff551339'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    if kwargs:
        args = args + (kwargs,)
    return md5(str(tuple(map(normalize_token, args))).encode()).hexdigest()

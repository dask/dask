from __future__ import absolute_import, division, print_function

from collections import OrderedDict
from functools import partial
from hashlib import md5
from operator import attrgetter
import pickle
import os
import uuid

from toolz import merge, groupby, curry, identity
from toolz.functoolz import Compose

from .compatibility import bind_method, unicode
from .context import _globals
from .utils import Dispatch

__all__ = ("Base", "compute", "normalize_token", "tokenize", "visualize")


class Base(object):
    """Base class for dask collections"""

    def visualize(self, filename='mydask', format=None, optimize_graph=False,
                  **kwargs):
        """
        Render the computation of this object's task graph using graphviz.

        Requires ``graphviz`` to be installed.

        Parameters
        ----------
        filename : str or None, optional
            The name (without an extension) of the file to write to disk.  If
            `filename` is None, no file will be written, and we communicate
            with dot using only pipes.
        format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
            Format in which to write output file.  Default is 'png'.
        optimize_graph : bool, optional
            If True, the graph is optimized before rendering.  Otherwise,
            the graph is displayed as is. Default is False.
        **kwargs
           Additional keyword arguments to forward to ``to_graphviz``.

        Returns
        -------
        result : IPython.diplay.Image, IPython.display.SVG, or None
            See dask.dot.dot_graph for more information.

        See also
        --------
        dask.base.visualize
        dask.dot.dot_graph

        Notes
        -----
        For more information on optimization see here:

        http://dask.pydata.org/en/latest/optimize.html
        """
        return visualize(self, filename=filename, format=format,
                         optimize_graph=optimize_graph, **kwargs)

    def compute(self, **kwargs):
        """Compute several dask collections at once.

        Parameters
        ----------
        get : callable, optional
            A scheduler ``get`` function to use. If not provided, the default
            is to check the global settings first, and then fall back to
            the collection defaults.
        optimize_graph : bool, optional
            If True [default], the graph is optimized before computation.
            Otherwise the graph is run as is. This can be useful for debugging.
        kwargs
            Extra keywords to forward to the scheduler ``get`` function.
        """
        return compute(self, **kwargs)[0]

    @classmethod
    def _get(cls, dsk, keys, get=None, **kwargs):
        get = get or _globals['get'] or cls._default_get
        dsk2 = cls._optimize(dsk, keys, **kwargs)
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

        if name in ('abs', 'invert', 'neg', 'pos'):
            bind_method(cls, meth, cls._get_unary_operator(op))
        else:
            bind_method(cls, meth, cls._get_binary_operator(op))

            if name in ('eq', 'gt', 'ge', 'lt', 'le', 'ne', 'getitem'):
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

    Parameters
    ----------
    args : object
        Any number of objects. If the object is a dask collection, it's
        computed and the result is returned. Otherwise it's passed through
        unchanged.
    get : callable, optional
        A scheduler ``get`` function to use. If not provided, the default is
        to check the global settings first, and then fall back to defaults for
        the collections.
    optimize_graph : bool, optional
        If True [default], the optimizations for each collection are applied
        before computation. Otherwise the graph is run as is. This can be
        useful for debugging.
    kwargs
        Extra keywords to forward to the scheduler ``get`` function.

    Examples
    --------
    >>> import dask.array as da
    >>> a = da.arange(10, chunks=2).sum()
    >>> b = da.arange(10, chunks=2).mean()
    >>> compute(a, b)
    (45, 4.5)
    """
    variables = [a for a in args if isinstance(a, Base)]
    if not variables:
        return args

    get = kwargs.pop('get', None) or _globals['get']
    optimizations = (kwargs.pop('optimizations', None) or
                     _globals.get('optimizations', []))

    if not get:
        get = variables[0]._default_get
        if not all(a._default_get == get for a in variables):
            raise ValueError("Compute called on multiple collections with "
                             "differing default schedulers. Please specify a "
                             "scheduler `get` function using either "
                             "the `get` kwarg or globally with `set_options`.")

    if kwargs.get('optimize_graph', True):
        groups = groupby(attrgetter('_optimize'), variables)
        groups = {opt: [merge([v.dask for v in val]),
                        [v._keys() for v in val]]
                  for opt, val in groups.items()}
        for opt in optimizations:
            groups = {k: [opt(dsk, keys), keys]
                      for k, (dsk, keys) in groups.items()}
        dsk = merge([opt(dsk, keys, **kwargs)
                    for opt, (dsk, keys) in groups.items()])
    else:
        dsk = merge(var.dask for var in variables)
    keys = [var._keys() for var in variables]
    results = get(dsk, keys, **kwargs)

    results_iter = iter(results)
    return tuple(a if not isinstance(a, Base)
                 else a._finalize(next(results_iter))
                 for a in args)


def visualize(*args, **kwargs):
    """
    Visualize several dask graphs at once.

    Requires ``graphviz`` to be installed. All options that are not the dask
    graph(s) should be passed as keyword arguments.

    Parameters
    ----------
    dsk : dict(s) or collection(s)
        The dask graph(s) to visualize.
    filename : str or None, optional
        The name (without an extension) of the file to write to disk.  If
        `filename` is None, no file will be written, and we communicate
        with dot using only pipes.
    format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
        Format in which to write output file.  Default is 'png'.
    optimize_graph : bool, optional
        If True, the graph is optimized before rendering.  Otherwise,
        the graph is displayed as is. Default is False.
    **kwargs
       Additional keyword arguments to forward to ``to_graphviz``.

    Returns
    -------
    result : IPython.diplay.Image, IPython.display.SVG, or None
        See dask.dot.dot_graph for more information.

    See also
    --------
    dask.dot.dot_graph

    Notes
    -----
    For more information on optimization see here:

    http://dask.pydata.org/en/latest/optimize.html
    """

    dsks = [arg for arg in args if isinstance(arg, dict)]
    args = [arg for arg in args if isinstance(arg, Base)]
    filename = kwargs.pop('filename', 'mydask')
    optimize_graph = kwargs.pop('optimize_graph', False)
    from dask.dot import dot_graph
    if optimize_graph:
        dsks.extend([arg._optimize(arg.dask, arg._keys()) for arg in args])
    else:
        dsks.extend([arg.dask for arg in args])
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
        try:
            result = pickle.dumps(func, protocol=0)
            if b'__main__' not in result:  # abort on dynamic functions
                return result
        except:
            pass
        try:
            import cloudpickle
            return cloudpickle.dumps(func, protocol=0)
        except:
            return str(func)


normalize_token = Dispatch()
normalize_token.register((int, float, str, unicode, bytes, type(None), type,
                          slice),
                         identity)


@normalize_token.register(dict)
def normalize_dict(d):
    return normalize_token(sorted(d.items(), key=str))


@normalize_token.register(OrderedDict)
def normalize_ordered_dict(d):
    return type(d).__name__, normalize_token(list(d.items()))


@normalize_token.register((tuple, list, set))
def normalize_seq(seq):
    return type(seq).__name__, list(map(normalize_token, seq))


@normalize_token.register(object)
def normalize_object(o):
    if callable(o):
        return normalize_function(o)
    else:
        return uuid.uuid4().hex


@normalize_token.register(Base)
def normalize_base(b):
    return type(b).__name__, b.key


@normalize_token.register_lazy("pandas")
def register_pandas():
    import pandas as pd

    @normalize_token.register(pd.Index)
    def normalize_index(ind):
        return [ind.name, normalize_token(ind.values)]

    @normalize_token.register(pd.Categorical)
    def normalize_categorical(cat):
        return [normalize_token(cat.codes),
                normalize_token(cat.categories),
                cat.ordered]

    @normalize_token.register(pd.Series)
    def normalize_series(s):
        return [s.name, s.dtype,
                normalize_token(s._data.blocks[0].values),
                normalize_token(s.index)]

    @normalize_token.register(pd.DataFrame)
    def normalize_dataframe(df):
        data = [block.values for block in df._data.blocks]
        data += [df.columns, df.index]
        return list(map(normalize_token, data))


@normalize_token.register_lazy("numpy")
def register_numpy():
    import numpy as np

    @normalize_token.register(np.ndarray)
    def normalize_array(x):
        if not x.shape:
            return (str(x), x.dtype)
        if hasattr(x, 'mode') and getattr(x, 'filename', None):
            if hasattr(x.base, 'ctypes'):
                offset = (x.ctypes.get_as_parameter().value -
                          x.base.ctypes.get_as_parameter().value)
            else:
                offset = 0  # root memmap's have mmap object as base
            return (x.filename, os.path.getmtime(x.filename), x.dtype,
                    x.shape, x.strides, offset)
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

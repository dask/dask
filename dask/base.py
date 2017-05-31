from __future__ import absolute_import, division, print_function

from collections import OrderedDict, Iterator
import copy
from functools import partial
from hashlib import md5
import inspect
import pickle
import os
import uuid

from toolz import merge, groupby, curry, identity
from toolz.functoolz import Compose

from . import sharedict
from .compatibility import bind_method, unicode, PY3
from .context import _globals
from .core import flatten
from .hashing import hash_buffer_hex
from .utils import Dispatch, ensure_dict
from .sharedict import ShareDict

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

    def persist(self, **kwargs):
        """ Persist this dask collection into memory

        See ``dask.base.persist`` for full docstring
        """
        (result,) = persist(self, **kwargs)
        return result

    def compute(self, **kwargs):
        """ Compute this dask collection

        This turns a lazy Dask collection into its in-memory equivalent.
        For example a Dask.array turns into a NumPy array and a Dask.dataframe
        turns into a Pandas dataframe.  The entire dataset must fit into memory
        before calling this operation.

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
        (result,) = compute(self, traverse=False, **kwargs)
        return result

    @classmethod
    def _get(cls, dsk, keys, get=None, **kwargs):
        get = get or _globals['get'] or cls._default_get
        dsk2 = optimization_function(cls)(ensure_dict(dsk), keys, **kwargs)
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
        Any number of objects. If it is a dask object, it's computed and the
        result is returned. By default, python builtin collections are also
        traversed to look for dask objects (for more information see the
        ``traverse`` keyword). Non-dask arguments are passed through unchanged.
    traverse : bool, optional
        By default dask traverses builtin python collections looking for dask
        objects passed to ``compute``. For large collections this can be
        expensive. If none of the arguments contain any dask objects, set
        ``traverse=False`` to avoid doing this traversal.
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

    By default, dask objects inside python collections will also be computed:

    >>> compute({'a': a, 'b': b, 'c': 1})  # doctest: +SKIP
    ({'a': 45, 'b': 4.5, 'c': 1},)
    """
    from dask.delayed import delayed
    traverse = kwargs.pop('traverse', True)
    if traverse:
        args = tuple(delayed(a)
                     if isinstance(a, (list, set, tuple, dict, Iterator))
                     else a for a in args)

    optimize_graph = kwargs.pop('optimize_graph', True)
    variables = [a for a in args if isinstance(a, Base)]
    if not variables:
        return args

    get = kwargs.pop('get', None) or _globals['get']

    if not get:
        get = variables[0]._default_get
        if not all(a._default_get == get for a in variables):
            raise ValueError("Compute called on multiple collections with "
                             "differing default schedulers. Please specify a "
                             "scheduler `get` function using either "
                             "the `get` kwarg or globally with `set_options`.")

    dsk = collections_to_dsk(variables, optimize_graph, **kwargs)
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
        dsks.extend([optimization_function(arg)(ensure_dict(arg.dask), arg._keys())
                     for arg in args])
    else:
        dsks.extend([arg.dask for arg in args])
    dsk = merge(dsks)

    return dot_graph(dsk, filename=filename, **kwargs)


function_cache = {}


def normalize_function(func):
    try:
        return function_cache[func]
    except KeyError:
        result = _normalize_function(func)
        if len(function_cache) >= 500:  # clear half of cache if full
            for k in list(function_cache)[::2]:
                del function_cache[k]
        function_cache[func] = result
        return result
    except TypeError:  # not hashable
        return _normalize_function(func)


def _normalize_function(func):
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
                          slice, complex),
                         identity)


@normalize_token.register(dict)
def normalize_dict(d):
    return normalize_token(sorted(d.items(), key=str))


@normalize_token.register(OrderedDict)
def normalize_ordered_dict(d):
    return type(d).__name__, normalize_token(list(d.items()))


@normalize_token.register(set)
def normalize_set(s):
    return normalize_token(sorted(s, key=str))


@normalize_token.register((tuple, list))
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
                data = hash_buffer_hex('-'.join(x.flat).encode('utf-8'))
            except TypeError:
                data = hash_buffer_hex(b'-'.join([unicode(item).encode('utf-8') for item in
                                                  x.flat]))
        else:
            try:
                data = hash_buffer_hex(x.ravel(order='K').view('i1'))
            except (BufferError, AttributeError, ValueError):
                data = hash_buffer_hex(x.copy().ravel(order='K').view('i1'))
        return (data, x.dtype, x.shape, x.strides)

    normalize_token.register(np.dtype, repr)
    normalize_token.register(np.generic, repr)

    @normalize_token.register(np.ufunc)
    def normalize_ufunc(x):
        try:
            name = x.__name__
            if getattr(np, name) is x:
                return 'np.' + name
        except:
            return normalize_function(x)


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


def dont_optimize(dsk, keys):
    return dsk


def optimization_function(obj):
    if isinstance(obj, type):
        cls = obj
    else:
        cls = type(obj)
    name = cls.__name__.lower() + '_optimize' # dask.set_options(array_optimize=foo)
    if name in _globals:
        return _globals[name] or dont_optimize
    try:
        return cls._optimize
    except AttributeError:
        return dont_optimize


def collections_to_dsk(collections, optimize_graph=True, **kwargs):
    """
    Convert many collections into a single dask graph, after optimization
    """
    optimizations = (kwargs.pop('optimizations', None) or
                     _globals.get('optimizations', []))
    if optimize_graph:
        groups = groupby(optimization_function, collections)
        groups = {opt: _extract_graph_and_keys(val)
                  for opt, val in groups.items()}
        for opt in optimizations:
            groups = {k: [opt(ensure_dict(dsk), keys), keys]
                      for k, (dsk, keys) in groups.items()}
        dsk = merge([opt(dsk, keys, **kwargs)
                     for opt, (dsk, keys) in groups.items()])
    else:
        dsk = ensure_dict(sharedict.merge(*[c.dask for c in collections]))

    return dsk


def _extract_graph_and_keys(vals):
    """Given a list of dask vals, return a single graph and a list of keys such
    that ``get(dsk, keys)`` is equivalent to ``[v.compute() v in vals]``."""
    dsk = {}
    keys = []
    for v in vals:
        d = v.dask
        if type(d) is ShareDict:
            for dd in d.dicts.values():
                dsk.update(dd)
        else:
            dsk.update(v.dask)
        keys.append(v._keys())

    return dsk, keys


def redict_collection(c, dsk):
    cc = copy.copy(c)
    cc.dask = dsk
    return cc


def persist(*args, **kwargs):
    """ Persist multiple Dask collections into memory

    This turns lazy Dask collections into Dask collections with the same
    metadata, but now with their results fully computed or actively computing
    in the background.

    For example a lazy dask.array built up from many lazy calls will now be a
    dask.array of the same shape, dtype, chunks, etc., but now with all of
    those previously lazy tasks either computed in memory as many small NumPy
    arrays (in the single-machine case) or asynchronously running in the
    background on a cluster (in the distributed case).

    This function operates differently if a ``dask.distributed.Client`` exists
    and is connected to a distributed scheduler.  In this case this function
    will return as soon as the task graph has been submitted to the cluster,
    but before the computations have completed.  Computations will continue
    asynchronously in the background.  When using this function with the single
    machine scheduler it blocks until the computations have finished.

    When using Dask on a single machine you should ensure that the dataset fits
    entirely within memory.

    Examples
    --------
    >>> df = dd.read_csv('/path/to/*.csv')  # doctest: +SKIP
    >>> df = df[df.name == 'Alice']  # doctest: +SKIP
    >>> df['in-debt'] = df.balance < 0  # doctest: +SKIP
    >>> df = df.persist()  # triggers computation  # doctest: +SKIP

    >>> df.value().min()  # future computations are now fast  # doctest: +SKIP
    -10
    >>> df.value().max()  # doctest: +SKIP
    100

    >>> from dask import persist  # use persist function on multiple collections
    >>> a, b = persist(a, b)  # doctest: +SKIP

    Parameters
    ----------
    *args: Dask collections
    get : callable, optional
        A scheduler ``get`` function to use. If not provided, the default
        is to check the global settings first, and then fall back to
        the collection defaults.
    optimize_graph : bool, optional
        If True [default], the graph is optimized before computation.
        Otherwise the graph is run as is. This can be useful for debugging.
    **kwargs
        Extra keywords to forward to the scheduler ``get`` function.

    Returns
    -------
    New dask collections backed by in-memory data
    """
    collections = [a for a in args if isinstance(a, Base)]
    if not collections:
        return args

    get = kwargs.pop('get', None) or _globals['get']

    if inspect.ismethod(get):
        try:
            from distributed.client import default_client
        except ImportError:
            pass
        else:
            try:
                client = default_client()
            except ValueError:
                pass
            else:
                if client.get == _globals['get']:
                    collections = client.persist(collections, **kwargs)
                    if isinstance(collections, list):  # distributed is inconsistent here
                        collections = tuple(collections)
                    else:
                        collections = (collections,)
                    results_iter = iter(collections)
                    return tuple(a if not isinstance(a, Base)
                                 else next(results_iter)
                                 for a in args)

    optimize_graph = kwargs.pop('optimize_graph', True)

    if not get:
        get = collections[0]._default_get
        if not all(a._default_get == get for a in collections):
            raise ValueError("Compute called on multiple collections with "
                             "differing default schedulers. Please specify a "
                             "scheduler `get` function using either "
                             "the `get` kwarg or globally with `set_options`.")

    dsk = collections_to_dsk(collections, optimize_graph, **kwargs)
    keys = list(flatten([var._keys() for var in collections]))
    results = get(dsk, keys, **kwargs)

    d = dict(zip(keys, results))

    result = [redict_collection(c, {k: d[k]
                                    for k in flatten(c._keys())})
              for c in collections]
    results_iter = iter(result)
    return tuple(a if not isinstance(a, Base)
                 else next(results_iter)
                 for a in args)


if PY3:
    Base.persist.__doc__ = persist.__doc__

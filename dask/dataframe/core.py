from itertools import count
from math import ceil, sqrt
from functools import wraps
import toolz
import bisect
import os
from toolz import (merge, partial, accumulate, unique, first, dissoc, valmap,
        first, partition)
from operator import getitem, setitem
import pandas as pd
import numpy as np
import operator
from chest import Chest
import gzip
import bz2
from pframe import pframe
import bcolz

from .. import array as da
from ..optimize import cull, fuse
from .. import core
from ..array.core import partial_by_order
from ..async import get_sync
from ..threaded import get as get_threaded
from ..compatibility import unicode, apply
from ..utils import repr_long_list, IndexCallable


def concat(args):
    """ Generic concat operation """
    if not args:
        return args
    if isinstance(first(core.flatten(args)), np.ndarray):
        return da.core.rec_concatenate(args)
    if len(args) == 1:
        return args[0]
    if isinstance(args[0], (pd.DataFrame, pd.Series)):
        args2 = [arg for arg in args if len(arg)]
        if not args2:
            return args[0]
        return pd.concat(args2)
    if isinstance(args[0], (pd.Index)):
        args = [arg for arg in args if len(arg)]
        result = pd.concat(map(pd.Series, args))
        result = type(args[0])(result.values)
        result.name = args[0].name
        return result
    return args


def compute(*args, **kwargs):
    """ Compute multiple frames at once """
    if len(args) == 1 and isinstance(args[0], (tuple, list)):
        args = args[0]
    dsk = merge(*[arg.dask for arg in args])
    keys = [arg._keys() for arg in args]
    results = get(dsk, keys, **kwargs)

    return list(map(concat, results))


names = ('f-%d' % i for i in count(1))


class Scalar(object):
    """ A Dask-thing to represent a scalar

    TODO: Clean up this abstraction
    """
    def __init__(self, dsk, _name):
        self.dask = dsk
        self._name = _name
        self.blockdivs = []

    def _keys(self):
        return [(self._name, 0)]

    def compute(self, **kwargs):
        return compute(self, **kwargs)[0]


class _Frame(object):
    """ Superclass for DataFrame and Series """
    @property
    def npartitions(self):
        return len(self.blockdivs) + 1

    def compute(self, **kwargs):
        return compute(self, **kwargs)[0]

    def _keys(self):
        return [(self._name, i) for i in range(self.npartitions)]

    def _visualize(self, optimize_graph=False):
        from dask.dot import dot_graph
        if optimize_graph:
            dot_graph(optimize(self.dask, self._keys()))
        else:
            dot_graph(self.dask)

    @property
    def index(self):
        name = self._name + '-index'
        dsk = dict(((name, i), (getattr, key, 'index'))
                   for i, key in enumerate(self._keys()))
        return Index(merge(dsk, self.dask), name, None, self.blockdivs)

    def cache(self, cache=Chest):
        """ Evaluate Dataframe and store in local cache

        Uses chest by default to store data on disk
        """
        if callable(cache):
            cache = cache()

        # Evaluate and store in cache
        name = next(names)
        dsk = dict(((name, i), (setitem, cache, (tuple, list(key)), key))
                    for i, key in enumerate(self._keys()))
        get(merge(dsk, self.dask), list(dsk.keys()))

        # Create new Frame pointing to that cache
        dsk2 = dict((key, (getitem, cache, (tuple, list(key))))
                    for key in self._keys())
        return type(self)(dsk2, name, self.column_info, self.blockdivs)

    def drop_duplicates(self):
        chunk = lambda s: s.drop_duplicates()
        return aca(self, chunk=chunk, aggregate=chunk, columns=self.columns)

    def __len__(self):
        return reduction(self, len, np.sum).compute()

    def map_blocks(self, func, columns=None):
        """ Apply Python function on each DataFrame block

        Provide columns of the output if they are not the same as the input.
        """
        if columns is None:
            columns = self.column_info
        name = next(names)
        dsk = dict(((name, i), (func, (self._name, i)))
                    for i in range(self.npartitions))

        return type(self)(merge(dsk, self.dask), name,
                          columns, self.blockdivs)

    def head(self, n=10, compute=True):
        """ First n rows of the dataset

        Caveat, the only checks the first n rows of the first partition.
        """
        name = next(names)
        dsk = {(name, 0): (head, (self._name, 0), n)}

        result = type(self)(merge(self.dask, dsk), name,
                            self.column_info, [])

        if compute:
            result = result.compute()
        return result

    def _partition_of_index_value(self, val):
        """ In which partition does this value lie? """
        return bisect.bisect_right(self.blockdivs, val)

    def _loc(self, ind):
        """ Helper function for the .loc accessor """
        name = next(names)
        if not isinstance(ind, slice):
            part = self._partition_of_index_value(ind)
            dsk = {(name, 0): (lambda df: df.loc[ind], (self._name, part))}
            return type(self)(merge(self.dask, dsk), name,
                              self.column_info, [])
        else:
            assert ind.step in (None, 1)
            if ind.start:
                start = self._partition_of_index_value(ind.start)
            else:
                start = 0
            if ind.stop is not None:
                stop = self._partition_of_index_value(ind.stop)
            else:
                stop = self.npartitions - 1
            if stop == start:
                dsk = {(name, 0): (_loc, (self._name, start), ind.start, ind.stop)}
            else:
                dsk = merge(
                  {(name, 0): (_loc, (self._name, start), ind.start, None)},
                  dict(((name, i), (self._name, start + i))
                      for i in range(1, stop - start)),
                  {(name, stop - start): (_loc, (self._name, stop), None, ind.stop)})

            return type(self)(merge(self.dask, dsk), name, self.column_info,
                              self.blockdivs[start:stop])

    @property
    def loc(self):
        return IndexCallable(self._loc)

    @property
    def iloc(self):
        raise AttributeError("Dask Dataframe does not support iloc")


class Series(_Frame):
    """ Out-of-core Series object

    Mimics ``pandas.Series``.

    See Also
    --------

    dask.dataframe.DataFrame
    """
    _partition_type = pd.Series
    def __init__(self, dsk, _name, name, blockdivs):
        self.dask = dsk
        self._name = _name
        self.name = name
        self.blockdivs = blockdivs

    @property
    def dtype(self):
        return self.head().dtype

    @property
    def column_info(self):
        return self.name

    @property
    def columns(self):
        return (self.name,)

    def __repr__(self):
        return ("dd.Series<%s, blockdivs=%s>" %
                (self._name, repr_long_list(self.blockdivs)))

    def quantiles(self, q):
        """ Approximate quantiles of column

        q : list/array of floats
            Iterable of numbers ranging from 0 to 100 for the desired quantiles
        """
        return quantiles(self, q)

    def __getitem__(self, key):
        name = next(names)
        if isinstance(key, Series) and self.blockdivs == key.blockdivs:
            dsk = dict(((name, i), (operator.getitem, (self._name, i),
                                                       (key._name, i)))
                        for i in range(self.npartitions))
            return Series(merge(self.dask, key.dask, dsk), name,
                          self.name, self.blockdivs)
        raise NotImplementedError()

    def __abs__(self):
        return elemwise(operator.abs, self)
    def __add__(self, other):
        return elemwise(operator.add, self, other)
    def __radd__(self, other):
        return elemwise(operator.add, other, self)
    def __and__(self, other):
        return elemwise(operator.and_, self, other)
    def __rand__(self, other):
        return elemwise(operator.and_, other, self)
    def __div__(self, other):
        return elemwise(operator.div, self, other)
    def __rdiv__(self, other):
        return elemwise(operator.div, other, self)
    def __eq__(self, other):
        return elemwise(operator.eq, self, other)
    def __gt__(self, other):
        return elemwise(operator.gt, self, other)
    def __ge__(self, other):
        return elemwise(operator.ge, self, other)
    def __invert__(self):
        return elemwise(operator.inv, self)
    def __lt__(self, other):
        return elemwise(operator.lt, self, other)
    def __le__(self, other):
        return elemwise(operator.le, self, other)
    def __mod__(self, other):
        return elemwise(operator.mod, self, other)
    def __rmod__(self, other):
        return elemwise(operator.mod, other, self)
    def __mul__(self, other):
        return elemwise(operator.mul, self, other)
    def __rmul__(self, other):
        return elemwise(operator.mul, other, self)
    def __ne__(self, other):
        return elemwise(operator.ne, self, other)
    def __neg__(self):
        return elemwise(operator.neg, self)
    def __or__(self, other):
        return elemwise(operator.or_, self, other)
    def __ror__(self, other):
        return elemwise(operator.or_, other, self)
    def __pow__(self, other):
        return elemwise(operator.pow, self, other)
    def __rpow__(self, other):
        return elemwise(operator.pow, other, self)
    def __sub__(self, other):
        return elemwise(operator.sub, self, other)
    def __rsub__(self, other):
        return elemwise(operator.sub, other, self)
    def __truediv__(self, other):
        return elemwise(operator.truediv, self, other)
    def __rtruediv__(self, other):
        return elemwise(operator.truediv, other, self)
    def __floordiv__(self, other):
        return elemwise(operator.floordiv, self, other)
    def __rfloordiv__(self, other):
        return elemwise(operator.floordiv, other, self)
    def __xor__(self, other):
        return elemwise(operator.xor, self, other)
    def __rxor__(self, other):
        return elemwise(operator.xor, other, self)

    def sum(self):
        return reduction(self, pd.Series.sum, np.sum)
    def max(self):
        return reduction(self, pd.Series.max, np.max)
    def min(self):
        return reduction(self, pd.Series.min, np.min)
    def count(self):
        return reduction(self, pd.Series.count, np.sum)

    def mean(self):
        def chunk(ser):
            return (ser.sum(), ser.count())
        def agg(seq):
            sums, counts = list(zip(*seq))
            return 1.0 * sum(sums) / sum(counts)
        return reduction(self, chunk, agg)

    def var(self, ddof=1):
        def chunk(ser):
            return (ser.sum(), (ser**2).sum(), ser.count())
        def agg(seq):
            x, x2, n = list(zip(*seq))
            x = float(sum(x))
            x2 = float(sum(x2))
            n = sum(n)
            result = (x2 / n) - (x / n)**2
            if ddof:
                result = result * n / (n - ddof)
            return result
        return reduction(self, chunk, agg)

    def std(self, ddof=1):
        name = next(names)
        f = self.var(ddof=ddof)
        dsk = {(name, 0): (sqrt, (f._name, 0))}
        return Scalar(merge(f.dask, dsk), name)

    def value_counts(self):
        chunk = lambda s: s.value_counts()
        agg = lambda s: s.groupby(level=0).sum()
        return aca(self, chunk=chunk, aggregate=agg, columns=self.columns)

    def isin(self, other):
        return elemwise(pd.Series.isin, self, other)

    @wraps(pd.Series.map)
    def map(self, arg, na_action=None):
        return elemwise(pd.Series.map, self, arg, na_action, name=self.name)


class Index(Series):
    pass


class DataFrame(_Frame):
    """
    Implements out-of-core DataFrame as a sequence of pandas DataFrames

    This is a work in progress.  It is buggy and far from complete.
    Please do not use it yet.

    Parameters
    ----------

    dask: dict
        The dask graph to compute this Dataframe
    name: str
        The key prefix that specifies which keys in the dask comprise this
        particular DataFrame
    columns: list of strings
        Column names.  This metadata aids usability
    blockdivs: tuple of index values
        Values along which we partition our blocks on the index
    """
    _partition_type = pd.DataFrame
    def __init__(self, dask, name, columns, blockdivs):
        self.dask = dask
        self._name = name
        self.columns = tuple(columns)
        self.blockdivs = tuple(blockdivs)

    def __getitem__(self, key):
        if isinstance(key, (str, unicode)):
            name = self._name + '.' + key
            if key in self.columns:
                dsk = dict(((name, i), (operator.getitem, (self._name, i), key))
                            for i in range(self.npartitions))
                return Series(merge(self.dask, dsk), name,
                              key, self.blockdivs)
        if isinstance(key, list):
            name = '%s[%s]' % (self._name, str(key))
            if all(k in self.columns for k in key):
                dsk = dict(((name, i), (operator.getitem,
                                         (self._name, i),
                                         (list, key)))
                            for i in range(self.npartitions))
                return DataFrame(merge(self.dask, dsk), name,
                                 key, self.blockdivs)
        if isinstance(key, Series) and self.blockdivs == key.blockdivs:
            name = next(names)
            dsk = dict(((name, i), (operator.getitem, (self._name, i),
                                                       (key._name, i)))
                        for i in range(self.npartitions))
            return DataFrame(merge(self.dask, key.dask, dsk), name,
                             self.columns, self.blockdivs)
        raise NotImplementedError()

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except NotImplementedError:
                raise AttributeError()

    def __dir__(self):
        return sorted(set(list(dir(type(self))) + list(self.columns)))

    def __repr__(self):
        return ("dd.DataFrame<%s, blockdivs=%s>" %
                (self._name, repr_long_list(self.blockdivs)))

    @property
    def dtypes(self):
        return get(self.dask, self._keys()[0]).dtypes

    def set_index(self, other, **kwargs):
        return set_index(self, other, **kwargs)

    def set_partition(self, column, blockdivs, **kwargs):
        """ Set explicit blockdivs for new column index

        >>> df2 = df.set_partition('new-index-column', blockdivs=[10, 20, 50])  # doctest: +SKIP

        See also:
            set_index
        """
        return set_partition(self, column, blockdivs, **kwargs)

    @property
    def column_info(self):
        return self.columns

    def groupby(self, key, **kwargs):
        return GroupBy(self, key, **kwargs)

    def categorize(self, columns=None, **kwargs):
        return categorize(self, columns, **kwargs)

    @wraps(pd.DataFrame.assign)
    def assign(self, **kwargs):
        pairs = list(sum(kwargs.items(), ()))

        # Figure out columns of the output
        df = pd.DataFrame(columns=self.columns)
        df2 = df.assign(**dict((k, []) for k in kwargs))

        return elemwise(_assign, self, *pairs, columns=list(df2.columns))


def _assign(df, *pairs):
    kwargs = dict(partition(2, pairs))
    return df.assign(**kwargs)

def _loc(df, start, stop):
    return df.loc[slice(start, stop)]

def head(x, n):
    """ First n elements of dask.Dataframe or dask.Series """
    return x.head(n)


def consistent_name(names):
    """ New name for series in elementwise operation

    If all truthy names are the same, choose that one, otherwise, choose None
    """
    allnames = set()
    for name in names:
        if name is None:
            continue
        if isinstance(name, (tuple, list)):
            allnames.update(name)
        else:
            allnames.add(name)

    if len(allnames) == 1:
        return first(allnames)
    else:
        return None


def elemwise(op, *args, **kwargs):
    """ Elementwise operation for dask.Dataframes """
    columns = kwargs.get('columns', None)
    name = kwargs.get('name', None)

    _name = next(names)

    frames = [arg for arg in args if isinstance(arg, _Frame)]
    other = [(i, arg) for i, arg in enumerate(args)
                      if not isinstance(arg, _Frame)]

    if other:
        op2 = partial_by_order(op, other)
    else:
        op2 = op

    assert all(f.blockdivs == frames[0].blockdivs for f in frames)
    assert all(f.npartitions == frames[0].npartitions for f in frames)

    dsk = dict(((_name, i), (op2,) + frs)
                for i, frs in enumerate(zip(*[f._keys() for f in frames])))

    if columns is not None:
        return DataFrame(merge(dsk, *[f.dask for f in frames]),
                         _name, columns, frames[0].blockdivs)
    else:
        column_name = name or consistent_name(n for f in frames
                                                 for n in f.columns)
        return Series(merge(dsk, *[f.dask for f in frames]),
                      _name, column_name, frames[0].blockdivs)


def reduction(x, chunk, aggregate):
    """ General version of reductions

    >>> reduction(my_frame, np.sum, np.sum)  # doctest: +SKIP
    """
    a = next(names)
    dsk = dict(((a, i), (chunk, (x._name, i)))
                for i in range(x.npartitions))

    b = next(names)
    dsk2 = {(b, 0): (aggregate, (tuple, [(a,i) for i in range(x.npartitions)]))}

    return Scalar(merge(x.dask, dsk, dsk2), b)


opens = {'gz': gzip.open, 'bz2': bz2.BZ2File}

def linecount(fn):
    """ Count the number of lines in a textfile

    We need this to build the graph for read_csv.  This is much faster than
    actually parsing the file, but still costly.
    """
    extension = os.path.splitext(fn)[-1].lstrip('.')
    myopen = opens.get(extension, open)
    f = myopen(os.path.expanduser(fn))
    result = toolz.count(f)
    f.close()
    return result


read_csv_names = ('_readcsv-%d' % i for i in count(1))

def get_chunk(x, start):
    if isinstance(x, tuple):
        x = x[1]
    df = x.get_chunk()
    df.index += start
    return df, x


@wraps(pd.read_csv)
def read_csv(fn, *args, **kwargs):
    chunksize = kwargs.pop('chunksize', 2**16)
    categorize = kwargs.pop('categorize', None)
    index = kwargs.pop('index', None)
    if index and categorize == None:
        categorize = True
    header = kwargs.get('header', 1)

    nlines = linecount(fn) - header
    nchunks = int(ceil(1.0 * nlines / chunksize))

    read = next(read_csv_names)

    blockdivs = tuple(range(chunksize, nlines, chunksize))

    one_chunk = pd.read_csv(fn, *args, nrows=100, **kwargs)

    cols = []

    if categorize or index:
        if categorize:
            category_columns = [c for c in one_chunk.dtypes.index
                                   if one_chunk.dtypes[c] == 'O']
        else:
            category_columns = []
        cols = category_columns + ([index] if index else [])
        d = read_csv(fn, *args, **merge(kwargs,
                                        dict(chunksize=chunksize,
                                             usecols=cols,
                                             categorize=False,
                                             parse_dates=None)))
        categories = [d[c].drop_duplicates() for c in category_columns]
        if index:
            quantiles = d[index].quantiles(np.linspace(0, 100, nchunks + 1)[1:-1])
            result = compute(quantiles, *categories)
            quantiles, categories = result[0], result[1:]
        else:
            categories = compute(*categories)
        categories = dict(zip(category_columns, categories))

    kwargs['chunksize'] = chunksize
    load = {(read, -1): (partial(pd.read_csv, *args, **kwargs), fn)}
    load.update(dict(((read, i), (get_chunk, (read, i-1), chunksize*i))
                     for i in range(nchunks)))

    name = next(names)

    dsk = dict(((name, i), (getitem, (read, i), 0))
                for i in range(nchunks))

    result = DataFrame(merge(dsk, load), name, one_chunk.columns, blockdivs)

    if categorize:
        func = partial(categorize_block, categories=categories)
        result = result.map_blocks(func, columns=result.columns)

    if index:
        result = set_partition(result, index, quantiles)

    return result


from_array_names = ('from-array-%d' % i for i in count(1))


def from_array(x, chunksize=50000):
    """ Read dask Dataframe from any slicable array with record dtype

    Uses getitem syntax to pull slices out of the array.  The array need not be
    a NumPy array but must support slicing syntax

        x[50000:100000]

    and have a record dtype

        x.dtype == [('name', 'O'), ('balance', 'i8')]

    """
    columns = tuple(x.dtype.names)
    blockdivs = tuple(range(chunksize, len(x), chunksize))
    name = next(from_array_names)
    dsk = dict(((name, i), (pd.DataFrame,
                             (getitem, x,
                             (slice(i * chunksize, (i + 1) * chunksize),))))
            for i in range(0, int(ceil(float(len(x)) / chunksize))))

    return DataFrame(dsk, name, columns, blockdivs)


from pframe.categories import reapply_categories

def from_bcolz(x, chunksize=None, categorize=True, index=None, **kwargs):
    """ Read dask Dataframe from bcolz.ctable

    Parameters
    ----------

    x : bcolz.ctable
        Input data
    chunksize : int (optional)
        The size of blocks to pull out from ctable.  Ideally as large as can
        comfortably fit in memory
    categorize : bool (defaults to True)
        Automatically categorize all string dtypes
    index : string (optional)
        Column to make the index

    See Also
    --------

    from_array: more generic function not optimized for bcolz
    """
    bc_chunklen = max(x[name].chunklen for name in x.names)
    if chunksize is None and bc_chunklen > 10000:
        chunksize = bc_chunklen

    categories = dict()
    if categorize:
        for name in x.names:
            if (np.issubdtype(x.dtype[name], np.string_) or
                np.issubdtype(x.dtype[name], np.unicode_) or
                np.issubdtype(x.dtype[name], np.object_)):
                a = da.from_array(x[name], chunks=(chunksize*len(x.names),))
                categories[name] = da.unique(a)

    columns = tuple(x.dtype.names)
    blockdivs = tuple(range(chunksize, len(x), chunksize))
    new_name = next(from_array_names)
    dsk = dict(((new_name, i),
                (dataframe_from_ctable,
                  x,
                  (slice(i * chunksize, (i + 1) * chunksize),),
                  None, categories))
           for i in range(0, int(ceil(float(len(x)) / chunksize))))

    result = DataFrame(dsk, new_name, columns, blockdivs)

    if index:
        assert index in x.names
        a = da.from_array(x[index], chunks=(chunksize*len(x.names),))
        q = np.linspace(1, 100, len(x) / chunksize + 2)[1:-1]
        blockdivs = da.percentile(a, q).compute()
        return set_partition(result, index, blockdivs, **kwargs)
    else:
        return result


def dataframe_from_ctable(x, slc, columns=None, categories=None):
    """ Get DataFrame from bcolz.ctable

    Parameters
    ----------

    x: bcolz.ctable
    slc: slice
    columns: list of column names or None

    >>> x = bcolz.ctable([[1, 2, 3, 4], [10, 20, 30, 40]], names=['a', 'b'])
    >>> dataframe_from_ctable(x, slice(1, 3))
       a   b
    0  2  20
    1  3  30

    >>> dataframe_from_ctable(x, slice(1, 3), columns=['b'])
        b
    0  20
    1  30

    >>> dataframe_from_ctable(x, slice(1, 3), columns='b')
    0    20
    1    30
    Name: b, dtype: int64

    """
    if columns is not None:
        if isinstance(columns, tuple):
            columns = list(columns)
        x = x[columns]

    name = next(names)

    if isinstance(x, bcolz.ctable):
        chunks = [x[name][slc] for name in x.names]
        if categories is not None:
            chunks = [pd.Categorical.from_codes(np.searchsorted(categories[name],
                                                                chunk),
                                                categories[name], True)
                       if name in categories else chunk
                       for name, chunk in zip(x.names, chunks)]
        return pd.DataFrame(dict(zip(x.names, chunks)))
    elif isinstance(x, bcolz.carray):
        chunk = x[slc]
        if categories is not None and columns and columns in categories:
            chunk = pd.Categorical.from_codes(
                        np.searchsorted(categories[columns], chunk),
                        categories[columns], True)
        return pd.Series(chunk, name=columns)


class GroupBy(object):
    def __init__(self, frame, index=None, **kwargs):
        self.frame = frame
        self.index = index
        self.kwargs = kwargs

        if isinstance(index, list):
            assert all(i in frame.columns for i in index)
        elif isinstance(index, Series):
            assert index.blockdivs == frame.blockdivs
        else:
            assert index in frame.columns

    def apply(self, func, columns=None):
        if isinstance(self.index, Series) and self.index._name == self.frame.index._name:
            f = self.frame
        else:
            f = set_index(self.frame, self.index, **self.kwargs)
        return f.map_blocks(lambda df: df.groupby(level=0).apply(func),
                            columns=columns)

    def __getitem__(self, key):
        if key in self.frame.columns:
            return SeriesGroupBy(self.frame, self.index, key)
        else:
            raise KeyError()

    def __dir__(self):
        return sorted(set(list(dir(type(self))) + list(self.frame.columns)))

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except KeyError:
                raise AttributeError()


class SeriesGroupBy(object):
    def __init__(self, frame, index, key, **kwargs):
        self.frame = frame
        self.index = index
        self.key = key
        self.kwargs = kwargs

    def apply(func, columns=None):
        f = set_index(self.frame, self.index, **self.kwargs)
        return f.map_blocks(lambda df:df.groupby(level=0)[self.key].apply(func),
                            columns=columns)

    def sum(self):
        chunk = lambda df, index: df.groupby(index)[self.key].sum()
        agg = lambda df: df.groupby(level=0).sum()
        return aca([self.frame, self.index],
                   chunk=chunk, aggregate=agg, columns=[self.key])

    def min(self):
        chunk = lambda df, index: df.groupby(index)[self.key].min()
        agg = lambda df: df.groupby(level=0).min()
        return aca([self.frame, self.index],
                   chunk=chunk, aggregate=agg, columns=[self.key])

    def max(self):
        chunk = lambda df, index: df.groupby(index)[self.key].max()
        agg = lambda df: df.groupby(level=0).max()
        return aca([self.frame, self.index],
                   chunk=chunk, aggregate=agg, columns=[self.key])

    def count(self):
        chunk = lambda df, index: df.groupby(index)[self.key].count()
        agg = lambda df: df.groupby(level=0).sum()
        return aca([self.frame, self.index],
                   chunk=chunk, aggregate=agg, columns=[self.key])

    def mean(self):
        def chunk(df, index):
            g = df.groupby(index)
            return g.agg({self.key: ['sum', 'count']})
        def agg(df):
            g = df.groupby(level=0)
            x = g.agg({(self.key, 'sum'): 'sum',
                       (self.key, 'count'): 'sum'})
            return 1.0 * x[self.key]['sum'] / x[self.key]['count']
        return aca([self.frame, self.index],
                   chunk=chunk, aggregate=agg, columns=[])


def apply_concat_apply(args, chunk=None, aggregate=None, columns=None):
    """ Apply a function to blocks, the concat, then apply again

    Parameters
    ----------

    args: dask.DataFrames
        All Dataframes should be partitioned and indexed equivalently
    chunk: function [block-per-arg] -> block
        Function to operate on each block of data
    aggregate: function concatenated-block -> block
        Function to operate on the concatenated result of chunk

    >>> def chunk(a_block, b_block):
    ...     pass

    >>> def agg(df):
    ...     pass

    >>> apply_concat_apply([a, b], chunk=chunk, aggregate=agg)  # doctest: +SKIP
    """
    if not isinstance(args, (tuple, list)):
        args = [args]
    assert all(arg.npartitions == args[0].npartitions
                for arg in args
                if isinstance(arg, _Frame))
    a = next(names)
    dsk = dict(((a, i), (apply, chunk, (list, [(x._name, i)
                                                if isinstance(x, _Frame)
                                                else x for x in args])))
                for i in range(args[0].npartitions))

    b = next(names)
    dsk2 = {(b, 0): (aggregate,
                      (pd.concat,
                        (list, [(a, i) for i in range(args[0].npartitions)])))}

    return type(args[0])(
            merge(dsk, dsk2, *[a.dask for a in args
                                      if isinstance(a, _Frame)]),
            b, columns, [])


aca = apply_concat_apply

def categorize_block(df, categories):
    """ Categorize a dataframe with given categories

    df: DataFrame
    categories: dict mapping column name to iterable of categories
    """
    df = df.copy()
    for col, vals in categories.items():
        df[col] = pd.Categorical(df[col], categories=vals,
                                    ordered=False, name=col)
    return df


def categorize(f, columns=None, **kwargs):
    """
    Convert columns of dask.frame to category dtype

    This greatly aids performance, both in-memory and in spilling to disk
    """
    if columns is None:
        dtypes = f.dtypes
        columns = [name for name, dt in zip(dtypes.index, dtypes.values)
                    if dt == 'O']
    if not isinstance(columns, (list, tuple)):
        columns = [columns]

    distincts = [f[col].drop_duplicates() for col in columns]
    values = compute(distincts, **kwargs)

    func = partial(categorize_block, categories=dict(zip(columns, values)))
    return f.map_blocks(func, columns=f.columns)


def quantiles(f, q, **kwargs):
    """ Approximate quantiles of column

    q : list/array of floats
        Iterable of numbers ranging from 0 to 100 for the desired quantiles
    """
    assert len(f.columns) == 1
    from dask.array.percentile import _percentile, merge_percentiles
    name = next(names)
    val_dsk = dict(((name, i), (_percentile, (getattr, key, 'values'), q))
                   for i, key in enumerate(f._keys()))
    name2 = next(names)
    len_dsk = dict(((name2, i), (len, key)) for i, key in enumerate(f._keys()))

    name3 = next(names)
    merge_dsk = {(name3, 0): (merge_percentiles, q, [q] * f.npartitions,
                                                sorted(val_dsk),
                                                sorted(len_dsk))}

    dsk = merge(f.dask, val_dsk, len_dsk, merge_dsk)
    return da.Array(dsk, name3, chunks=((len(q),),))


#################
# Optimizations #
#################


a, b, c, d, e = '~a', '~b', '~c', '~d', '~e'
from dask.rewrite import RuleSet, RewriteRule

rewrite_rules = RuleSet(
        # Merge column access into pframe loading
        RewriteRule((getitem, (pframe.get_partition, a, b), c),
                    (pframe.get_partition, a, b, c),
                    (a, b, c)),
        # Merge column access into bcolz loading
        RewriteRule((getitem, (dataframe_from_ctable, a, b, c, d), e),
                    (dataframe_from_ctable, a, b, e, d),
                    (a, b, c, d, e)))


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk2 = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2 = cull(dsk, [keys])
    dsk3 = fuse(dsk2)
    dsk4 = valmap(rewrite_rules.rewrite, dsk3)
    return dsk4


def get(dsk, keys, get=get_sync, **kwargs):
    """ Get function with optimizations specialized to dask.Dataframe """
    dsk2 = optimize(dsk, keys, **kwargs)
    return get(dsk2, keys, **kwargs)  # use synchronous scheduler for now


from .shuffle import set_index, set_partition

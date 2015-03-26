from itertools import count
from math import ceil, sqrt
from functools import wraps
import toolz
import bisect
import os
from toolz import merge, partial, accumulate, unique, first, dissoc
from operator import getitem, setitem
import pandas as pd
import numpy as np
import operator
from chest import Chest

from .. import array as da
from ..optimize import cull, fuse
from .. import core
from ..array.core import partial_by_order
from ..async import get_sync
from ..threaded import get as get_threaded
from ..compatibility import unicode, apply
from ..utils import repr_long_list, IndexCallable


def get(dsk, keys, get=get_sync, **kwargs):
    """ Get function with optimizations specialized to dask.frame """
    if isinstance(keys, list):
        dsk2 = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2 = cull(dsk, [keys])
    dsk3 = fuse(dsk2)
    return get(dsk3, keys, **kwargs)  # use synchronous scheduler for now


def concat(args):
    """ Generic concat operation """
    if not args:
        return args
    if isinstance(args[0], (pd.DataFrame, pd.Series)):
        args = [arg for arg in args if len(arg)]
        return pd.concat(args)
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

    return [concat(dfs) if arg.blockdivs else dfs[0]
            for arg, dfs in zip(args, results)]


names = ('f-%d' % i for i in count(1))


class Frame(object):
    """
    Dask.Frame implements a DataFrame as a sequence of pandas DataFrames

    Dask.frame is a work in progress.  It is buggy and far from complete.
    Please do not use it yet.

    Parameters
    ----------

    dask: dict
        The dask graph to compute this frame
    name: str
        The key prefix that specifies which keys in the dask comprise this
        particular Frame
    columns: list of strings
        Column names.  This metadata aids usability
    blockdivs: tuple of index values
        Values along which we partition our blocks on the index
    """
    def __init__(self, dask, name, columns, blockdivs):
        self.dask = dask
        self.name = name
        self.columns = tuple(columns)
        self.blockdivs = tuple(blockdivs)

    @property
    def npartitions(self):
        return len(self.blockdivs) + 1

    def compute(self, **kwargs):
        return compute(self, **kwargs)[0]

    def _keys(self):
        return [(self.name, i) for i in range(self.npartitions)]

    def __getitem__(self, key):
        name = next(names)
        if isinstance(key, (str, unicode)):
            if key in self.columns:
                dsk = dict(((name, i), (operator.getitem, (self.name, i), key))
                            for i in range(self.npartitions))
                return Frame(merge(self.dask, dsk), name, [key], self.blockdivs)
        if isinstance(key, list):
            if all(k in self.columns for k in key):
                dsk = dict(((name, i), (operator.getitem,
                                         (self.name, i),
                                         (list, key)))
                            for i in range(self.npartitions))
                return Frame(merge(self.dask, dsk), name, key, self.blockdivs)
        if isinstance(key, Frame) and self.blockdivs == key.blockdivs:
            dsk = dict(((name, i), (operator.getitem, (self.name, i),
                                                       (key.name, i)))
                        for i in range(self.npartitions))
            return Frame(merge(self.dask, key.dask, dsk), name,
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

    @property
    def index(self):
        name = next(names)
        dsk = dict(((name, i), (getattr, key, 'index'))
                   for i, key in enumerate(self._keys()))
        return Frame(merge(dsk, self.dask), name, [], self.blockdivs)

    def __dir__(self):
        return sorted(set(list(dir(type(self))) + list(self.columns)))

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

    def cache(self, cache=Chest):
        """ Evaluate frame and store in local cache

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
        return Frame(dsk2, self.name, self.columns, self.blockdivs)

    def groupby(self, key, **kwargs):
        return GroupBy(self, key, **kwargs)

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

    # Examples of reduction behavior
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
        dsk = {(name, 0): (sqrt, (f.name, 0))}
        return Frame(merge(f.dask, dsk), name, [], [])

    def drop_duplicates(self):
        chunk = lambda s: s.drop_duplicates()
        return aca(self, chunk=chunk, aggregate=chunk, columns=self.columns)

    def value_counts(self):
        chunk = lambda s: s.value_counts()
        agg = lambda s: s.groupby(level=0).sum()
        return aca(self, chunk=chunk, aggregate=agg, columns=self.columns)

    def isin(self, other):
        return elemwise(pd.Series.isin, self, other)

    def __len__(self):
        return reduction(self, len, np.sum).compute()

    def map_blocks(self, func, columns=None):
        if columns is None:
            columns = self.columns
        name = next(names)
        dsk = dict(((name, i), (func, (self.name, i)))
                    for i in range(self.npartitions))

        return Frame(merge(dsk, self.dask), name, columns, self.blockdivs)

    def head(self, n=10, compute=True):
        name = next(names)
        dsk = {(name, 0): (head, (self.name, 0), n)}

        result = Frame(merge(self.dask, dsk), name, self.columns, [])

        if compute:
            result = result.compute()
        return result

    def __repr__(self):
        return ("dask.frame<%s, blockdivs=%s>" %
                (self.name, repr_long_list(self.blockdivs)))

    def categorize(self, columns=None, **kwargs):
        return categorize(self, columns, **kwargs)

    def quantiles(self, q):
        """ Approximate quantiles of column

        q : list/array of floats
            Iterable of numbers ranging from 0 to 100 for the desired quantiles
        """
        return quantiles(self, q)

    def _partition_of_index_value(self, val):
        """ In which partition does this value lie? """
        return bisect.bisect_right(self.blockdivs, val)

    def _loc(self, ind):
        name = next(names)
        if not isinstance(ind, slice):
            part = self._partition_of_index_value(ind)
            dsk = {(name, 0): (lambda df: df.loc[ind], (self.name, part))}
            return Frame(merge(self.dask, dsk), name, self.columns, [])
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
                dsk = {(name, 0): (_loc, (self.name, start), ind.start, ind.stop)}
            else:
                dsk = merge(
                  {(name, 0): (_loc, (self.name, start), ind.start, None)},
                  dict(((name, i), (self.name, start + i))
                      for i in range(1, stop - start)),
                  {(name, stop - start): (_loc, (self.name, stop), None, ind.stop)})

            return Frame(merge(self.dask, dsk), name, self.columns,
                         self.blockdivs[start:stop])

    @property
    def loc(self):
        return IndexCallable(self._loc)

    @property
    def iloc(self):
        raise AttributeError("Dask frame does not support iloc")


def _loc(df, start, stop):
    return df.loc[slice(start, stop)]

def head(x, n):
    """ First n elements of dask.frame """
    return x.head(n)


def consistent_name(names):
    """ New name for series in elementwise operation

    If all truthy names are the same, choose that one, otherwise, choose None
    """
    names = set(n for n in names if n is not None)
    if len(names) == 1:
        return first(names)
    else:
        return None


def elemwise(op, *args):
    """ Elementwise operation for dask.frames """
    name = next(names)

    frames = [arg for arg in args if isinstance(arg, Frame)]
    other = [(i, arg) for i, arg in enumerate(args)
                      if not isinstance(arg, Frame)]

    if other:
        op2 = partial_by_order(op, other)
    else:
        op2 = op

    assert all(f.blockdivs == frames[0].blockdivs for f in frames)
    assert all(f.npartitions == frames[0].npartitions for f in frames)

    dsk = dict(((name, i), (op2,) + frs)
                for i, frs in enumerate(zip(*[f._keys() for f in frames])))

    columns = [consistent_name(n for f in frames for n in f.columns)]

    return Frame(merge(dsk, *[f.dask for f in frames]),
                 name, columns, frames[0].blockdivs)


def reduction(x, chunk, aggregate):
    """ General version of reductions

    >>> reduction(my_frame, np.sum, np.sum)  # doctest: +SKIP
    """
    a = next(names)
    dsk = dict(((a, i), (chunk, (x.name, i)))
                for i in range(x.npartitions))

    b = next(names)
    dsk2 = {(b, 0): (aggregate, (tuple, [(a, i) for i in range(x.npartitions)]))}

    return Frame(merge(x.dask, dsk, dsk2), b, x.columns, [])


def linecount(fn):
    """ Count the number of lines in a textfile

    We need this to build the graph for read_csv.  This is much faster than
    actually parsing the file, but still costly.
    """
    with open(os.path.expanduser(fn)) as f:
        result = toolz.count(f)
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
    header = kwargs.get('header', 1)

    nlines = linecount(fn) - header
    nchunks = int(ceil(1.0 * nlines / chunksize))

    read = next(read_csv_names)

    blockdivs = tuple(range(chunksize, nlines, chunksize))

    one_chunk = pd.read_csv(fn, *args, nrows=100, **kwargs)

    kwargs['chunksize'] = chunksize
    load = {(read, -1): (partial(pd.read_csv, *args, **kwargs), fn)}
    load.update(dict(((read, i), (get_chunk, (read, i-1), chunksize*i))
                     for i in range(nchunks)))

    name = next(names)

    dsk = dict(((name, i), (getitem, (read, i), 0))
                for i in range(nchunks))

    return Frame(merge(dsk, load), name, one_chunk.columns, blockdivs)


from_array_names = ('from-array-%d' % i for i in count(1))


def from_array(x, chunksize=50000):
    """ Read dask frame from any slicable array with record dtype

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

    return Frame(dsk, name, columns, blockdivs)


from pframe.categories import reapply_categories

def from_bcolz(x, chunksize=None, categorize=True, index=None, **kwargs):
    """ Read dask frame from bcolz.ctable

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
                np.issubdtype(x.dtype[name], np.object_)):
                a = da.from_array(x[name], blockshape=(chunksize*len(x.names),))
                categories[name] = da.unique(a)

    columns = tuple(x.dtype.names)
    blockdivs = tuple(range(chunksize, len(x), chunksize))
    new_name = next(from_array_names)
    dsk = dict(((new_name, i),
        (pd.DataFrame,
          (dict, (zip,
            x.names,
            [(getitem, x[name], (slice(i * chunksize, (i + 1) * chunksize),))
             if name not in categories else
             (pd.Categorical.from_codes,
                 (np.searchsorted,
                   categories[name],
                   (getitem, x[name], (slice(i * chunksize, (i + 1) * chunksize),))),
                 categories[name],
                 True)
             for name in x.names]))))
           for i in range(0, int(ceil(float(len(x)) / chunksize))))

    result = Frame(dsk, new_name, columns, blockdivs)

    if index:
        assert index in x.names
        a = da.from_array(x[index], blockshape=(chunksize*len(x.names),))
        q = np.linspace(1, 100, len(x) / chunksize + 2)[1:-1]
        blockdivs = da.percentile(a, q).compute()
        return set_partition(result, index, blockdivs, **kwargs)
    else:
        return result


class GroupBy(object):
    def __init__(self, frame, index, **kwargs):
        self.frame = frame
        self.index = index
        self.kwargs = kwargs

        if isinstance(index, list):
            assert all(i in frame.columns for i in index)
        elif isinstance(index, Frame):
            assert index.blockdivs == frame.blockdivs
        else:
            assert index in frame.columns

    def apply(self, func, columns=None):
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
        return f.map_blocks(lambda df: df.groupby(level=0)[self.key].apply(func),
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

    args: dask.Frames
        All frames should be partitioned and indexed equivalently
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
                if isinstance(arg, Frame))
    a = next(names)
    dsk = dict(((a, i), (apply, chunk, (list, [(x.name, i)
                                                if isinstance(x, Frame)
                                                else x for x in args])))
                for i in range(args[0].npartitions))

    b = next(names)
    dsk2 = {(b, 0): (aggregate,
                      (pd.concat,
                        (list, [(a, i) for i in range(args[0].npartitions)])))}

    return Frame(
            merge(dsk, dsk2, *[a.dask for a in args if isinstance(a, Frame)]),
            b, columns, [])


aca = apply_concat_apply


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

    def categorize(block):
        block = block.copy()
        for col, vals in zip(columns, values):
            block[col] = pd.Categorical(block[col], categories=vals,
                                        ordered=False, name=col)
        return block

    return f.map_blocks(categorize, columns=f.columns)


def quantiles(f, q, **kwargs):
    """ Approximate quantiles of column

    q : list/array of floats
        Iterable of numbers ranging from 0 to 100 for the desired quantiles
    """
    assert len(f.columns) == 1
    from dask.array.percentile import _percentile, merge_percentiles
    name = next(names)
    val_dsk = {(name, i): (_percentile, (getattr, key, 'values'), q)
            for i, key in enumerate(f._keys())}
    name2 = next(names)
    len_dsk = {(name2, i): (len, key) for i, key in enumerate(f._keys())}

    vals, lens = get(merge(val_dsk, len_dsk, f.dask),
                     [sorted(val_dsk.keys()), sorted(len_dsk.keys())])

    result = merge_percentiles(q, [q] * f.npartitions, vals, lens)

    return result


from .shuffle import set_index, set_partition

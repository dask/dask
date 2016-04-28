from __future__ import absolute_import, division, print_function

from fnmatch import fnmatch
from functools import wraps
from glob import glob
from math import ceil
from operator import getitem
import os
from threading import Lock
import uuid
from warnings import warn

import pandas as pd
import numpy as np
from toolz import merge

from ..base import tokenize
from ..compatibility import unicode, apply
from .. import array as da
from ..async import get_sync

from .core import _Frame, DataFrame, Series
from .shuffle import set_partition


lock = Lock()


def _dummy_from_array(x, columns=None):
    """ Create empty pd.DataFrame or pd.Series which has correct dtype """

    if x.ndim > 2:
        raise ValueError('from_array does not input more than 2D array, got'
                         ' array with shape %r' % (x.shape,))

    if getattr(x.dtype, 'names', None) is not None:
        # record array has named columns
        cols = tuple(x.dtype.names)
        dtypes = [x.dtype.fields[n][0] for n in x.dtype.names]
    elif x.ndim == 1 and (np.isscalar(columns) or columns is None):
        # Series
        return pd.Series([], name=columns, dtype=x.dtype)
    else:
        cols = list(range(x.shape[1])) if x.ndim == 2 else [0]
        dtypes = [x.dtype] * len(cols)

    data = {}
    for c, dt in zip(cols, dtypes):
        data[c] = np.array([], dtype=dt)
    data = pd.DataFrame(data, columns=cols)

    if columns is not None:
        # if invalid, raise error from pandas
        data.columns = columns
    return data


def from_array(x, chunksize=50000, columns=None):
    """ Read dask Dataframe from any slicable array

    Uses getitem syntax to pull slices out of the array.  The array need not be
    a NumPy array but must support slicing syntax

        x[50000:100000]

    and have 2 dimensions:

        x.ndim == 2

    or have a record dtype:

        x.dtype == [('name', 'O'), ('balance', 'i8')]

    """
    if isinstance(x, da.Array):
        return from_dask_array(x, columns=columns)

    dummy = _dummy_from_array(x, columns)

    divisions = tuple(range(0, len(x), chunksize))
    divisions = divisions + (len(x) - 1,)
    token = tokenize(x, chunksize, columns)
    name = 'from_array-' + token

    dsk = {}
    for i in range(0, int(ceil(len(x) / chunksize))):
        data = (getitem, x, slice(i * chunksize, (i + 1) * chunksize))
        if isinstance(dummy, pd.Series):
            dsk[name, i] = (pd.Series, data, None, dummy.dtype, dummy.name)
        else:
            dsk[name, i] = (pd.DataFrame, data, None, dummy.columns)
    return _Frame(dsk, name, dummy, divisions)


def from_pandas(data, npartitions=None, chunksize=None, sort=True, name=None):
    """Construct a dask object from a pandas object.

    If given a ``pandas.Series`` a ``dask.Series`` will be returned. If given a
    ``pandas.DataFrame`` a ``dask.DataFrame`` will be returned. All other
    pandas objects will raise a ``TypeError``.

    Parameters
    ----------
    df : pandas.DataFrame or pandas.Series
        The DataFrame/Series with which to construct a dask DataFrame/Series
    npartitions : int, optional
        The number of partitions of the index to create.
    chunksize : int, optional
        The size of the partitions of the index.

    Returns
    -------
    dask.DataFrame or dask.Series
        A dask DataFrame/Series partitioned along the index

    Examples
    --------
    >>> df = pd.DataFrame(dict(a=list('aabbcc'), b=list(range(6))),
    ...                   index=pd.date_range(start='20100101', periods=6))
    >>> ddf = from_pandas(df, npartitions=3)
    >>> ddf.divisions  # doctest: +NORMALIZE_WHITESPACE
    (Timestamp('2010-01-01 00:00:00', offset='D'),
     Timestamp('2010-01-03 00:00:00', offset='D'),
     Timestamp('2010-01-05 00:00:00', offset='D'),
     Timestamp('2010-01-06 00:00:00', offset='D'))
    >>> ddf = from_pandas(df.a, npartitions=3)  # Works with Series too!
    >>> ddf.divisions  # doctest: +NORMALIZE_WHITESPACE
    (Timestamp('2010-01-01 00:00:00', offset='D'),
     Timestamp('2010-01-03 00:00:00', offset='D'),
     Timestamp('2010-01-05 00:00:00', offset='D'),
     Timestamp('2010-01-06 00:00:00', offset='D'))

    Raises
    ------
    TypeError
        If something other than a ``pandas.DataFrame`` or ``pandas.Series`` is
        passed in.

    See Also
    --------
    from_array : Construct a dask.DataFrame from an array that has record dtype
    from_bcolz : Construct a dask.DataFrame from a bcolz ctable
    read_csv : Construct a dask.DataFrame from a CSV file
    """
    if isinstance(getattr(data, 'index', None), pd.MultiIndex):
        raise NotImplementedError("Dask does not support MultiIndex Dataframes.")

    if not isinstance(data, (pd.Series, pd.DataFrame)):
        raise TypeError("Input must be a pandas DataFrame or Series")

    if ((npartitions is None) == (chunksize is None)):
        raise ValueError('Exactly one of npartitions and chunksize must be specified.')

    nrows = len(data)

    if chunksize is None:
        chunksize = int(ceil(nrows / npartitions))
    else:
        npartitions = int(ceil(nrows / chunksize))

    if sort and not data.index.is_monotonic_increasing:
        data = data.sort_index(ascending=True)
    if sort:
        divisions, locations = sorted_division_locations(data.index,
                                                         chunksize=chunksize)
    else:
        divisions = [None] * (npartitions + 1)
        locations = list(range(0, nrows, chunksize)) + [len(data)]

    name = name or ('from_pandas-' + tokenize(data, chunksize))
    dsk = dict(((name, i), data.iloc[start: stop])
               for i, (start, stop) in enumerate(zip(locations[:-1],
                   locations[1:])))
    return _Frame(dsk, name, data, divisions)


def from_bcolz(x, chunksize=None, categorize=True, index=None, lock=lock,
               **kwargs):
    """ Read dask Dataframe from bcolz.ctable

    Parameters
    ----------

    x : bcolz.ctable
        Input data
    chunksize : int, optional
        The size of blocks to pull out from ctable.  Ideally as large as can
        comfortably fit in memory
    categorize : bool, defaults to True
        Automatically categorize all string dtypes
    index : string, optional
        Column to make the index
    lock: bool or Lock
        Lock to use when reading or False for no lock (not-thread-safe)

    See Also
    --------

    from_array: more generic function not optimized for bcolz
    """
    if lock is True:
        lock = Lock()

    import dask.array as da
    import bcolz

    if isinstance(x, (str, unicode)):
        x = bcolz.ctable(rootdir=x)
    bc_chunklen = max(x[name].chunklen for name in x.names)
    if chunksize is None and bc_chunklen > 10000:
        chunksize = bc_chunklen

    categories = dict()
    if categorize:
        for name in x.names:
            if (np.issubdtype(x.dtype[name], np.string_) or
                np.issubdtype(x.dtype[name], np.unicode_) or
                np.issubdtype(x.dtype[name], np.object_)):
                a = da.from_array(x[name], chunks=(chunksize * len(x.names),))
                categories[name] = da.unique(a)

    columns = tuple(x.dtype.names)
    divisions = tuple(range(0, len(x), chunksize))
    divisions = divisions + (len(x) - 1,)
    if x.rootdir:
        token = tokenize((x.rootdir, os.path.getmtime(x.rootdir)), chunksize,
                         categorize, index, kwargs)
    else:
        token = tokenize((id(x), x.shape, x.dtype), chunksize, categorize,
                         index, kwargs)
    new_name = 'from_bcolz-' + token

    dsk = dict(((new_name, i),
                (dataframe_from_ctable,
                 x,
                 (slice(i * chunksize, (i + 1) * chunksize),),
                 columns, categories, lock))
               for i in range(0, int(ceil(len(x) / chunksize))))

    result = DataFrame(dsk, new_name, columns, divisions)

    if index:
        assert index in x.names
        a = da.from_array(x[index], chunks=(chunksize * len(x.names),))
        q = np.linspace(0, 100, len(x) // chunksize + 2)
        divisions = da.percentile(a, q).compute()
        return set_partition(result, index, divisions, **kwargs)
    else:
        return result


def dataframe_from_ctable(x, slc, columns=None, categories=None, lock=lock):
    """ Get DataFrame from bcolz.ctable

    Parameters
    ----------

    x: bcolz.ctable
    slc: slice
    columns: list of column names or None

    >>> import bcolz
    >>> x = bcolz.ctable([[1, 2, 3, 4], [10, 20, 30, 40]], names=['a', 'b'])
    >>> dataframe_from_ctable(x, slice(1, 3))
       a   b
    1  2  20
    2  3  30

    >>> dataframe_from_ctable(x, slice(1, 3), columns=['b'])
        b
    1  20
    2  30

    >>> dataframe_from_ctable(x, slice(1, 3), columns='b')
    1    20
    2    30
    Name: b, dtype: int...

    """
    import bcolz
    if columns is None:
        columns = x.dtype.names
    if isinstance(columns, tuple):
        columns = list(columns)

    x = x[columns]
    if type(slc) is slice:
        start = slc.start
        stop = slc.stop if slc.stop < len(x) else len(x)
    else:
        start = slc[0].start
        stop = slc[0].stop if slc[0].stop < len(x) else len(x)
    idx = pd.Index(range(start, stop))

    if lock:
        lock.acquire()
    try:
        if isinstance(x, bcolz.ctable):
            chunks = [x[name][slc] for name in columns]
            if categories is not None:
                chunks = [pd.Categorical.from_codes(
                                    np.searchsorted(categories[name], chunk),
                                    categories[name], True)
                           if name in categories else chunk
                           for name, chunk in zip(columns, chunks)]
            result = pd.DataFrame(dict(zip(columns, chunks)), columns=columns,
                                index=idx)

        elif isinstance(x, bcolz.carray):
            chunk = x[slc]
            if categories is not None and columns and columns in categories:
                chunk = pd.Categorical.from_codes(
                            np.searchsorted(categories[columns], chunk),
                            categories[columns], True)
            result = pd.Series(chunk, name=columns, index=idx)
    finally:
        if lock:
            lock.release()
    return result


def from_dask_array(x, columns=None):
    """ Convert dask Array to dask DataFrame

    Converts a 2d array into a DataFrame and a 1d array into a Series.

    Parameters
    ----------
    x: da.Array
    columns: list or string
        list of column names if DataFrame, single string if Series

    Examples
    --------

    >>> import dask.array as da
    >>> import dask.dataframe as dd
    >>> x = da.ones((4, 2), chunks=(2, 2))
    >>> df = dd.io.from_dask_array(x, columns=['a', 'b'])
    >>> df.compute()
         a    b
    0  1.0  1.0
    1  1.0  1.0
    2  1.0  1.0
    3  1.0  1.0
    """

    dummy = _dummy_from_array(x, columns)

    name = 'from-dask-array' + tokenize(x, columns)
    divisions = [0]
    for c in x.chunks[0]:
        divisions.append(divisions[-1] + c)

    index = [(np.arange, a, b, 1, 'i8') for a, b in
             zip(divisions[:-1], divisions[1:])]
    divisions[-1] -= 1

    if x.ndim == 2:
        if len(x.chunks[1]) > 1:
           x = x.rechunk({1: x.shape[1]})

    dsk = {}
    for i, (chunk, ind) in enumerate(zip(x._keys(), index)):
        if x.ndim == 2:
            chunk = chunk[0]

        if isinstance(dummy, pd.Series):
            dsk[name, i] = (pd.Series, chunk, ind, x.dtype, dummy.name)
        else:
            dsk[name, i] = (pd.DataFrame, chunk, ind, dummy.columns)

    return _Frame(merge(x.dask, dsk), name, dummy, divisions)


def from_castra(x, columns=None):
    """Load a dask DataFrame from a Castra.

    Parameters
    ----------
    x : filename or Castra
    columns: list or string, optional
        The columns to load. Default is all columns.
    """
    from castra import Castra
    if not isinstance(x, Castra):
        x = Castra(x, readonly=True)
    return x.to_dask(columns)


def _link(token, result):
    """ A dummy function to link results together in a graph

    We use this to enforce an artificial sequential ordering on tasks that
    don't explicitly pass around a shared resource
    """
    return None


@wraps(pd.DataFrame.to_hdf)
def to_hdf(df, path_or_buf, key, mode='a', append=False, complevel=0,
           complib=None, fletcher32=False, get=get_sync, dask_kwargs=None,
           **kwargs):
    name = 'to-hdf-' + uuid.uuid1().hex

    pd_to_hdf = getattr(df._partition_type, 'to_hdf')

    dsk = dict()
    dsk[(name, 0)] = (_link, None,
                      (apply, pd_to_hdf,
                          (tuple, [(df._name, 0), path_or_buf, key]),
                          merge(kwargs,
                            {'mode':  mode, 'format': 'table', 'append': append,
                             'complevel': complevel, 'complib': complib,
                             'fletcher32': fletcher32})))
    for i in range(1, df.npartitions):
        dsk[(name, i)] = (_link, (name, i - 1),
                          (apply, pd_to_hdf,
                           (tuple, [(df._name, i), path_or_buf, key]),
                           merge(kwargs,
                             {'mode': 'a', 'format': 'table', 'append': True,
                              'complevel': complevel, 'complib': complib,
                              'fletcher32': fletcher32})))

    dask_kwargs = dask_kwargs or {}

    DataFrame._get(merge(df.dask, dsk), (name, df.npartitions - 1),
                   get=get, **dask_kwargs)


dont_use_fixed_error_message = """
This HDFStore is not partitionable and can only be use monolithically with
pandas.  In the future when creating HDFStores use the ``format='table'``
option to ensure that your dataset can be parallelized"""

read_hdf_error_msg = """
The start and stop keywords are not supported when reading from more than
one file/dataset.

The combination is ambiguous because it could be interpreted as the starting
and stopping index per file, or starting and stopping index of the global
dataset."""

def _read_single_hdf(path, key, start=0, stop=None, columns=None,
                     chunksize=int(1e6), lock=None):
    """
    Read a single hdf file into a dask.dataframe. Used for each file in
    read_hdf.
    """
    def get_keys_and_stops(path, key, stop):
        """
        Get the "keys" or group identifiers which match the given key, which
        can contain wildcards. This uses the hdf file identified by the
        given path. Also get the index of the last row of data for each matched
        key.
        """
        with pd.HDFStore(path) as hdf:
            keys = [k for k in hdf.keys() if fnmatch(k, key)]
            stops = []
            for k in keys:
                storer = hdf.get_storer(k)
                if storer.format_type != 'table':
                    raise TypeError(dont_use_fixed_error_message)
                if stop is None:
                    stops.append(storer.nrows)
                else:
                    stops.append(stop)
        return keys, stops


    def one_path_one_key(path, key, start, stop, columns, chunksize, lock):
        """
        Get the data frame corresponding to one path and one key (which should
        not contain any wildcards).
        """
        empty = pd.read_hdf(path, key, stop=0)
        if columns is not None:
            empty = empty[columns]

        token = tokenize((path, os.path.getmtime(path), key, start,
                          stop, empty, chunksize))
        name = 'read-hdf-' + token

        dsk = dict(((name, i), (_pd_read_hdf, path, key, lock,
                                 {'start': s,
                                  'stop': s + chunksize,
                                  'columns': empty.columns}))
                    for i, s in enumerate(range(start, stop, chunksize)))

        divisions = [None] * (len(dsk) + 1)
        return DataFrame(dsk, name, empty, divisions)

    if lock is True:
        lock = Lock()

    keys, stops = get_keys_and_stops(path, key, stop)
    if (start != 0 or stop is not None) and len(keys) > 1:
        raise NotImplementedError(read_hdf_error_msg)
    from .multi import concat
    return concat([one_path_one_key(path, k, start, s, columns, chunksize, lock)
                   for k, s in zip(keys, stops)])


def _pd_read_hdf(path, key, lock, kwargs):
    """ Read from hdf5 file with a lock """
    if lock:
        lock.acquire()
    try:
        result = pd.read_hdf(path, key, **kwargs)
    finally:
        if lock:
            lock.release()
    return result


@wraps(pd.read_hdf)
def read_hdf(pattern, key, start=0, stop=None, columns=None,
             chunksize=1000000, lock=True):
    """
    Read hdf files into a dask dataframe. Like pandas.read_hdf, except it we
    can read multiple files, and read multiple keys from the same file by using
    pattern matching.

    Parameters
    ----------
    pattern : pattern (string), or buffer to read from. Can contain wildcards
    key : group identifier in the store. Can contain wildcards
    start : optional, integer (defaults to 0), row number to start at
    stop : optional, integer (defaults to None, the last row), row number to
        stop at
    columns : optional, a list of columns that if not None, will limit the
        return columns
    chunksize : optional, nrows to include in iteration, return an iterator

    Returns
    -------
    dask.DataFrame

    Examples
    --------
    Load single file

    >>> dd.read_hdf('myfile.1.hdf5', '/x')  # doctest: +SKIP

    Load multiple files

    >>> dd.read_hdf('myfile.*.hdf5', '/x')  # doctest: +SKIP

    Load multiple datasets

    >>> dd.read_hdf('myfile.1.hdf5', '/*')  # doctest: +SKIP
    """
    key = key if key.startswith('/')  else '/' + key
    paths = sorted(glob(pattern))
    if (start != 0 or stop is not None) and len(paths) > 1:
        raise NotImplementedError(read_hdf_error_msg)
    from .multi import concat
    return concat([_read_single_hdf(path, key, start=start, stop=stop,
                                    columns=columns, chunksize=chunksize,
                                    lock=lock)
                   for path in paths])


def to_castra(df, fn=None, categories=None, sorted_index_column=None,
              compute=True, get=get_sync):
    """ Write DataFrame to Castra on-disk store

    See https://github.com/blosc/castra for details

    See Also
    --------
    Castra.to_dask
    """
    from castra import Castra
    if isinstance(categories, list):
        categories = (list, categories)

    name = 'to-castra-' + uuid.uuid1().hex

    if sorted_index_column:
        set_index = lambda x: x.set_index(sorted_index_column)
        func = lambda part: (set_index, part)
    else:
        func = lambda part: part

    dsk = dict()
    dsk[(name, -1)] = (Castra, fn, func((df._name, 0)), categories)
    for i in range(0, df.npartitions):
        dsk[(name, i)] = (_link, (name, i - 1),
                          (Castra.extend, (name, -1), func((df._name, i))))

    dsk = merge(dsk, df.dask)
    keys = [(name, -1), (name, df.npartitions - 1)]
    if compute:
        c, _ = DataFrame._get(dsk, keys, get=get)
        return c
    else:
        return dsk, keys


def to_csv(df, filename, compression=None, get=None, **kwargs):
    if compression:
        raise NotImplementedError("Writing compressed csv files not supported")
    name = 'to-csv-' + uuid.uuid1().hex

    dsk = dict()
    dsk[(name, 0)] = (lambda df, fn, kwargs: df.to_csv(fn, **kwargs),
                        (df._name, 0), filename, kwargs)

    kwargs2 = kwargs.copy()
    kwargs2['mode'] = 'a'
    kwargs2['header'] = False

    for i in range(1, df.npartitions):
        dsk[(name, i)] = (_link, (name, i - 1),
                           (lambda df, fn, kwargs: df.to_csv(fn, **kwargs),
                             (df._name, i), filename, kwargs2))

    DataFrame._get(merge(dsk, df.dask), (name, df.npartitions - 1), get=get)


def to_bag(df, index=False):
    from ..bag.core import Bag
    if isinstance(df, DataFrame):
        func = lambda df: list(df.itertuples(index))
    elif isinstance(df, Series):
        func = (lambda df: list(df.iteritems())) if index else list
    else:
        raise TypeError("df must be either DataFrame or Series")
    name = 'to_bag-' + tokenize(df, index)
    dsk = dict(((name, i), (func, block)) for (i, block) in enumerate(df._keys()))
    dsk.update(df._optimize(df.dask, df._keys()))
    return Bag(dsk, name, df.npartitions)


def from_imperative(*args, **kwargs):
    warn("Deprecation warning: moved to from_delayed")
    return from_delayed(*args, **kwargs)


def from_delayed(dfs, metadata=None, divisions=None, columns=None,
                      prefix='from-delayed'):
    """ Create DataFrame from many dask.delayed objects

    Parameters
    ----------
    dfs: list of Values
        An iterable of dask.delayed.Value objects, such as come from dask.do
        These comprise the individual partitions of the resulting dataframe
    metadata: list or string of column names or empty dataframe
    divisions: list or None
    """
    if columns is not None:
        warn("Deprecation warning: Use metadata argument, not columns")
        metadata = columns
    from dask.delayed import Value
    if isinstance(dfs, Value):
        dfs = [dfs]
    dsk = merge(df.dask for df in dfs)

    name = prefix + '-' + tokenize(*dfs)
    names = [(name, i) for i in range(len(dfs))]
    values = [df.key for df in dfs]
    dsk2 = dict(zip(names, values))

    if divisions is None:
        divisions = [None] * (len(dfs) + 1)

    if isinstance(metadata, str):
        return Series(merge(dsk, dsk2), name, metadata, divisions)
    else:
        return DataFrame(merge(dsk, dsk2), name, metadata, divisions)


def sorted_division_locations(seq, npartitions=None, chunksize=None):
    """ Find division locations and values in sorted list

    Examples
    --------

    >>> L = ['A', 'B', 'C', 'D', 'E', 'F']
    >>> sorted_division_locations(L, chunksize=2)
    (['A', 'C', 'E', 'F'], [0, 2, 4, 6])

    >>> sorted_division_locations(L, chunksize=3)
    (['A', 'D', 'F'], [0, 3, 6])

    >>> L = ['A', 'A', 'A', 'A', 'B', 'B', 'B', 'C']
    >>> sorted_division_locations(L, chunksize=3)
    (['A', 'B', 'C'], [0, 4, 8])

    >>> sorted_division_locations(L, chunksize=2)
    (['A', 'B', 'C'], [0, 4, 8])

    >>> sorted_division_locations(['A'], chunksize=2)
    (['A', 'A'], [0, 1])
    """
    if ((npartitions is None) == (chunksize is None)):
        raise ValueError('Exactly one of npartitions and chunksize must be specified.')

    if npartitions:
        chunksize = ceil(len(seq) / npartitions)

    positions = [0]
    values = [seq[0]]
    for pos in list(range(0, len(seq), chunksize)):
        if pos <= positions[-1]:
            continue
        while pos + 1 < len(seq) and seq[pos - 1] == seq[pos]:
            pos += 1
        values.append(seq[pos])
        if pos == len(seq) - 1:
            pos += 1
        positions.append(pos)

    if positions[-1] != len(seq):
        positions.append(len(seq))
        values.append(seq[-1])

    return values, positions

from __future__ import absolute_import, division, print_function

from fnmatch import fnmatch
from functools import wraps
from glob import glob
import io
from math import ceil
from operator import getitem
import os
from threading import Lock
import multiprocessing
import uuid
from warnings import warn
import sys

import pandas as pd
import numpy as np
import dask
from toolz import merge

from ..base import tokenize
from ..compatibility import unicode, PY2
from .. import array as da
from ..async import get_sync
from ..context import _globals
from ..delayed import Delayed, delayed
import dask.multiprocessing

from .core import DataFrame, Series, new_dd_object
from .shuffle import set_partition
from .utils import insert_meta_param_description

from ..utils import build_name_function
from ..bytes.core import write_bytes

lock = Lock()


def _meta_from_array(x, columns=None):
    """ Create empty pd.DataFrame or pd.Series which has correct dtype """

    if x.ndim > 2:
        raise ValueError('from_array does not input more than 2D array, got'
                         ' array with shape %r' % (x.shape,))

    if getattr(x.dtype, 'names', None) is not None:
        # record array has named columns
        if columns is None:
            columns = list(x.dtype.names)
        elif np.isscalar(columns):
            raise ValueError("For a struct dtype, columns must be a list.")
        elif not all(i in x.dtype.names for i in columns):
            extra = sorted(set(columns).difference(x.dtype.names))
            raise ValueError("dtype {0} doesn't have fields "
                             "{1}".format(x.dtype, extra))
        fields = x.dtype.fields
        dtypes = [fields[n][0] if n in fields else 'f8' for n in columns]
    elif x.ndim == 1:
        if np.isscalar(columns) or columns is None:
            return pd.Series([], name=columns, dtype=x.dtype)
        elif len(columns) == 1:
            return pd.DataFrame(np.array([], dtype=x.dtype), columns=columns)
        raise ValueError("For a 1d array, columns must be a scalar or single "
                         "element list")
    else:
        if columns is None:
            columns = list(range(x.shape[1])) if x.ndim == 2 else [0]
        elif len(columns) != x.shape[1]:
            raise ValueError("Number of column names must match width of the "
                             "array. Got {0} names for {1} "
                             "columns".format(len(columns), x.shape[1]))
        dtypes = [x.dtype] * len(columns)

    data = {c: np.array([], dtype=dt) for (c, dt) in zip(columns, dtypes)}
    return pd.DataFrame(data, columns=columns)


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

    meta = _meta_from_array(x, columns)

    divisions = tuple(range(0, len(x), chunksize))
    divisions = divisions + (len(x) - 1,)
    token = tokenize(x, chunksize, columns)
    name = 'from_array-' + token

    dsk = {}
    for i in range(0, int(ceil(len(x) / chunksize))):
        data = (getitem, x, slice(i * chunksize, (i + 1) * chunksize))
        if isinstance(meta, pd.Series):
            dsk[name, i] = (pd.Series, data, None, meta.dtype, meta.name)
        else:
            dsk[name, i] = (pd.DataFrame, data, None, meta.columns)
    return new_dd_object(dsk, name, meta, divisions)


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

    name = name or ('from_pandas-' + tokenize(data, chunksize))

    if not nrows:
        return new_dd_object({(name, 0): data}, name, data, [None, None])

    if sort and not data.index.is_monotonic_increasing:
        data = data.sort_index(ascending=True)
    if sort:
        divisions, locations = sorted_division_locations(data.index,
                                                         chunksize=chunksize)
    else:
        locations = list(range(0, nrows, chunksize)) + [len(data)]
        divisions = [None] * len(locations)

    dsk = dict(((name, i), data.iloc[start: stop])
               for i, (start, stop) in enumerate(zip(locations[:-1],
                   locations[1:])))
    return new_dd_object(dsk, name, data, divisions)


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

    meta = dataframe_from_ctable(x, slice(0, 0), columns, categories, lock)
    result = DataFrame(dsk, new_name, meta, divisions)

    if index:
        assert index in x.names
        a = da.from_array(x[index], chunks=(chunksize * len(x.names),))
        q = np.linspace(0, 100, len(x) // chunksize + 2)
        divisions = tuple(da.percentile(a, q).compute())
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

    meta = _meta_from_array(x, columns)

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

        if isinstance(meta, pd.Series):
            dsk[name, i] = (pd.Series, chunk, ind, x.dtype, meta.name)
        else:
            dsk[name, i] = (pd.DataFrame, chunk, ind, meta.columns)

    return new_dd_object(merge(x.dask, dsk), name, meta, divisions)


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


def _pd_to_hdf(pd_to_hdf, lock, args, kwargs=None):
    """ A wrapper function around pd_to_hdf that enables locking"""

    if lock:
        lock.acquire()
    try:
        pd_to_hdf(*args, **kwargs)
    finally:
        if lock:
            lock.release()

    return None


@wraps(pd.DataFrame.to_hdf)
def to_hdf(df, path_or_buf, key, mode='a', append=False, get=None,
           name_function=None, compute=True, lock=None, dask_kwargs={},
           **kwargs):
    name = 'to-hdf-' + uuid.uuid1().hex

    pd_to_hdf = getattr(df._partition_type, 'to_hdf')

    single_file = True
    single_node = True

    # if path_or_buf is string, format using i_name
    if isinstance(path_or_buf, str):
        if path_or_buf.count('*') + key.count('*') > 1:
            raise ValueError("A maximum of one asterisk is accepted in file path and dataset key")

        fmt_obj = lambda path_or_buf, i_name: path_or_buf.replace('*', i_name)

        if '*' in path_or_buf:
            single_file = False
    else:
        if key.count('*') > 1:
            raise ValueError("A maximum of one asterisk is accepted in dataset key")

        fmt_obj = lambda path_or_buf, _: path_or_buf

    if '*' in key:
        single_node = False

    if 'format' in kwargs and kwargs['format'] != 'table':
        raise ValueError("Dask only support 'table' format in hdf files.")

    if mode not in ('a', 'w', 'r+'):
        raise ValueError("Mode must be one of 'a', 'w' or 'r+'")

    if name_function is None:
        name_function = build_name_function(df.npartitions - 1)

    # we guarantee partition order is preserved when its saved and read
    # so we enforce name_function to maintain the order of its input.
    if not (single_file and single_node):
        formatted_names = [name_function(i) for i in range(df.npartitions)]
        if formatted_names != sorted(formatted_names):
            warn("To preserve order between partitions name_function "
                 "must preserve the order of its input")

    # If user did not specify scheduler and write is sequential default to the
    # sequential scheduler. otherwise let the _get method choose the scheduler
    if get is None and 'get' not in _globals and single_node and single_file:
        get = get_sync

    # handle lock default based on whether we're writing to a single entity
    _actual_get = get or _globals.get('get') or df._default_get
    if lock is None:
        if not single_node:
            lock = True
        elif not single_file and _actual_get is not dask.multiprocessing.get:
            # if we're writing to multiple files with the multiprocessing
            # scheduler we don't need to lock
            lock = True
        else:
            lock = False

    if lock is True:
        if _actual_get == dask.multiprocessing.get:
            lock = multiprocessing.Manager().Lock()
        else:
            lock = Lock()

    kwargs.update({'format': 'table', 'mode': mode, 'append': append})

    dsk = dict()

    i_name = name_function(0)
    dsk[(name, 0)] = (_pd_to_hdf, pd_to_hdf, lock,
                      [(df._name, 0), fmt_obj(path_or_buf, i_name),
                             key.replace('*', i_name)], kwargs)

    kwargs2 = kwargs.copy()
    if single_file:
        kwargs2['mode'] = 'a'
    if single_node:
        kwargs2['append'] = True

    for i in range(1, df.npartitions):
        i_name = name_function(i)
        task = (_pd_to_hdf, pd_to_hdf, lock,
                [(df._name, i), fmt_obj(path_or_buf, i_name),
                    key.replace('*', i_name)], kwargs2)
        if single_file:
            link_dep = i - 1 if single_node else 0
            task = (_link, (name, link_dep), task)
        dsk[(name, i)] = task

    dsk = merge(df.dask, dsk)
    if single_file and single_node:
        keys = [(name, df.npartitions - 1)]
    else:
        keys = [(name, i) for i in range(df.npartitions)]

    if compute:
        return DataFrame._get(dsk, keys, get=get, **dask_kwargs)
    else:
        return delayed([Delayed(key, [dsk]) for key in keys])


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
                     chunksize=int(1e6), sorted_index=False, lock=None, mode=None):
    """
    Read a single hdf file into a dask.dataframe. Used for each file in
    read_hdf.
    """
    def get_keys_stops_divisions(path, key, stop, sorted_index):
        """
        Get the "keys" or group identifiers which match the given key, which
        can contain wildcards. This uses the hdf file identified by the
        given path. Also get the index of the last row of data for each matched
        key.
        """
        with pd.HDFStore(path, mode=mode) as hdf:
            keys = [k for k in hdf.keys() if fnmatch(k, key)]
            stops = []
            divisions = []
            for k in keys:
                storer = hdf.get_storer(k)
                if storer.format_type != 'table':
                    raise TypeError(dont_use_fixed_error_message)
                if stop is None:
                    stops.append(storer.nrows)
                elif stop > storer.nrows:
                    raise ValueError("Stop keyword exceeds dataset number "
                                     "of rows ({})".format(storer.nrows))
                else:
                    stops.append(stop)
                if sorted_index:
                    division_start = storer.read_column('index', start=0, stop=1)[0]
                    division_end = storer.read_column('index', start=storer.nrows-1, stop=storer.nrows)[0]
                    divisions.append([division_start, division_end])
                else:
                    divisions.append(None)
        return keys, stops, divisions

    def one_path_one_key(path, key, start, stop, columns, chunksize, division, lock):
        """
        Get the data frame corresponding to one path and one key (which should
        not contain any wildcards).
        """
        empty = pd.read_hdf(path, key, mode=mode, stop=0)
        if columns is not None:
            empty = empty[columns]

        token = tokenize((path, os.path.getmtime(path), key, start,
                          stop, empty, chunksize, division))
        name = 'read-hdf-' + token

        if start >= stop:
            raise ValueError("Start row number ({}) is above or equal to stop "
                             "row number ({})".format(start, stop))

        if division:
            dsk = {(name, 0): (_pd_read_hdf, path, key, lock,
                                 {'mode': mode,
                                  'columns': empty.columns})}

            divisions = division
        else:
            dsk = dict(((name, i), (_pd_read_hdf, path, key, lock,
                                     {'mode': mode,
                                      'start': s,
                                      'stop': s + chunksize,
                                      'columns': empty.columns}))
                        for i, s in enumerate(range(start, stop, chunksize)))

            divisions = [None] * (len(dsk) + 1)

        return DataFrame(dsk, name, empty, divisions)

    keys, stops, divisions = get_keys_stops_divisions(path, key, stop, sorted_index)
    if (start != 0 or stop is not None) and len(keys) > 1:
        raise NotImplementedError(read_hdf_error_msg)
    from .multi import concat
    return concat([one_path_one_key(path, k, start, s, columns, chunksize, d, lock)
                   for k, s, d in zip(keys, stops, divisions)])


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
             chunksize=1000000, sorted_index=False, lock=True, mode=None):
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
    chunksize : optional, positive integer
        maximal number of rows per partition

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
    if lock is True:
        lock = Lock()

    key = key if key.startswith('/') else '/' + key
    paths = sorted(glob(pattern))
    if (start != 0 or stop is not None) and len(paths) > 1:
        raise NotImplementedError(read_hdf_error_msg)
    if chunksize <= 0:
        raise ValueError("Chunksize must be a positive integer")
    if (start != 0 or stop is not None) and sorted_index:
        raise ValueError("When assuming pre-partitioned data, data must be "
                         "read in its entirety using the same chunksizes")
    from .multi import concat
    return concat([_read_single_hdf(path, key, start=start, stop=stop,
                                    columns=columns, chunksize=chunksize,
                                    sorted_index=sorted_index,
                                    lock=lock, mode=mode)
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
        return DataFrame._get(dsk, keys, get=get)[0]
    else:
        return delayed([Delayed(key, [dsk]) for key in keys])[0]


def to_csv(df, filename, name_function=None, compression=None, compute=True,
           **kwargs):

    def func(df, **kwargs):
        if PY2:
            out = io.BytesIO()
        else:
            out = io.StringIO()
        df.to_csv(out, **kwargs)
        out.seek(0)
        if PY2:
            return out.getvalue()
        return out.getvalue().encode(encoding)

    encoding = kwargs.get('encoding', sys.getdefaultencoding())
    values = [delayed(func)(d, **kwargs) for d in df.to_delayed()]
    values = write_bytes(values, filename, name_function, compression,
                         encoding=None)

    if compute:
        from dask import compute
        compute(*values)
    else:
        return values


def _df_to_bag(df, index=False):
    if isinstance(df, pd.DataFrame):
        return list(map(tuple, df.itertuples(index)))
    elif isinstance(df, pd.Series):
        return list(df.iteritems()) if index else list(df)


def to_bag(df, index=False):
    from ..bag.core import Bag
    if not isinstance(df, (DataFrame, Series)):
        raise TypeError("df must be either DataFrame or Series")
    name = 'to_bag-' + tokenize(df, index)
    dsk = dict(((name, i), (_df_to_bag, block, index))
               for (i, block) in enumerate(df._keys()))
    dsk.update(df._optimize(df.dask, df._keys()))
    return Bag(dsk, name, df.npartitions)


def from_imperative(*args, **kwargs):
    warn("Deprecation warning: moved to from_delayed")
    return from_delayed(*args, **kwargs)


@insert_meta_param_description
def from_delayed(dfs, meta=None, divisions=None, prefix='from-delayed',
                 metadata=None):
    """ Create DataFrame from many dask.delayed objects

    Parameters
    ----------
    dfs : list of Values
        An iterable of ``dask.delayed.Delayed`` objects, such as come from
        ``dask.delayed`` These comprise the individual partitions of the
        resulting dataframe.
    $META
    divisions : tuple, str, optional
        Partition boundaries along the index.
        For tuple, see http://dask.pydata.io/en/latest/dataframe-partitions.html
        For string 'sorted' will compute the delayed values to find index
        values.  Assumes that the indexes are mutually sorted.
        If None, then won't use index information
    prefix : str, optional
        Prefix to prepend to the keys.
    """
    if metadata is not None and meta is None:
        warn("Deprecation warning: Use meta keyword, not metadata")
        meta = metadata
    from dask.delayed import Delayed
    if isinstance(dfs, Delayed):
        dfs = [dfs]
    dsk = merge(df.dask for df in dfs)

    name = prefix + '-' + tokenize(*dfs)
    names = [(name, i) for i in range(len(dfs))]
    values = [df.key for df in dfs]
    dsk2 = dict(zip(names, values))
    dsk3 = merge(dsk, dsk2)

    if meta is None:
        meta = dfs[0].compute()
    if isinstance(meta, (str, pd.Series)):
        Frame = Series
    else:
        Frame = DataFrame

    if divisions == 'sorted':
        from .core import compute_divisions
        divisions = [None] * (len(dfs) + 1)
        df = Frame(dsk3, name, meta, divisions)
        return compute_divisions(df)
    elif divisions is None:
        divisions = [None] * (len(dfs) + 1)

    return Frame(dsk3, name, meta, divisions)


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

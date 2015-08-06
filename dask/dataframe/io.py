from __future__ import division

import pandas as pd
import numpy as np
from functools import wraps, partial
import re
import os
from glob import glob
from math import ceil
from toolz import merge, dissoc, assoc
from operator import getitem
from hashlib import md5
import uuid

from ..compatibility import BytesIO, unicode, range, apply
from ..utils import textblock, file_size
from ..base import compute
from .utils import tokenize_dataframe
from .. import array as da
from ..async import get_sync

from . import core
from .core import (DataFrame, Series, concat, categorize_block, tokens,
        tokenize)
from .shuffle import set_partition


csv_defaults = {'compression': None}

def fill_kwargs(fn, args, kwargs):
    """ Read a csv file and fill up kwargs

    This normalizes kwargs against a sample file.  It does the following:

    1.  If given a globstring, just use one file
    2.  Get names from csv file if not given
    3.  Identify the presence of a header
    4.  Identify dtypes
    5.  Establish column names
    6.  Switch around dtypes and column names if parse_dates is active

    Normally ``pd.read_csv`` does this for us.  However for ``dd.read_csv`` we
    need to be consistent across multiple files and don't want to do these
    heuristics each time so we use the pandas solution once, record the
    results, and then send back a fully explicit kwargs dict to send to future
    calls to ``pd.read_csv``.

    Returns
    -------

    kwargs: dict
        keyword arguments to give to pd.read_csv
    """
    kwargs = merge(csv_defaults, kwargs)
    sample_nrows = kwargs.pop('sample_nrows', 1000)
    essentials = ['columns', 'names', 'header', 'parse_dates', 'dtype']
    if set(essentials).issubset(kwargs):
        return kwargs

    # Let pandas infer on the first 100 rows
    if '*' in fn:
        filenames = sorted(glob(fn))
        if not filenames:
            raise ValueError("No files found matching name %s" % fn)
        fn = filenames[0]

    if 'names' not in kwargs:
        kwargs['names'] = csv_names(fn, **kwargs)
    if 'header' not in kwargs:
        kwargs['header'] = infer_header(fn, **kwargs)
        if kwargs['header'] is True:
            kwargs['header'] = 0

    try:
        head = pd.read_csv(fn, *args, **assoc(kwargs, 'nrows', sample_nrows))
    except StopIteration:
        head = pd.read_csv(fn, *args, **kwargs)

    if 'parse_dates' not in kwargs:
        kwargs['parse_dates'] = [col for col in head.dtypes.index
                           if np.issubdtype(head.dtypes[col], np.datetime64)]
    if 'dtype' not in kwargs:
        kwargs['dtype'] = dict(head.dtypes)
        for col in kwargs['parse_dates']:
            del kwargs['dtype'][col]

    kwargs['columns'] = list(head.columns)

    return kwargs

@wraps(pd.read_csv)
def read_csv(fn, *args, **kwargs):
    chunkbytes = kwargs.pop('chunkbytes', 2**25)  # 50 MB
    categorize = kwargs.pop('categorize', None)
    index = kwargs.pop('index', None)
    if index and categorize == None:
        categorize = True

    kwargs = fill_kwargs(fn, args, kwargs)

    # Handle glob strings
    if '*' in fn:
        return concat([read_csv(f, *args, **kwargs) for f in sorted(glob(fn))])

    arg_token = (os.path.getmtime(fn),
                 args,
                 sorted(kwargs.items(), key=lambda kv: kv[0]))
    name = 'read-csv-%s-%s' % (fn, tokenize(arg_token))

    columns = kwargs.pop('columns')
    header = kwargs.pop('header')

    if 'nrows' in kwargs:  # Just create single partition
        dsk = {(name, 0): (apply, pd.read_csv, (fn,),
                                  assoc(kwargs, 'header', header))}
        result = DataFrame(dsk, name, columns, [None, None])

    else:
        # Chunk sizes and numbers
        total_bytes = file_size(fn, kwargs['compression'])
        nchunks = int(ceil(total_bytes / chunkbytes))
        divisions = [None] * (nchunks + 1)

        first_read_csv = partial(pd.read_csv, *args, header=header,
                               **dissoc(kwargs, 'compression'))
        rest_read_csv = partial(pd.read_csv, *args, header=None,
                              **dissoc(kwargs, 'compression'))

        # Create dask graph
        dsk = dict(((name, i), (rest_read_csv, (BytesIO,
                                   (textblock, fn,
                                       i*chunkbytes, (i+1) * chunkbytes,
                                       kwargs['compression']))))
                   for i in range(1, nchunks))
        dsk[(name, 0)] = (first_read_csv, (BytesIO,
                           (textblock, fn, 0, chunkbytes, kwargs['compression'])))

        result = DataFrame(dsk, name, columns, divisions)

    if categorize or index:
        categories, quantiles = categories_and_quantiles(fn, args, kwargs,
                                                         index, categorize,
                                                         chunkbytes=chunkbytes)

    if categorize:
        func = partial(categorize_block, categories=categories)
        result = result.map_partitions(func, columns=columns)

    if index:
        result = set_partition(result, index, quantiles)

    return result


def infer_header(fn, encoding='utf-8', compression=None, **kwargs):
    """ Guess if csv file has a header or not

    This uses Pandas to read a sample of the file, then looks at the column
    names to see if they are all word-like.

    Returns True or False
    """
    # See read_csv docs for header for reasoning
    try:
        df = pd.read_csv(fn, encoding=encoding, compression=compression,nrows=5)
    except StopIteration:
        df = pd.read_csv(fn, encoding=encoding, compression=compression)
    return (len(df) > 0 and
            all(re.match('^\s*\D\w*\s*$', n) for n in df.columns) and
            not all(dt == 'O' for dt in df.dtypes))


def csv_names(fn, encoding='utf-8', compression=None, names=None,
                parse_dates=None, usecols=None, dtype=None, **kwargs):
    try:
        kwargs['nrows'] = 5
        df = pd.read_csv(fn, encoding=encoding, compression=compression,
                names=names, parse_dates=parse_dates, **kwargs)
    except StopIteration:
        kwargs['nrows'] = None
        df = pd.read_csv(fn, encoding=encoding, compression=compression,
                names=names, parse_dates=parse_dates, **kwargs)
    return list(df.columns)


def categories_and_quantiles(fn, args, kwargs, index=None, categorize=None,
        chunkbytes=2**26):
    """
    Categories of Object columns and quantiles of index column for CSV

    Computes both of the following in a single pass

    1.  The categories for all object dtype columns
    2.  The quantiles of the index column

    Parameters
    ----------

    fn: string
        Filename of csv file
    args: tuple
        arguments to be passed in to read_csv function
    kwargs: dict
        keyword arguments to pass in to read_csv function
    index: string or None
        Name of column on which to compute quantiles
    categorize: bool
        Whether or not to compute categories of Object dtype columns
    """
    kwargs = kwargs.copy()

    compression = kwargs.get('compression', None)
    total_bytes = file_size(fn, compression)
    nchunks = int(ceil(total_bytes / chunkbytes))

    if infer_header(fn, **kwargs):
        kwargs['header'] = 0

    one_chunk = pd.read_csv(fn, *args, nrows=100, **kwargs)

    if categorize is not False:
        category_columns = [c for c in one_chunk.dtypes.index
                               if one_chunk.dtypes[c] == 'O'
                               and c not in kwargs.get('parse_dates', ())]
    else:
        category_columns = []
    cols = category_columns + ([index] if index else [])

    dtypes = dict((c, one_chunk.dtypes[c]) for c in cols)
    d = read_csv(fn, *args, **merge(kwargs,
                                    dict(usecols=cols,
                                         parse_dates=None,
                                         dtype=dtypes)))
    categories = [d[c].drop_duplicates() for c in category_columns]

    if index:
        quantiles = d[index].quantile(np.linspace(0, 1, nchunks + 1))
        result = compute(quantiles, *categories)
        quantiles, categories = result[0].values, result[1:]
    else:
        categories = compute(*categories)
        quantiles = None

    categories = dict(zip(category_columns, categories))

    return categories, quantiles


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
    has_record_dtype = getattr(x.dtype, 'names', None) is not None
    if x.ndim > 2:
        raise ValueError('from_array does not input more than 2D array, got'
                         ' array with shape %r' % (x.shape,))
    if columns is None:
        if has_record_dtype:
            columns = tuple(x.dtype.names)  # record array has named columns
        elif x.ndim == 2:
            columns = [str(i) for i in range(x.shape[1])]
    if isinstance(x, da.Array):
        return from_dask_array(x, columns=columns)
    divisions = tuple(range(0, len(x), chunksize))
    if divisions[-1] != len(x) - 1:
        divisions = divisions + (len(x) - 1,)
    token = tokenize((id(x), x.dtype, x.shape, chunksize, columns))
    name = 'from_array-' + token
    dsk = dict(((name, i), (pd.DataFrame,
                             (getitem, x,
                              slice(i * chunksize, (i + 1) * chunksize))))
            for i in range(0, int(ceil(len(x) / chunksize))))

    return DataFrame(dsk, name, columns, divisions)


def from_pandas(data, npartitions):
    """Construct a dask object from a pandas object.

    If given a ``pandas.Series`` a ``dask.Series`` will be returned. If given a
    ``pandas.DataFrame`` a ``dask.DataFrame`` will be returned. All other
    pandas objects will raise a ``TypeError``.

    Parameters
    ----------
    df : pandas.DataFrame or pandas.Series
        The DataFrame/Series with which to construct a dask DataFrame/Series
    npartitions : int
        The number of partitions of the index to create

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
    columns = getattr(data, 'columns', getattr(data, 'name', None))
    if columns is None and not isinstance(data, pd.Series):
        raise TypeError("Input must be a pandas DataFrame or Series")
    nrows = len(data)
    chunksize = int(ceil(nrows / npartitions))
    data = data.sort_index(ascending=True)
    divisions = tuple(data.index[i]
                      for i in range(0, nrows, chunksize))
    divisions = divisions + (data.index[-1],)

    token = (tokenize_dataframe(data), chunksize) # this could be improved
    name = 'from_pandas-' + tokenize(token)
    dsk = dict(((name, i), data.iloc[i * chunksize:(i + 1) * chunksize])
               for i in range(npartitions - 1))
    dsk[(name, npartitions - 1)] = data.iloc[chunksize*(npartitions - 1):]
    return getattr(core, type(data).__name__)(dsk, name, columns, divisions)


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
    divisions = (0,) + tuple(range(-1, len(x), chunksize))[1:]
    if divisions[-1] != len(x) - 1:
        divisions = divisions + (len(x) - 1,)
    if x.rootdir:
        token = tokenize((x.rootdir, os.path.getmtime(x.rootdir), chunksize,
                          categorize, index, sorted(kwargs.items())))
    else:
        token = tokenize((id(x), x.shape, x.dtype, chunksize, categorize,
                         index, sorted(kwargs.items())))
    new_name = 'from_bcolz-' + token
    dsk = dict(((new_name, i),
                (dataframe_from_ctable,
                 x,
                 (slice(i * chunksize, (i + 1) * chunksize),),
                 None, categories))
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


def dataframe_from_ctable(x, slc, columns=None, categories=None):
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
    import bcolz
    if columns is not None:
        if isinstance(columns, tuple):
            columns = list(columns)
        x = x[columns]

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


def from_dask_array(x, columns=None):
    """ Convert dask Array to dask DataFrame

    Converts a 2d array into a DataFrame and a 1d array into a Series.

    Parameters
    ----------
    x: da.Array
    columns: list or string
        list of column names if DataFrame, single string if Series

    Example
    -------

    >>> import dask.array as da
    >>> import dask.dataframe as dd
    >>> x = da.ones((4, 2), chunks=(2, 2))
    >>> df = dd.io.from_dask_array(x, columns=['a', 'b'])
    >>> df.compute()
       a  b
    0  1  1
    1  1  1
    2  1  1
    3  1  1
    """
    name = 'from-dask-array' + tokenize((x.name, columns))
    divisions = [0]
    for c in x.chunks[0]:
        divisions.append(divisions[-1] + c)

    index = [(range, a, b) for a, b in zip(divisions[:-1], divisions[1:])]

    divisions[-1] -= 1

    if x.ndim == 1:
        dsk = dict(((name, i), (pd.Series, chunk, ind, x.dtype, columns))
                for i, (chunk, ind) in enumerate(zip(x._keys(), index)))
        return Series(merge(x.dask, dsk), name, columns, divisions)

    elif x.ndim == 2:
        if columns is None:
            raise ValueError("Must provide columns for DataFrame")
        if len(columns) != x.shape[1]:
            raise ValueError("Columns must be the same length as array width\n"
                    "  columns: %s\n  width: %d" % (str(columns), x.shape[1]))
        if len(x.chunks[1]) > 1:
            x = x.rechunk({1: x.shape[1]})
        dsk = dict(((name, i), (pd.DataFrame, chunk[0], ind, columns))
                for i, (chunk, ind) in enumerate(zip(x._keys(), index)))
        return DataFrame(merge(x.dask, dsk), name, columns, divisions)

    else:
        raise ValueError("Array must have one or two dimensions.  Had %d" %
                         x.ndim)


def _link(token, result):
    """ A dummy function to link results together in a graph

    We use this to enforce an artificial sequential ordering on tasks that
    don't explicitly pass around a shared resource
    """
    return None


@wraps(pd.DataFrame.to_hdf)
def to_hdf(df, path_or_buf, key, mode='a', append=False, complevel=0,
           complib=None, fletcher32=False, **kwargs):
    name = 'to-hdf-' + uuid.uuid1().hex

    pd_to_hdf = getattr(df._partition_type, 'to_hdf')

    dsk = dict()
    dsk[(name, 0)] = (_link, None,
                      (apply, pd_to_hdf,
                          (tuple, [(df._name, 0), path_or_buf, key]),
                          {'mode':  mode, 'format': 'table', 'append': append,
                           'complevel': complevel, 'complib': complib,
                           'fletcher32': fletcher32}))
    for i in range(1, df.npartitions):
        dsk[(name, i)] = (_link, (name, i - 1),
                          (apply, pd_to_hdf,
                           (tuple, [(df._name, i), path_or_buf, key]),
                           {'mode': 'a', 'format': 'table', 'append': True,
                            'complevel': complevel, 'complib': complib,
                            'fletcher32': fletcher32}))

    DataFrame._get(merge(df.dask, dsk), (name, i), **kwargs)


dont_use_fixed_error_message = """
This HDFStore is not partitionable and can only be use monolithically with
pandas.  In the future when creating HDFStores use the ``format='table'``
option to ensure that your dataset can be parallelized"""

@wraps(pd.read_hdf)
def read_hdf(path_or_buf, key, start=0, stop=None, columns=None,
        chunksize=1000000):
    with pd.HDFStore(path_or_buf) as hdf:
        storer = hdf.get_storer(key)
        if storer.format_type != 'table':
            raise TypeError(dont_use_fixed_error_message)
        if stop is None:
            stop = storer.nrows

    if columns is None:
        columns = list(pd.read_hdf(path_or_buf, key, stop=0).columns)

    token = tokenize((path_or_buf, os.path.getmtime(path_or_buf), key, start,
                      stop, columns, chunksize))
    name = 'read-hdf-' + token

    dsk = dict(((name, i), (apply, pd.read_hdf,
                             (path_or_buf, key),
                             {'start': s, 'stop': s + chunksize,
                              'columns': columns}))
                for i, s in enumerate(range(start, stop, chunksize)))

    divisions = [None] * (len(dsk) + 1)

    return DataFrame(dsk, name, columns, divisions)


def to_castra(df, fn=None, categories=None, compute=True):
    """ Write DataFrame to Castra on-disk store

    See https://github.com/blosc/castra for details

    See Also:
        Castra.to_dask
    """
    from castra import Castra
    if isinstance(categories, list):
        categories = (list, categories)

    name = 'to-castra-' + uuid.uuid1().hex

    dsk = dict()
    dsk[(name, -1)] = (Castra, fn, (df._name, 0), categories)
    for i in range(0, df.npartitions):
        dsk[(name, i)] = (_link, (name, i - 1),
                          (Castra.extend, (name, -1), (df._name, i)))

    dsk = merge(dsk, df.dask)
    keys = [(name, -1), (name, df.npartitions - 1)]
    if compute:
        c, _ = DataFrame._get(dsk, keys, get=get_sync)
        return c
    else:
        return dsk, keys


def to_csv(df, filename, **kwargs):
    myget = kwargs.pop('get', None)
    name = 'to-csv-' + uuid.uuid1().hex

    dsk = dict()
    dsk[(name, 0)] = (apply, pd.DataFrame.to_csv,
                        (tuple, [(df._name, 0), filename]),
                        kwargs)

    kwargs2 = kwargs.copy()
    kwargs2['mode'] = 'a'
    kwargs2['header'] = False

    for i in range(1, df.npartitions):
        dsk[(name, i)] = (_link, (name, i - 1),
                           (apply, pd.DataFrame.to_csv,
                             (tuple, [(df._name, i), filename]),
                             kwargs2))

    DataFrame._get(merge(dsk, df.dask), (name, df.npartitions - 1), get=myget)

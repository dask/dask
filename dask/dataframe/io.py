import pandas as pd
import numpy as np
from functools import wraps
import re
import struct
import os
from glob import glob
from math import ceil
from toolz import curry, merge, partial
from itertools import count
import bcolz
from operator import getitem, attrgetter

from ..compatibility import StringIO, unicode
from ..utils import textblock

from .core import names, DataFrame, compute, concat, categorize_block
from .shuffle import set_partition


def _StringIO(data):
    if isinstance(data, bytes):
        data = data.decode()
    return StringIO(data)


def file_size(fn, compression=None):
    """ Size of a file on disk

    If compressed then return the uncompressed file size
    """
    if compression == 'gzip':
        with open(fn, 'rb') as f:
            f.seek(-4, 2)
            result = struct.unpack('I', f.read(4))[0]
    else:
        result = os.stat(fn).st_size
    return result


@wraps(pd.read_csv)
def read_csv(fn, *args, **kwargs):
    chunkbytes = kwargs.pop('chunkbytes', 2**25)  # 50 MB
    compression = kwargs.get('compression', None)
    categorize = kwargs.pop('categorize', None)
    sample_nrows = kwargs.pop('sample_nrows', 1000)
    index = kwargs.pop('index', None)
    if index and categorize == None:
        categorize = True

    # Let pandas infer on the first 100 rows
    if '*' in fn:
        first_fn = sorted(glob(fn))[0]
    else:
        first_fn = fn

    if names not in kwargs:
        kwargs['names'] = csv_names(first_fn, **kwargs)
    if 'header' not in kwargs:
        header = infer_header(first_fn, **kwargs)
        if header is True:
            header = 0
    else:
        header = kwargs.pop('header')

    try:
        head = pd.read_csv(first_fn, *args, nrows=sample_nrows, header=header, **kwargs)
    except StopIteration:
        head = pd.read_csv(first_fn, *args, header=header, **kwargs)


    if 'parse_dates' not in kwargs:
        parse_dates = [col for col in head.dtypes.index
                           if np.issubdtype(head.dtypes[col], np.datetime64)]
        if parse_dates:
            kwargs['parse_dates'] = parse_dates
    else:
        parse_dates = kwargs.get('parse_dates')
    if 'dtypes' in kwargs:
        dtype = kwargs['dtype']
    else:
        dtype = dict(head.dtypes)
        if parse_dates:
            for col in parse_dates:
                del dtype[col]

    kwargs['dtype'] = dtype

    # Handle glob strings
    if '*' in fn:
        return concat([read_csv(f, *args, **kwargs) for f in sorted(glob(fn))])

    # Chunk sizes and numbers
    total_bytes = file_size(fn, compression)
    nchunks = int(ceil(float(total_bytes) / chunkbytes))
    divisions = [None] * (nchunks - 1)

    kwargs.pop('compression', None)  # these functions will take StringIO

    first_read_csv = curry(pd.read_csv, *args, header=header, **kwargs)
    rest_read_csv = curry(pd.read_csv, *args, header=None, **kwargs)

    # Create dask graph
    name = next(names)
    dsk = dict(((name, i), (rest_read_csv, (_StringIO,
                               (textblock, fn,
                                   i*chunkbytes, (i+1) * chunkbytes,
                                   compression))))
               for i in range(1, nchunks))
    dsk[(name, 0)] = (first_read_csv, (_StringIO,
                       (textblock, fn, 0, chunkbytes, compression)))

    result = DataFrame(dsk, name, head.columns, divisions)

    if categorize or index:
        categories, quantiles = categories_and_quantiles(fn, args, kwargs,
                                                         index, categorize,
                                                         chunkbytes=chunkbytes)

    if categorize:
        func = partial(categorize_block, categories=categories)
        result = result.map_blocks(func, columns=result.columns)

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
        df = pd.read_csv(fn, encoding=encoding, compression=compression,
                names=names, parse_dates=parse_dates, nrows=5, **kwargs)
    except StopIteration:
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
    nchunks = int(ceil(float(total_bytes) / chunkbytes))

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

    import dask
    if index:
        quantiles = d[index].quantiles(np.linspace(0, 100, nchunks + 1)[1:-1])
        result = compute(quantiles, *categories)
        quantiles, categories = result[0], result[1:]
    else:
        categories = compute(*categories)
        quantiles = None

    categories = dict(zip(category_columns, categories))

    return categories, quantiles


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
    divisions = tuple(range(chunksize, len(x), chunksize))
    name = next(from_array_names)
    dsk = dict(((name, i), (pd.DataFrame,
                             (getitem, x,
            for i in range(0, int(ceil(float(len(x)) / chunksize))))
                              slice(i * chunksize, (i + 1) * chunksize))))

    return DataFrame(dsk, name, columns, divisions)


from_dataframe_names = ('from-dataframe-%d' % i for i in count(1))


def from_dataframe(df, npartitions=10):
    nrows = len(df)
    chunksize = int(ceil(float(nrows) / npartitions))
    divisions = tuple(range(chunksize, nrows, chunksize))
    name = next(from_dataframe_names)
    iloc = df.iloc
    dsk = dict(((name, i),
                (getitem, iloc, slice(i * chunksize, (i + 1) * chunksize)))
               for i in range(npartitions))
    return DataFrame(dsk, name, tuple(df.columns), divisions)



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
    import dask.array as da
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
                a = da.from_array(x[name], chunks=(chunksize*len(x.names),))
                categories[name] = da.unique(a)

    columns = tuple(x.dtype.names)
    divisions = tuple(range(chunksize, len(x), chunksize))
    new_name = next(from_array_names)
    dsk = dict(((new_name, i),
                (dataframe_from_ctable,
                  x,
                  (slice(i * chunksize, (i + 1) * chunksize),),
                  None, categories))
           for i in range(0, int(ceil(float(len(x)) / chunksize))))

    result = DataFrame(dsk, new_name, columns, divisions)

    if index:
        assert index in x.names
        a = da.from_array(x[index], chunks=(chunksize*len(x.names),))
        q = np.linspace(1, 100, len(x) / chunksize + 2)[1:-1]
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

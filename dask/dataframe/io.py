import pandas as pd
import numpy as np
from functools import wraps
import struct
import os
from math import ceil
from toolz import curry, merge

from ..compatibility import StringIO
from ..utils import textblock

from .core import names, DataFrame, compute


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
    assert not kwargs.pop('categorize', None)
    assert not kwargs.pop('index', None)
    compression = kwargs.get('compression', None)
    header = kwargs.pop('header', 'infer')

    # Chunk sizes and numbers
    chunkbytes = kwargs.pop('chunkbytes', 2**28)  # 500 MB
    total_bytes = file_size(fn, compression)
    nchunks = int(ceil(float(total_bytes) / chunkbytes))
    divisions = [None] * (nchunks - 1)

    # Let pandas infer on the first 100 rows
    head = pd.read_csv(fn, *args, nrows=100, header=header, **kwargs)
    first_read_csv = curry(pd.read_csv, *args, names=head.columns,
                           header=header, **kwargs)
    rest_read_csv = curry(pd.read_csv, *args, names=head.columns,
                          header=None, **kwargs)

    # Create dask graph
    name = next(names)
    dsk = dict(((name, i), (rest_read_csv, (StringIO,
                               (textblock, fn, i*chunkbytes, (i+1) * chunkbytes))))
               for i in range(1, nchunks))
    dsk[(name, 0)] = (first_read_csv, (StringIO,
                       (textblock, fn, 0, chunkbytes)))

    return DataFrame(dsk, name, head.columns, divisions)


def categories_and_quantiles(fn, args, kwargs, index=None, categorize=None):
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

    chunkbytes = kwargs.pop('chunkbytes', 2**28)  # 500 MB
    compression = kwargs.get('compression', None)
    total_bytes = file_size(fn, compression)
    nchunks = int(ceil(float(total_bytes) / chunkbytes))

    one_chunk = pd.read_csv(fn, *args, nrows=100, **kwargs)

    if categorize is not False:
        category_columns = [c for c in one_chunk.dtypes.index
                               if one_chunk.dtypes[c] == 'O']
    else:
        category_columns = []
    cols = category_columns + ([index] if index else [])

    d = read_csv(fn, *args, **merge(kwargs,
                                    dict(usecols=cols,
                                         parse_dates=None)))
    categories = [d[c].drop_duplicates() for c in category_columns]

    if index:
        quantiles = d[index].quantiles(np.linspace(0, 100, nchunks + 1)[1:-1])
        result = compute(quantiles, *categories)
        quantiles, categories = result[0], result[1:]
    else:
        categories = compute(*categories)
        quantiles = None

    categories = dict(zip(category_columns, categories))

    return categories, quantiles

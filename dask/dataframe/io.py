import pandas as pd
from functools import wraps
import struct
import os
from math import ceil
from toolz import curry

from ..compatibility import StringIO
from ..utils import textblock

from .core import names, DataFrame


def file_size(fn, compression=None):
    """ Size of a file on disk

    If compressed then return the uncompressed file size
    """
    if compression == 'gzip':
        with open(fn) as f:
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

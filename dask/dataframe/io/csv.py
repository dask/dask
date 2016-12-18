from __future__ import print_function, division, absolute_import

from io import BytesIO
from warnings import warn, catch_warnings, simplefilter
import sys

try:
    import psutil
except ImportError:
    psutil = None

import numpy as np
import pandas as pd

from ...compatibility import PY2
from ...delayed import delayed
from .io import from_delayed

from ...bytes import read_bytes
from ...bytes.core import write_bytes
from ...bytes.compression import seekable_files, files as cfiles


delayed = delayed(pure=True)


def pandas_read_text(reader, b, header, kwargs, dtypes=None, columns=None,
                     write_header=True, enforce=False):
    """ Convert a block of bytes to a Pandas DataFrame

    Parameters
    ----------
    reader : callable
        ``pd.read_csv`` or ``pd.read_table``.
    b : bytestring
        The content to be parsed with ``reader``
    header : bytestring
        An optional header to prepend to ``b``
    kwargs : dict
        A dictionary of keyword arguments to be passed to ``reader``
    dtypes : dict
        DTypes to assign to columns

    See Also
    --------
    dask.dataframe.csv.read_pandas_from_bytes
    """
    bio = BytesIO()
    if write_header and not b.startswith(header.rstrip()):
        bio.write(header)
    bio.write(b)
    bio.seek(0)
    df = reader(bio, **kwargs)
    if dtypes:
        coerce_dtypes(df, dtypes)

    if enforce and columns and (list(df.columns) != list(columns)):
        raise ValueError("Columns do not match", df.columns, columns)
    elif columns:
        df.columns = columns
    return df


def coerce_dtypes(df, dtypes):
    """ Coerce dataframe to dtypes safely

    Operates in place

    Parameters
    ----------
    df: Pandas DataFrame
    dtypes: dict like {'x': float}
    """
    for c in df.columns:
        if c in dtypes and df.dtypes[c] != dtypes[c]:
            if (np.issubdtype(df.dtypes[c], np.floating) and
                    np.issubdtype(dtypes[c], np.integer)):
                if (df[c] % 1).any():
                    msg = ("Runtime type mismatch. "
                           "Add {'%s': float} to dtype= keyword in "
                           "read_csv/read_table")
                    raise TypeError(msg % c)
            df[c] = df[c].astype(dtypes[c])


def text_blocks_to_pandas(reader, block_lists, header, head, kwargs,
                          collection=True, enforce=False):
    """ Convert blocks of bytes to a dask.dataframe or other high-level object

    This accepts a list of lists of values of bytes where each list corresponds
    to one file, and the value of bytes concatenate to comprise the entire
    file, in order.

    Parameters
    ----------
    reader : callable
        ``pd.read_csv`` or ``pd.read_table``.
    block_lists : list of lists of delayed values of bytes
        The lists of bytestrings where each list corresponds to one logical file
    header : bytestring
        The header, found at the front of the first file, to be prepended to
        all blocks
    head : pd.DataFrame
        An example Pandas DataFrame to be used for metadata.
        Can be ``None`` if ``collection==False``
    kwargs : dict
        Keyword arguments to pass down to ``reader``
    collection: boolean, optional (defaults to True)

    Returns
    -------
    A dask.dataframe or list of delayed values
    """
    dtypes = head.dtypes.to_dict()
    columns = list(head.columns)
    delayed_pandas_read_text = delayed(pandas_read_text)
    dfs = []
    for blocks in block_lists:
        if not blocks:
            continue
        df = delayed_pandas_read_text(reader, blocks[0], header, kwargs,
                                      dtypes, columns, write_header=False,
                                      enforce=enforce)
        dfs.append(df)
        for b in blocks[1:]:
            dfs.append(delayed_pandas_read_text(reader, b, header, kwargs,
                                                dtypes, columns,
                                                enforce=enforce))

    if collection:
        return from_delayed(dfs, head)
    else:
        return dfs


def auto_blocksize(total_memory, cpu_count):
    memory_factor = 10
    blocksize = int(total_memory // cpu_count / memory_factor)
    return min(blocksize, int(64e6))


# guess blocksize if psutil is installed or use acceptable default one if not
if psutil is not None:
    with catch_warnings():
        simplefilter("ignore", RuntimeWarning)
        TOTAL_MEM = psutil.virtual_memory().total
        CPU_COUNT = psutil.cpu_count()
        AUTO_BLOCKSIZE = auto_blocksize(TOTAL_MEM, CPU_COUNT)
else:
    AUTO_BLOCKSIZE = 2**25


def read_pandas(reader, urlpath, blocksize=AUTO_BLOCKSIZE, collection=True,
                lineterminator=None, compression=None, sample=256000,
                enforce=False, storage_options=None, sorted_index_col = None, 
                divisions = None,**kwargs):
    reader_name = reader.__name__
    if lineterminator is not None and len(lineterminator) == 1:
        kwargs['lineterminator'] = lineterminator
    else:
        lineterminator = '\n'
    if 'index' in kwargs or 'index_col' in kwargs: 
        raise ValueError("Keyword 'index' not supported "
                         "dd.{0}(...).set_index('my-index') "
                         "instead. If you can assure, that your "
                         "index column is already sorted, you can "
                         "use dd.{0}(..., sorted_index = 'my-index') "
                         "instead.".format(reader_name))
    if sorted_index_col is not None:
        warn("Setting an index by providing an index column is experimental. "
             "It is upon your responsibility to asure, that the specified "
             "index column is sorted. "
             "The recomendet way of use is dd.{0}(...).set_index('my-index')")
        kwargs['index_col'] = sorted_index_col
    if divisions is not None and blocksize is not None:
        raise ValueError("If you want to provide divisions, please set the "
                         "blocksize attribute to None.")
    for kw in ['iterator', 'chunksize']:
        if kw in kwargs:
            raise ValueError("{0} not supported for "
                             "dd.{1}".format(kw, reader_name))
    if kwargs.get('nrows', None):
        raise ValueError("The 'nrows' keyword is not supported by "
                         "`dd.{0}`. To achieve the same behavior, it's "
                         "recommended to use `dd.{0}(...)."
                         "head(n=nrows)`".format(reader_name))
    if isinstance(kwargs.get('skiprows'), list):
        raise TypeError("List of skiprows not supported for "
                        "dd.{0}".format(reader_name))
    if isinstance(kwargs.get('header'), list):
        raise TypeError("List of header rows not supported for "
                        "dd.{0}".format(reader_name))

    if blocksize and compression not in seekable_files:
        warn("Warning %s compression does not support breaking apart files\n"
             "Please ensure that each individual file can fit in memory and\n"
             "use the keyword ``blocksize=None to remove this message``\n"
             "Setting ``blocksize=None``" % compression)
        blocksize = None
    if compression not in seekable_files and compression not in cfiles:
        raise NotImplementedError("Compression format %s not installed" %
                                  compression)

    b_lineterminator = lineterminator.encode()
    sample, values = read_bytes(urlpath, delimiter=b_lineterminator,
                                blocksize=blocksize,
                                sample=sample,
                                compression=compression,
                                **(storage_options or {}))

    if not isinstance(values[0], (tuple, list)):
        values = [values]

    if kwargs.get('header', 'infer') is None:
        header = b''
    else:
        header = sample.split(b_lineterminator)[0] + b_lineterminator

    head = reader(BytesIO(sample), **kwargs)

    ddf = text_blocks_to_pandas(reader, values, header, head, kwargs,
                                 collection=collection, enforce=enforce)
     
    
    #set divisions if they are provided by the user
    if divisions is not None:
        assert(blocksize == None)
        ddf.divisions = divisions
    
    return ddf  

READ_DOC_TEMPLATE = """
Read {file_type} files into a Dask.DataFrame

This parallelizes the ``pandas.{reader}`` file in the following ways:

1.  It supports loading many files at once using globstrings as follows:

    >>> df = dd.{reader}('myfiles.*.csv')  # doctest: +SKIP

2.  In some cases it can break up large files as follows:

    >>> df = dd.{reader}('largefile.csv', blocksize=25e6)  # 25MB chunks  # doctest: +SKIP

3.  You can read CSV files from external resources (e.g. S3, HDFS)
    providing a URL:

    >>> df = dd.{reader}('s3://bucket/myfiles.*.csv')  # doctest: +SKIP
    >>> df = dd.{reader}('hdfs:///myfiles.*.csv')  # doctest: +SKIP
    >>> df = dd.{reader}('hdfs://namenode.example.com/myfiles.*.csv')  # doctest: +SKIP

Internally ``dd.{reader}`` uses ``pandas.{reader}`` and so supports many
of the same keyword arguments with the same performance guarantees.

See the docstring for ``pandas.{reader}`` for more information on available
keyword arguments.

Note that this function may fail if a {file_type} file includes quoted strings
that contain the line terminator.

Parameters
----------
urlpath : string
    Absolute or relative filepath, URL (may include protocols like
    ``s3://``), or globstring for {file_type} files.
blocksize : int or None
    Number of bytes by which to cut up larger files. Default value is
    computed based on available physical memory and the number of cores.
    If ``None``, use a single block for each file.
collection : boolean
    Return a dask.dataframe if True or list of dask.delayed objects if False
sample : int
    Number of bytes to use when determining dtypes
storage_options : dict
    Extra options that make sense to a particular storage connection, e.g.
    host, port, username, password, etc.
**kwargs : dict
    Options to pass down to ``pandas.{reader}``
"""


def make_reader(reader, reader_name, file_type):
    def read(urlpath, blocksize=AUTO_BLOCKSIZE, collection=True,
             lineterminator=None, compression=None, sample=256000,
             enforce=False, storage_options=None, **kwargs):
        return read_pandas(reader, urlpath, blocksize=blocksize,
                           collection=collection,
                           lineterminator=lineterminator,
                           compression=compression, sample=sample,
                           enforce=enforce, storage_options=storage_options,
                           **kwargs)
    read.__doc__ = READ_DOC_TEMPLATE.format(reader=reader_name,
                                            file_type=file_type)
    read.__name__ = reader_name
    return read


read_csv = make_reader(pd.read_csv, 'read_csv', 'CSV')
read_table = make_reader(pd.read_table, 'read_table', 'delimited')


@delayed
def _to_csv_chunk(df, **kwargs):
    import io
    if PY2:
        out = io.BytesIO()
    else:
        out = io.StringIO()
    df.to_csv(out, **kwargs)
    out.seek(0)
    if PY2:
        return out.getvalue()
    encoding = kwargs.get('encoding', sys.getdefaultencoding())
    return out.getvalue().encode(encoding)


def to_csv(df, filename, name_function=None, compression=None, compute=True,
           get=None, **kwargs):
    values = [_to_csv_chunk(d, **kwargs) for d in df.to_delayed()]
    values = write_bytes(values, filename, name_function, compression,
                         encoding=None)

    if compute:
        from dask import compute
        compute(*values, get=get)
    else:
        return values

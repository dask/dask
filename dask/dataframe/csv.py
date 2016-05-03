from __future__ import print_function, division, absolute_import

from io import BytesIO
from warnings import warn

import numpy as np
import pandas as pd

from ..delayed import delayed
from .io import from_delayed

from ..bytes import read_bytes
from ..bytes.compression import seekable_files, files as cfiles


delayed = delayed(pure=True)


def bytes_read_csv(b, header, kwargs, dtypes=None, columns=None):
    """ Convert a block of bytes to a Pandas DataFrame

    Parameters
    ----------
    b: bytestring
        The content to be parsed with pandas.read_csv
    header: bytestring
        An optional header to prepend to b
    kwargs: dict
        A dictionary of keyword arguments to be passed to pandas.read_csv
    dtypes: dict
        DTypes to assign to columns

    See Also:
        dask.dataframe.csv.read_csv_from_bytes
    """
    bio = BytesIO()
    if not b.startswith(header.rstrip()):
        bio.write(header)
    bio.write(b)
    bio.seek(0)
    df = pd.read_csv(bio, **kwargs)
    if dtypes:
        coerce_dtypes(df, dtypes)

    if columns and (list(df.columns) != list(columns)):
        raise ValueError("Columns do not match", df.columns, columns)
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
                    raise TypeError("Runtime type mismatch. "
                    "Add {'%s': float} to dtype= keyword in read_csv" % c)
            df[c] = df[c].astype(dtypes[c])


def read_csv_from_bytes(block_lists, header, head, kwargs, collection=True,
        enforce_dtypes=True):
    """ Convert blocks of bytes to a dask.dataframe or other high-level object

    This accepts a list of lists of values of bytes where each list corresponds
    to one file, and the value of bytes concatenate to comprise the entire
    file, in order.

    Parameters
    ----------
    block_lists: list of lists of delayed values of bytes
        The lists of bytestrings where each list corresponds to one logical file
    header: bytestring
        The header, found at the front of the first file, to be prepended to
        all blocks
    head: pd.DataFrame
        An example Pandas DataFrame to be used for metadata.
        Can be ``None`` if ``collection==False``
    kwargs: dict
        Keyword arguments to pass down to ``pd.read_csv``
    collection: boolean, optional (defaults to True)

    Returns
    -------
    A dask.dataframe or list of delayed values
    """
    dtypes = head.dtypes.to_dict()
    columns = list(head.columns)
    func = delayed(bytes_read_csv)
    dfs = [func(b, header, kwargs, dtypes, columns)
              for blocks in block_lists
              for b in blocks]

    if collection:
        return from_delayed(dfs, head)
    else:
        return dfs


def read_csv(filename, blocksize=2**25, chunkbytes=None,
        collection=True, lineterminator='\n', compression=None,
        enforce_dtypes=True, sample=10000, **kwargs):
    """ Read CSV files into a Dask.DataFrame

    This parallelizes the ``pandas.read_csv`` file in the following ways:

    1.  It supports loading many files at once using globstrings as follows:

        >>> df = dd.read_csv('myfiles.*.csv')  # doctest: +SKIP

    2.  In some cases it can break up large files as follows:

        >>> df = dd.read_csv('largefile.csv', blocksize=25e6)  # 25MB chunks  # doctest: +SKIP

    Internally dd.read_csv uses pandas.read_csv and so supports many of the
    same keyword arguments with the same performance guarantees.

    See the docstring for ``pandas.read_csv`` for more information on available
    keyword arguments.

    Parameters
    ----------

    filename: string
        Filename or globstring for CSV files.  May include protocols like s3://
    blocksize: int or None
        Number of bytes by which to cut up larger files
    collection: boolean
        Return a dask.dataframe if True or list of dask.delayed objects if False
    sample: int
        Number of bytes to use when determining dtypes
    **kwargs: dict
        Options to pass down to ``pandas.read_csv``
    """
    if len(lineterminator) == 1:
        kwargs['lineterminator'] = lineterminator
    if chunkbytes is not None:
        warn("Deprecation warning: chunksize csv keyword renamed to blocksize")
        blocksize=chunkbytes
    if 'index' in kwargs or 'index_col' in kwargs:
        raise ValueError("Keyword 'index' not supported "
                         "dd.read_csv(...).set_index('my-index') instead")
    for kw in ['iterator', 'chunksize']:
        if kw in kwargs:
            raise ValueError("%s not supported for dd.read_csv" % kw)
    if isinstance(kwargs.get('skiprows'), list):
        raise TypeError("List of skiprows not supported for dd.read_csv")
    if isinstance(kwargs.get('header'), list):
        raise TypeError("List of header rows not supported for dd.read_csv")

    if blocksize and compression not in seekable_files:
        warn("Warning %s compression does not support breaking apart files\n"
             "Please ensure that each individiaul file can fit in memory and\n"
             "use the keyword ``blocksize=None to remove this message``\n"
             "Setting ``blocksize=None``" % compression)
        blocksize = None
    if compression not in seekable_files and compression not in cfiles:
        raise NotImplementedError("Compression format %s not installed" %
                                  compression)

    b_lineterminator = lineterminator.encode()
    sample, values = read_bytes(filename, delimiter=b_lineterminator,
                                          blocksize=blocksize,
                                          sample=sample, compression=compression)
    if not isinstance(values[0], (tuple, list)):
        values = [values]

    if 'nrows' in kwargs:
        values = [[values[0][0]]]

    if kwargs.get('header', 'infer') is None:
        header = b''
    else:
        header = sample.split(b_lineterminator)[0] + b_lineterminator
    head = pd.read_csv(BytesIO(sample), **kwargs)

    df = read_csv_from_bytes(values, header, head, kwargs,
            collection=collection, enforce_dtypes=enforce_dtypes)


    return df

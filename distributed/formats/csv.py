from __future__ import print_function, division, absolute_import

from io import BytesIO

from dask import do

from .compression import compressors, decompressors

from ..executor import default_executor, ensure_default_get
from ..utils import ensure_bytes, log_errors


def bytes_read_csv(b, header, kwargs):
    """ Convert a block of bytes to a Pandas DataFrame

    Parameters
    ----------
    b: bytestring
        The content to be parsed with pandas.read_csv
    header: bytestring
        An optional header to prepend to b
    kwargs: dict
        A dictionary of keyword arguments to be passed to pandas.read_csv

    See Also:
        distributed.formats.csv.read_csv
    """
    import pandas as pd
    with log_errors():
        compression = kwargs.pop('compression', None)
        b2 = decompressors[compression](b)
        bio = BytesIO()
        if header:
            if not header.endswith(b'\n') and not header.endswith(b'\r'):
                header = header + ensure_bytes(kwargs.get('lineterminator', b'\n'))
            bio.write(header)
        bio.write(b2)
        bio.seek(0)
        return pd.read_csv(bio, **kwargs)


def read_csv(block_lists, header, head, kwargs, lazy=True, collection=True,
        executor=None):
    """ Convert blocks of bytes to a dask.dataframe or other high-level object

    This accepts a list of lists of futures/values of bytes where each list
    corresponds to one file, and the futures/values of bytes concatenate to
    comprise the entire file, in order.

    Parameters
    ----------
    block_lists: list of lists of futures of bytes
        The lists of bytestrings with each list corresponding to one logical file
    header: bytestring
        The header, found at the front of the first file, to be prepended to
        all blocks
    head: pd.DataFrame
        An example Pandas DataFrame to be used for metadata
    kwargs: dict
        Keyword arguments to pass down to ``pd.read_csv``
    lazy: boolean, optional (defaults to True)
    collection: boolean, optional (defaults to True)

    Returns
    -------
    A dask.dataframe, or list of futures or values, depending on the value of
    lazy and collection.
    """
    from dask.dataframe import from_imperative
    executor = default_executor(executor)

    dfs1 = [[do(bytes_read_csv)(blocks[0], '', kwargs)] +
            [do(bytes_read_csv)(b, header, kwargs)
                for b in blocks[1:]]
            for blocks in block_lists]
    dfs2 = sum(dfs1, [])

    ensure_default_get(executor)

    if collection:
        result = from_imperative(dfs2, head)
    else:
        result = dfs2

    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)

    return result

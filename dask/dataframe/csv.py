from __future__ import print_function, division, absolute_import

from io import BytesIO

from ..delayed import delayed
from .io import from_delayed

from ..bytes.compression import compressors, decompressors

from ..utils import ensure_bytes


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
        dask.dataframe.csv.read_csv_from_bytes
    """
    import pandas as pd
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


def read_csv_from_bytes(block_lists, header, head, kwargs, collection=True):
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
    dfs1 = [[delayed(bytes_read_csv)(blocks[0], '', kwargs)] +
            [delayed(bytes_read_csv)(b, header, kwargs) for b in blocks[1:]]
            for blocks in block_lists]
    dfs2 = sum(dfs1, [])

    if collection:
        return from_delayed(dfs2, head)
    else:
        return dfs2

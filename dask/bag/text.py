from __future__ import print_function, division, absolute_import

import io
import os

from toolz import concat

from ..utils import system_encoding
from ..delayed import delayed
from ..bytes import open_text_files, read_bytes
from .core import from_delayed

delayed = delayed(pure=True)


def read_text(urlpath, blocksize=None, compression='infer',
              encoding=system_encoding, errors='strict',
              linedelimiter=os.linesep, collection=True,
              storage_options=None):
    """ Read lines from text files

    Parameters
    ----------
    urlpath: string or list
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), globstring, or a list of beforementioned strings.
    blocksize: None or int
        Size (in bytes) to cut up larger files.  Streams by default.
    compression: string
        Compression format like 'gzip' or 'xz'.  Defaults to 'infer'
    encoding: string
    errors: string
    linedelimiter: string
    collection: bool, optional
        Return dask.bag if True, or list of delayed values if false
    storage_options: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> b = read_text('myfiles.1.txt')  # doctest: +SKIP
    >>> b = read_text('myfiles.*.txt')  # doctest: +SKIP
    >>> b = read_text('myfiles.*.txt.gz')  # doctest: +SKIP
    >>> b = read_text('s3://bucket/myfiles.*.txt')  # doctest: +SKIP
    >>> b = read_text('s3://key:secret@bucket/myfiles.*.txt')  # doctest: +SKIP
    >>> b = read_text('hdfs://namenode.example.com/myfiles.*.txt')  # doctest: +SKIP

    Parallelize a large file by providing the number of uncompressed bytes to
    load into each partition.

    >>> b = read_text('largefile.txt', blocksize=1e7)  # doctest: +SKIP

    Returns
    -------
    dask.bag.Bag if collection is True or list of Delayed lists otherwise

    See Also
    --------
    from_sequence: Build bag from Python sequence
    """
    if isinstance(urlpath, (tuple, list, set)):
        blocks = sum([read_text(fn, blocksize=blocksize,
                      compression=compression, encoding=encoding, errors=errors,
                      linedelimiter=linedelimiter, collection=False,
                      storage_options=storage_options)
                     for fn in urlpath], [])
    else:
        if blocksize is None:
            files = open_text_files(urlpath, encoding=encoding, errors=errors,
                                    compression=compression,
                                    **(storage_options or {}))
            blocks = [delayed(list, pure=True)(delayed(file_to_blocks)(file))
                      for file in files]

        else:
            _, blocks = read_bytes(urlpath, delimiter=linedelimiter.encode(),
                                   blocksize=blocksize, sample=False,
                                   compression=compression,
                                   **(storage_options or {}))
            if isinstance(blocks[0], (tuple, list)):
                blocks = list(concat(blocks))
            blocks = [delayed(decode)(b, encoding, errors)
                      for b in blocks]

    if not blocks:
        raise ValueError("No files found", urlpath)

    if not collection:
        return blocks
    else:
        return from_delayed(blocks)


def file_to_blocks(lazy_file):
    with lazy_file as f:
        for line in f:
            yield line


def decode(block, encoding, errors):
    text = block.decode(encoding, errors)
    lines = io.StringIO(text)
    return list(lines)

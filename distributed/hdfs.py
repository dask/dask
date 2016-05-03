""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

import logging
import json
from math import log
import os
import io
from warnings import warn

from dask.delayed import Delayed, delayed
from dask.base import tokenize
import dask.bytes.core
from toolz import merge

from .compatibility import unicode
from .executor import default_executor, ensure_default_get
from .utils import ensure_bytes


logger = logging.getLogger(__name__)


def walk_glob(hdfs, path):
    if '*' not in path and hdfs.info(path)['kind'] == 'directory':
        return sorted([fn for fn in hdfs.walk(path) if fn[-1] != '/'])
    else:
        return sorted(hdfs.glob(path))


def get_block_locations(hdfs, filename):
    """ Get block locations from a filename or globstring """
    return [merge({'filename': fn}, block)
            for fn in walk_glob(hdfs, filename)
            for block in hdfs.get_block_locations(fn)]


def read_block_from_hdfs(filename, offset, length, host=None, port=None,
        delimiter=None):
    from hdfs3 import HDFileSystem
    from locket import lock_file
    with lock_file('.lock'):
        hdfs = HDFileSystem(host=host, port=port)
        bytes = hdfs.read_block(filename, offset, length, delimiter)
    return bytes


def read_bytes(path, executor=None, hdfs=None, lazy=True, delimiter=None,
               not_zero=False, sample=True, blocksize=None, compression=None, **hdfs_auth):
    """ Convert location in HDFS to a list of distributed futures

    Parameters
    ----------
    path: string
        location in HDFS
    executor: Executor (optional)
        defaults to most recently created executor
    hdfs: HDFileSystem (optional)
    lazy: boolean (optional)
        If True then return lazily evaluated dask Values
    delimiter: bytes
        An optional delimiter, like ``b'\n'`` on which to split blocks of bytes
    not_zero: force seek of start-of-file delimiter, discarding header
    **hdfs_auth: keyword arguments
        Extra keywords to send to ``hdfs3.HDFileSystem``

    Returns
    -------
    List of ``distributed.Future`` objects if ``lazy=False``
    or ``dask.Value`` objects if ``lazy=True``
    """
    if compression:
        raise NotImplementedError("hdfs compression")
    from hdfs3 import HDFileSystem
    hdfs = hdfs or HDFileSystem(**hdfs_auth)
    executor = default_executor(executor)
    blocks = get_block_locations(hdfs, path)
    filenames = [d['filename'] for d in blocks]
    offsets = [d['offset'] for d in blocks]
    if not_zero:
        offsets = [max([o, 1]) for o in offsets]
    lengths = [d['length'] for d in blocks]
    workers = [[h.decode() for h in d['hosts']] for d in blocks]
    names = ['read-binary-hdfs3-%s-%s' % (path, tokenize(offset, length, delimiter, not_zero))
            for fn, offset, length in zip(filenames, offsets, lengths)]

    logger.debug("Read %d blocks of binary bytes from %s", len(blocks), path)
    restrictions = dict(zip(names, workers))

    if sample is True:
        sample = 10000
    if sample:
        with hdfs.open(filenames[0], 'rb') as f:
            sample = f.read(sample)
    else:
        sample = b''

    executor._send_to_scheduler({'op': 'update-graph',
                                 'tasks': {},
                                 'dependencies': [],
                                 'keys': [],
                                 'restrictions': restrictions,
                                 'loose_restrictions': names,
                                 'client': executor.id})
    values = [Delayed(name, [{name: (read_block_from_hdfs, fn, offset, length, hdfs.host, hdfs.port, delimiter)}])
              for name, fn, offset, length in zip(names, filenames, offsets, lengths)]

    return sample, values

dask.bytes.core._read_bytes['hdfs'] = read_bytes


def hdfs_open_file(path, auth):
    from hdfs3 import HDFileSystem
    hdfs = HDFileSystem(**auth)
    return hdfs.open(path, mode='rb')


def open_files(path, hdfs=None, lazy=None, **auth):
    if lazy is not None:
        raise DeprecationWarning("Lazy keyword has been deprecated. "
                                 "Now always lazy")
    from hdfs3 import HDFileSystem
    hdfs = hdfs or HDFileSystem(**auth)
    filenames = sorted(hdfs.glob(path))
    myopen = delayed(hdfs_open_file)
    return [myopen(fn, auth) for fn in filenames]


dask.bytes.core._open_files['hdfs'] = open_files


def write_block_to_hdfs(fn, data, hdfs=None):
    """ Write bytes to HDFS """
    if not isinstance(data, bytes):
        raise TypeError("Data to write to HDFS must be of type bytes, got %s" %
                        type(data).__name__)
    with hdfs.open(fn, 'wb') as f:
        f.write(data)
    return len(data)


def write_bytes(path, futures, executor=None, hdfs=None, **hdfs_auth):
    """ Write bytestring futures to HDFS

    Parameters
    ----------
    path: string
        Path on HDFS to write data.  Either globstring like ``/data/file.*.dat``
        or a directory name like ``/data`` (directory will be created)
    futures: list
        List of futures.  Each future should refer to a block of bytes.
    executor: Executor
    hdfs: HDFileSystem

    Returns
    -------
    Futures that wait until writing is complete.  Returns the number of bytes
    written.

    Examples
    --------

    >>> write_bytes('/data/file.*.dat', futures, hdfs=hdfs)  # doctest: +SKIP
    >>> write_bytes('/data/', futures, hdfs=hdfs)  # doctest: +SKIP
    """
    from hdfs3 import HDFileSystem
    hdfs = hdfs or HDFileSystem(**hdfs_auth)
    executor = default_executor(executor)

    n = len(futures)
    n_digits = int(log(n) / log(10))
    template = '%0' + str(n_digits) + 'd'

    if '*' in path:
        dirname = os.path.split(path)[0]
        hdfs.mkdir(dirname)
        filenames = [path.replace('*', template % i) for i in range(n)]
    else:
        hdfs.mkdir(path)
        filenames = [os.path.join(path, template % i) for i in range(n)]

    return executor.map(write_block_to_hdfs, filenames, futures, hdfs=hdfs)


def read_text(path, encoding='utf-8', errors='strict', lineterminator='\n',
               executor=None, hdfs=None, lazy=True, collection=True):
    """ Read text lines from HDFS

    Parameters
    ----------
    path: string
        filename or globstring of files on HDFS
    collection: boolean, optional
        Whether or not to return a high level collection
    lazy: boolean, optional
        Whether or not to start reading immediately

    Returns
    -------
    Dask bag (if collection=True) or Futures or dask values
    """
    warn("hdfs.read_text moved to dask.bag.read_text('hdfs://...')")
    import dask.bag as db
    result = db.read_text('hdfs://' + path, encoding=encoding, errors=errors,
            linedelimiter=lineterminator, hdfs=hdfs, collection=collection)

    executor = default_executor(executor)
    ensure_default_get(executor)
    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)

    return result


def read_text(path, encoding='utf-8', errors='strict', lineterminator='\n',
               executor=None, hdfs=None, lazy=True, collection=True):
    warn("hdfs.read_text moved to dask.bag.read_text('hdfs://...')")
    import dask.bag as db
    result = db.read_text('hdfs://' + path, encoding=encoding, errors=errors,
            linedelimiter=lineterminator, hdfs=hdfs, collection=collection)

    executor = default_executor(executor)
    ensure_default_get(executor)
    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)

    return result


def read_csv(path, executor=None, hdfs=None, lazy=True, collection=True,
        **kwargs):
    warn("hdfs.read_csv moved to dask.dataframe.read_csv('hdfs://...')")
    import dask.dataframe as dd
    result = dd.read_csv('hdfs://' + path, hdfs=hdfs, collection=collection,
            **kwargs)

    executor = default_executor(executor)
    ensure_default_get(executor)
    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)

    return result

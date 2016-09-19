""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

import logging
from math import log
import os
from warnings import warn

import dask.bytes.core
from dask.delayed import delayed
from toolz import merge
from hdfs3 import HDFileSystem

from .client import default_client, ensure_default_get


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
    from locket import lock_file
    with lock_file('.lock'):
        hdfs = HDFileSystem(host=host, port=port)
        bytes = hdfs.read_block(filename, offset, length, delimiter)
    return bytes


def open_file_write(paths, hdfs=None, **kwargs):
    """ Open list of files using delayed """
    if hdfs is None:
        hdfs = HDFileSystem(kwargs.get('host'), kwargs.get('port'))
    out = [delayed(hdfs.open)(path, 'wb') for path in paths]
    return out


def open_file_write_direct(path, hdfs=None, **kwargs):
    if hdfs is None:
        hdfs = HDFileSystem(kwargs.get('host'), kwargs.get('port'))
    return hdfs.open(path, 'wb')

dask.bytes.core._open_files_write['hdfs'] = open_file_write_direct


def read_bytes(path, client=None, hdfs=None, lazy=True, delimiter=None,
               not_zero=False, sample=True, blocksize=None, compression=None, **hdfs_auth):
    """ Convert location in HDFS to a list of distributed futures

    Parameters
    ----------
    path: string
        location in HDFS
    client: Client (optional)
        defaults to most recently created client
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
    hdfs = hdfs or HDFileSystem(**hdfs_auth)
    client = default_client(client)
    blocks = get_block_locations(hdfs, path)
    filenames = [d['filename'] for d in blocks]
    offsets = [d['offset'] for d in blocks]
    if not_zero:
        offsets = [max([o, 1]) for o in offsets]
    lengths = [d['length'] for d in blocks]
    workers = [[h.decode() for h in d['hosts']] for d in blocks]

    logger.debug("Read %d blocks of binary bytes from %s", len(blocks), path)

    if sample is True:
        sample = 10000
    if sample:
        with hdfs.open(filenames[0], 'rb') as f:
            sample = f.read(sample)
    else:
        sample = b''

    f = delayed(read_block_from_hdfs, pure=True)
    values = [f(fn, offset, length, hdfs.host, hdfs.port, delimiter)
              for fn, offset, length in zip(filenames, offsets, lengths)]

    restrictions = {v.key: w for v, w in zip(values, workers)}

    client._send_to_scheduler({'op': 'update-graph',
                                 'tasks': {},
                                 'dependencies': [],
                                 'keys': [],
                                 'restrictions': restrictions,
                                 'loose_restrictions': list(restrictions),
                                 'client': client.id})

    return sample, values

dask.bytes.core._read_bytes['hdfs'] = read_bytes


def hdfs_open_file(path, auth):
    hdfs = HDFileSystem(**auth)
    return hdfs.open(path, mode='rb')


def open_files(path, hdfs=None, lazy=None, **auth):
    if lazy is not None:
        raise DeprecationWarning("Lazy keyword has been deprecated. "
                                 "Now always lazy")
    hdfs = hdfs or HDFileSystem(**auth)
    filenames = sorted(hdfs.glob(path))
    myopen = delayed(hdfs_open_file)
    return [myopen(fn, auth) for fn in filenames]


dask.bytes.core._open_files['hdfs'] = open_files

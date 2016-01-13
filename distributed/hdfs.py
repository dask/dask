""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

import logging
import os

from dask.imperative import Value
from toolz import merge
from hdfs3.utils import seek_delimiter

from .executor import default_executor
from .utils import ignoring


logger = logging.getLogger(__name__)


def read(fn, offset, length, hdfs=None, delimiter=None):
    """ Read a block of bytes from a particular file """
    with hdfs.open(fn, 'r') as f:
        if delimiter:
            f.seek(offset)
            seek_delimiter(f, delimiter, 2**16)
            start = f.tell()
            length -= start - offset

            f.seek(start + length)
            seek_delimiter(f, delimiter, 2**16)
            end = f.tell()
            eof = not f.read(1)

            offset = start
            length = end - start

            if length and not eof:
                length -= len(delimiter)

        f.seek(offset)
        bytes = f.read(length)
        logger.debug("Read %d bytes from %s:%d", len(bytes), fn, offset)
    return bytes


def get_block_locations(hdfs, filename):
    """ Get block locations from a filename or globstring """
    return [merge({'filename': fn}, block)
            for fn in hdfs.glob(filename)
            for block in hdfs.get_block_locations(fn)]


def read_binary(fn, executor=None, hdfs=None, lazy=False, delimiter=None, **hdfs_auth):
    """ Convert location in HDFS to a list of distributed futures

    Parameters
    ----------
    fn: string
        location in HDFS
    executor: Executor (optional)
        defaults to most recently created executor
    hdfs: HDFileSystem
    **hdfs_auth: keyword arguments
        Extra keywords to send to ``hdfs3.HDFileSystem``

    Returns
    -------
    List of ``distributed.Future`` objects
    """
    from hdfs3 import HDFileSystem
    hdfs = hdfs or HDFileSystem(**hdfs_auth)
    executor = default_executor(executor)
    blocks = get_block_locations(hdfs, fn)
    filenames = [d['filename'] for d in blocks]
    offsets = [d['offset'] for d in blocks]
    lengths = [d['length'] for d in blocks]
    workers = [d['hosts'] for d in blocks]
    names = ['read-binary-%s-%d-%d' % (fn, offset, length)
            for fn, offset, length in zip(filenames, offsets, lengths)]


    logger.debug("Read %d blocks of binary bytes from %s", len(blocks), fn)
    if lazy:
        restrictions = dict(zip(names, workers))
        executor._send_to_scheduler({'op': 'update-graph',
                                    'dsk': {},
                                    'keys': [],
                                    'restrictions': restrictions,
                                    'loose_restrictions': set(names)})
        values = [Value(name, [{name: (read, fn, offset, length, hdfs, delimiter)}])
                  for name, fn, offset, length in zip(names, filenames, offsets, lengths)]
        return values
    else:
        return executor.map(read, filenames, offsets, lengths,
                            hdfs=hdfs, delimiter=delimiter, workers=workers, allow_other_workers=True)

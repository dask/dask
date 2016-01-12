""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

import logging
import os

from toolz import merge

from .executor import default_executor
from .utils import ignoring


logger = logging.getLogger(__name__)


def read(fn, offset, length, hdfs=None):
    """ Read a block of bytes from a particular file """
    with hdfs.open(fn, 'r') as f:
        f.seek(offset)
        bytes = f.read(length)
        logger.debug("Read %d bytes from %s:%d", len(bytes), fn, offset)
    return bytes


def get_block_locations(hdfs, filename):
    """ Get block locations from a filename or globstring """
    return [merge({'filename': fn}, block)
            for fn in hdfs.glob(filename)
            for block in hdfs.get_block_locations(fn)]


def read_binary(fn, executor=None, hdfs=None, **hdfs_auth):
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

    logger.debug("Read %d blocks of binary bytes from %s", len(blocks), fn)
    return executor.map(read, filenames, offsets, lengths,
                        hdfs=hdfs, workers=workers, allow_other_workers=True)

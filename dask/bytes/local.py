from __future__ import print_function, division, absolute_import

from glob import glob
import logging
import os
import sys

from .compression import files as compress_files, seekable_files
from .utils import SeekableFile
from ..delayed import delayed
from ..utils import read_block, system_encoding
from ..base import tokenize

logger = logging.getLogger(__name__)


def read_bytes(fn, delimiter=None, not_zero=False, blocksize=2**27,
        sample=True, compression=None):
    """ See dask.bytes.core.read_bytes for docstring """
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)
    if '*' in fn:
        filenames = list(map(os.path.abspath, sorted(glob(fn))))
        sample, first = read_bytes(filenames[0], delimiter, not_zero,
                                blocksize, sample=True, compression=compression)
        rest = [read_bytes(f, delimiter, not_zero, blocksize, sample=False,
                           compression=compression)[1] for f in filenames[1:]]
        return sample, [first] + rest
    else:
        if blocksize is None:
            offsets = [0]
        else:
            size = getsize(fn, compression)

            offsets = list(range(0, size, blocksize))
            if not_zero:
                offsets[0] = 1

        token = tokenize(delimiter, blocksize, not_zero, compression)

        logger.debug("Read %d blocks of binary bytes from %s", len(offsets), fn)

        values = [delayed(read_block_from_file, name='read-file-block-%s-%d-%s' % (fn, offset, token))(fn, offset, blocksize, delimiter, compression)
                  for offset in offsets]

        if sample:
            if isinstance(sample, int) and not isinstance(sample, bool):
                nbytes = sample
            else:
                nbytes = 10000
            sample = read_block_from_file(fn, 0, nbytes, None, compression)

        return sample, values


# TODO add modification time to input
def read_block_from_file(fn, offset, length, delimiter, compression):
    with open(fn, 'rb') as f:
        if compression:
            f = SeekableFile(f)
            f = compress_files[compression](f)
        result = read_block(f, offset, length, delimiter)
        if compression:
            f.close()
    return result


def open_files(path):
    """ Open many files.  Return delayed objects. """
    myopen = delayed(open)
    filenames = sorted(glob(path))
    return [myopen(fn, mode='rb') for fn in filenames]


from . import core
core._read_bytes['local'] = read_bytes
core._open_files['local'] = open_files


if sys.version_info[0] >= 3:
    def open_text_files(path, encoding=system_encoding, errors='strict'):
        """ Open many files in text mode.  Return delayed objects. """
        myopen = delayed(open)
        filenames = sorted(glob(path))
        return [myopen(fn, encoding=encoding, errors=errors)
                for fn in filenames]

    core._open_text_files['local'] = open_text_files


def getsize(fn, compression=None):
    if compression is None:
        return os.path.getsize(fn)
    else:
        with open(fn, 'rb') as f:
            g = seekable_files[compression](f)
            g.seek(0, 2)
            result = g.tell()
            g.close()
        return result

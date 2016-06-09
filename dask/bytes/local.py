from __future__ import print_function, division, absolute_import

from glob import glob
import logging
import os
import sys

from .compression import files as compress_files, seekable_files
from .utils import SeekableFile, read_block
from ..base import tokenize
from ..compatibility import FileNotFoundError
from ..delayed import delayed
from ..utils import system_encoding

logger = logging.getLogger(__name__)


def read_bytes(path, delimiter=None, not_zero=False, blocksize=2**27,
        sample=True, compression=None):
    """ See dask.bytes.core.read_bytes for docstring """
    if '*' in path:
        filenames = list(map(os.path.abspath, sorted(glob(path))))
        sample, first = read_bytes(filenames[0], delimiter, not_zero,
                                   blocksize, sample=True,
                                   compression=compression)
        rest = [read_bytes(f, delimiter, not_zero, blocksize, sample=False,
                           compression=compression)[1]
                for f in filenames[1:]]
        return sample, [first] + rest
    else:
        if not os.path.exists(path):
            raise FileNotFoundError(path)

        if blocksize is None:
            offsets = [0]
        else:
            size = getsize(path, compression)

            offsets = list(range(0, size, blocksize))
            if not_zero:
                offsets[0] = 1

        token = tokenize(path, delimiter, blocksize, not_zero, compression,
                         os.path.getmtime(path))

        logger.debug("Read %d blocks of binary bytes from %s", len(offsets), path)
        f = delayed(read_block_from_file)

        values = [f(path, offset, blocksize, delimiter, compression,
                    dask_key_name='read-file-block-%s-%d' % (token, offset))
                  for offset in offsets]

        if sample:
            if sample is not True:
                nbytes = sample
            else:
                nbytes = 10000
            sample = read_block_from_file(path, 0, nbytes, None, compression)

        return sample, values


def read_block_from_file(path, offset, length, delimiter, compression):
    with open(path, 'rb') as f:
        if compression:
            f = SeekableFile(f)
            f = compress_files[compression](f)
        try:
            result = read_block(f, offset, length, delimiter)
        finally:
            f.close()
    return result


def open_files(path):
    """ Open many files.  Return delayed objects.

    See Also
    --------
    dask.bytes.core.open_files: User function
    """
    myopen = delayed(open)
    filepaths = sorted(glob(path))
    return [myopen(_path, mode='rb',
                   dask_key_name='open-%s' % tokenize(_path,
                                                      os.path.getmtime(_path)))
            for _path in filepaths]


from . import core
core._read_bytes['file'] = read_bytes
core._open_files['file'] = open_files


if sys.version_info[0] >= 3:
    def open_text_files(path, encoding=system_encoding, errors='strict'):
        """ Open many files in text mode.  Return delayed objects.

        See Also
        --------
        dask.bytes.core.open_text_files: User function
        """
        myopen = delayed(open)
        filepaths = sorted(glob(path))
        return [myopen(_path, encoding=encoding, errors=errors,
                       dask_key_name='open-%s'
                                     % tokenize(_path,
                                                encoding,
                                                errors,
                                                os.path.getmtime(_path)))
                for _path in filepaths]

    core._open_text_files['file'] = open_text_files


def getsize(path, compression=None):
    if compression is None:
        return os.path.getsize(path)
    else:
        with open(path, 'rb') as f:
            f = SeekableFile(f)
            g = seekable_files[compression](f)
            g.seek(0, 2)
            result = g.tell()
            g.close()
        return result

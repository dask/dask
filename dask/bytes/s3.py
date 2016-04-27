from __future__ import print_function, division, absolute_import

import logging

from s3fs import S3FileSystem

from .compression import files as compress_files, seekable_files
from .utils import read_block

from ..base import tokenize
from ..delayed import delayed

logger = logging.getLogger(__name__)


def read_bytes(path, s3=None, delimiter=None, not_zero=False, blocksize=2**27,
               sample=True, compression=None, **s3_params):
    """ Convert location in S3 to a list of delayed values

    Parameters
    ----------
    path: string
        location in S3
    s3: S3FileSystem (optional)
    delimiter: bytes
        An optional delimiter, like ``b'\n'`` on which to split blocks of bytes
    not_zero: force seek of start-of-file delimiter, discarding header
    blocksize: int (=128MB)
        Chunk size
    sample: bool, int
        Whether or not to return a sample from the first 10k bytes
    compression: string or None
        String like 'gzip' or 'xz'.  Must support efficient random access.
    **s3_params: keyword arguments
        Extra keywords to send to boto3 session (anon, key, secret...)

    Returns
    -------
    10kB sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``path`` is a globstring.
    """
    if s3 is None:
        s3 = S3FileSystem(**s3_params)

    if '*' in path:
        filenames = sorted(s3.glob(path))
        sample, first = read_bytes(filenames[0], s3, delimiter, not_zero,
                                   blocksize, sample=True,
                                   compression=compression, **s3_params)
        rest = [read_bytes(f, s3, delimiter, not_zero, blocksize,
                       sample=False, compression=compression,  **s3_params)[1]
                for f in filenames[1:]]
        return sample, [first] + rest
    else:
        if blocksize is None:
            offsets = [0]
        else:
            size = getsize(path, compression, s3)
            offsets = list(range(0, size, blocksize))
            if not_zero:
                offsets[0] = 1

        info = s3.ls(path, detail=True)[0]

        token = tokenize(info['ETag'], delimiter, blocksize, not_zero, compression)

        logger.debug("Read %d blocks of binary bytes from %s", len(offsets), path)

        s3safe_pars = s3_params.copy()
        s3safe_pars.update(s3.get_delegated_s3pars())

        values = [delayed(read_block_from_s3)(path, offset, blocksize,
                    s3safe_pars, delimiter, compression,
                    dask_key_name='read-block-s3-%s-%d' % (token, offset))
                    for offset in offsets]

        if sample:
            if isinstance(sample, int) and not isinstance(sample, bool):
                nbytes = sample
            else:
                nbytes = 10000
            sample = read_block_from_s3(path, 0, nbytes, s3safe_pars, delimiter,
                                        compression)

        return sample, values


def read_block_from_s3(filename, offset, length, s3_params={}, delimiter=None,
                       compression=None):
    s3 = S3FileSystem(**s3_params)
    with s3.open(filename, 'rb') as f:
        if compression:
            f = compress_files[compression](f)
        try:
            result = read_block(f, offset, length, delimiter)
        finally:
            f.close()
    return result


def s3_open_file(path, s3_params):
    s3 = S3FileSystem(**s3_params)
    return s3.open(path, mode='rb')


def open_files(path, s3=None, **s3_params):
    """ Open many files.  Return delayed objects.

    See Also
    --------
    dask.bytes.core.open_files:  User function
    """
    s3 = s3 or S3FileSystem(**s3_params)
    filenames = sorted(s3.glob(path))
    myopen = delayed(s3_open_file)
    s3_params = s3_params.copy()
    s3_params.update(s3.get_delegated_s3pars())

    return [myopen(path, s3_params,
                   dask_key_name='s3-open-file-%s' % s3.info(path)['ETag'])
            for path in filenames]


def getsize(path, compression, s3):
    if compression is None:
        return s3.info(path)['Size']
    else:
        with s3.open(path, 'rb') as f:
            g = seekable_files[compression](f)
            g.seek(0, 2)
            result = g.tell()
            g.close()
        return result


from . import core
core._read_bytes['s3'] = read_bytes
core._open_files['s3'] = open_files

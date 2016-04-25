from __future__ import print_function, division, absolute_import

import logging

from s3fs import S3FileSystem

from .compression import files as compress_files

from ..base import tokenize
from ..delayed import delayed
from ..utils import read_block

logger = logging.getLogger(__name__)


def read_bytes(fn, s3=None, delimiter=None, not_zero=False, blocksize=2**27,
               sample=True, compression=None, **s3_params):
    """ Convert location in S3 to a list of delayed values

    Parameters
    ----------
    fn: string
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
    delayed objects if ``fn`` is a globstring.
    """
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)

    if s3 is None:
        s3 = S3FileSystem(**s3_params)

    if '*' in fn:
        filenames = sorted(s3.glob(fn))
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
            size = s3.info(fn)['Size']
            offsets = list(range(0, size, blocksize))
            if not_zero:
                offsets[0] = 1

        token = tokenize(delimiter, blocksize, not_zero, compression)

        logger.debug("Read %d blocks of binary bytes from %s", len(offsets), fn)

        s3safe_pars = s3_params.copy()
        if not s3.anon:
            s3safe_pars.update(s3.get_delegated_s3pars())

        values = [delayed(read_block_from_s3, name='read-s3-block-%s-%d-%s-%s'
            % (fn, offset, blocksize, token))(fn, offset, blocksize, s3safe_pars,
                delimiter, compression) for offset in offsets]

        if sample:
            if isinstance(sample, int) and not isinstance(sample, bool):
                nbytes = sample
            else:
                nbytes = 10000
            sample = read_block_from_s3(fn, 0, nbytes, s3safe_pars, delimiter,
                                        compression)

        return sample, values


def read_block_from_s3(filename, offset, length, s3_params={}, delimiter=None,
                       compression=None):
    s3 = S3FileSystem(**s3_params)
    with s3.open(filename, 'rb') as f:
        if compression:
            f = compress_files[compression](f)
        result = read_block(f, offset, length, delimiter)
        if compression:
            f.close()
    return result


def s3_open_file(fn, s3_params):
    s3 = S3FileSystem(**s3_params)
    return s3.open(fn, mode='rb')


def open_files(path, mode='rb', s3=None, **s3_params):
    """ Open many files.  Return delayed objects. """
    if mode != 'rb':
        raise NotImplementedError("Only support readbyte mode, got %s" % mode)

    s3 = s3 or S3FileSystem(**s3_params)
    filenames = sorted(s3.glob(path))
    myopen = delayed(s3_open_file)
    s3_params = s3_params.copy()
    s3_params.update(s3.get_delegated_s3pars())

    return [myopen(fn, s3_params) for fn in filenames]


from . import core
core._read_bytes['s3'] = read_bytes
core._open_files['s3'] = open_files

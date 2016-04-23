from __future__ import print_function, division, absolute_import

import logging

from s3fs import S3FileSystem

from ..delayed import delayed
from ..base import tokenize

logger = logging.getLogger(__name__)


def read_bytes(fn, s3=None, delimiter=None, not_zero=False, blocksize=2**27,
               sample=True, **s3_params):
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
    **s3_params: keyword arguments
        Extra keywords to send to boto3 session (anon, key, secret...)

    Returns
    -------
    10kB sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``fn`` is a globstring.
    """
    if s3 is None:
        s3 = S3FileSystem(**s3_params)

    if '*' in fn:
        filenames = sorted(s3.glob(fn))
        sample, first = read_bytes(filenames[0], s3, delimiter, not_zero,
                                   blocksize, sample=True, **s3_params)
        rest = [read_bytes(f, s3, delimiter, not_zero, blocksize,
                       sample=False, **s3_params)[1] for f in filenames[1:]]
        return sample, [first] + rest
    else:
        if blocksize is None:
            offsets = [0]
        else:
            size = s3.info(fn)['Size']
            offsets = list(range(0, size, blocksize))
            if not_zero:
                offsets[0] = 1

        token = tokenize(delimiter, blocksize, not_zero)

        logger.debug("Read %d blocks of binary bytes from %s", len(offsets), fn)

        s3safe_pars = s3_params.copy()
        s3safe_pars.update(s3.get_delegated_s3pars())

        values = [delayed(read_block_from_s3, name='read-s3-block-%s-%d-%s-%s'
            % (fn, offset, blocksize, token))(fn, offset, blocksize, s3safe_pars,
                delimiter) for offset in offsets]

        if sample:
            if isinstance(sample, int) and not isinstance(sample, bool):
                nbytes = sample
            else:
                nbytes = 10000
            sample = read_block_from_s3(fn, 0, nbytes, s3safe_pars, delimiter)

        return sample, values


def read_block_from_s3(filename, offset, length, s3_params={}, delimiter=None):
    s3 = S3FileSystem(**s3_params)
    return s3.read_block(filename, offset, length, delimiter)


from . import storage_systems
storage_systems['s3'] = read_bytes

from __future__ import print_function, division, absolute_import

from glob import glob
import logging
import os

from .compression import files as compress_files
from ..delayed import delayed
from ..utils import read_block
from ..base import tokenize

logger = logging.getLogger(__name__)


def read_bytes(fn, delimiter=None, not_zero=False, blocksize=2**27,
        sample=True, compression=None):
    """ Convert location on filesystem to a list of delayed values

    Parameters
    ----------
    fn: string
        location in S3
    delimiter: bytes
        An optional delimiter, like ``b'\n'`` on which to split blocks of bytes
    not_zero: force seek of start-of-file delimiter, discarding header
    blocksize: int (=128MB)
        Chunk size
    compression: string or None
        String like 'gzip' or 'xz'.  Must support efficient random access.
    sample: bool, int
        Whether or not to return a sample from the first 10k bytes

    Returns
    -------
    10kB sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``fn`` is a globstring.
    """
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)
    if '*' in fn:
        filenames = sorted(glob(fn))
        sample, first = read_bytes(filenames[0], delimiter, not_zero,
                                blocksize, sample=True, compression=compression)
        rest = [read_bytes(f, delimiter, not_zero, blocksize, sample=False,
                           compression=compression)[1] for f in filenames[1:]]
        return sample, [first] + rest
    else:
        if blocksize is None:
            offsets = [0]
        else:
            size = os.path.getsize(fn)
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
            sample = read_block_from_file(fn, 0, nbytes, delimiter, compression)

        return sample, values


# TODO add modification time to input
def read_block_from_file(fn, offset, length, delimiter, compression):
    with open(fn, 'rb') as f:
        if compression:
            f = compress_files[compression](f)
        if length is None and offset == 0:
            result = f.read()
        else:
            result = read_block(f, offset, length, delimiter)
        if compression:
            f.close()
    return result


from . import storage_systems
storage_systems[None] = read_bytes

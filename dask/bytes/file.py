from __future__ import print_function, division, absolute_import

from glob import glob
import logging
import os

from ..delayed import delayed
from ..utils import read_block
from ..base import tokenize

logger = logging.getLogger(__name__)


def read_bytes(fn, delimiter=None, not_zero=False, blocksize=2**27, sample=True):
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
    sample: bool, int
        Whether or not to return a sample from the first 10k bytes

    Returns
    -------
    10kB sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``fn`` is a globstring.
    """
    if '*' in fn:
        filenames = sorted(glob(fn))
        sample, first = read_bytes(filenames[0], delimiter, not_zero,
                                    blocksize, sample=True)
        rest = [read_bytes(f, delimiter, not_zero, blocksize, sample=False)[1]
                 for f in filenames[1:]]
        return sample, [first] + rest
    else:
        if blocksize is None:
            offsets = [0]
        else:
            size = os.path.getsize(fn)
            offsets = list(range(0, size, blocksize))
            if not_zero:
                offsets[0] = 1

        token = tokenize(delimiter, blocksize, not_zero)

        logger.debug("Read %d blocks of binary bytes from %s", len(offsets), fn)

        values = [delayed(read_block_from_file, name='read-file-block-%s-%d-%s-%s' % (fn, offset, blocksize, token))(fn, offset, blocksize, delimiter)
                  for offset in offsets]

        if sample:
            if isinstance(sample, int) and not isinstance(sample, bool):
                nbytes = sample
            else:
                nbytes = 10000
            sample = read_block_from_file(fn, 0, nbytes, delimiter)

        return sample, values


# TODO add modification time to input
def read_block_from_file(fn, offset, length, delimiter):
    with open(fn, 'rb') as f:
        return read_block(f, offset, length, delimiter)


from . import storage_systems
storage_systems[None] = read_bytes

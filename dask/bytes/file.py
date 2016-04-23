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
    List of ``dask.Delayed`` objects
    """
    filenames, lengths, offsets = [], [], []
    if blocksize is None:
        filenames = sorted(glob(fn))
        lengths = [None] * len(filenames)
        offsets = [0] * len(filenames)
    else:
        for afile in sorted(glob(fn)):
            size = os.path.getsize(afile)
            offset = list(range(0, size, blocksize))
            if not_zero:
                offset[0] = 1
            offsets.extend(offset)
            filenames.extend([afile]*len(offset))
            lengths.extend([blocksize]*len(offset))

    token = tokenize(delimiter, blocksize, not_zero)
    names = ['read-file-block-%s-%d-%s-%s' % (fn, offset, length, token)
             for fn, offset, length in zip(filenames, offsets, lengths)]

    logger.debug("Read %d blocks of binary bytes from %s", len(offsets), fn)

    values = [delayed(read_block_from_file, name=name)(
                    fn, offset, length, delimiter)
              for fn, offset, length, name
              in zip(filenames, offsets, lengths, names)]

    if sample:
        if isinstance(sample, int) and not isinstance(sample, bool):
            nbytes = sample
        else:
            nbytes = 10000
        sample = read_block_from_file(filenames[0], 0, nbytes, delimiter)

    return sample, values


# TODO add modification time to input
def read_block_from_file(fn, offset, length, delimiter):
    with open(fn, 'rb') as f:
        return read_block(f, offset, length, delimiter)

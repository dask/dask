from __future__ import print_function, division, absolute_import

from glob import glob
import logging
import os

from ..delayed import delayed
from ..utils import read_block

logger = logging.getLogger(__name__)


def read_bytes(fn, delimiter=None, not_zero=False, blocksize=2**27):
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

    logger.debug("Read %d blocks of binary bytes from %s", len(offsets), fn)

    read = delayed(read_block_from_file)

    return [read(fn, offset, length, delimiter)
             for fn, offset, length in zip(filenames, offsets, lengths)]


# TODO add modification time to input
def read_block_from_file(fn, offset, length, delimiter):
    with open(fn, 'rb') as f:
        return read_block(f, offset, length, delimiter)

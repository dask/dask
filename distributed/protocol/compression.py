"""
Record known compressors

Includes utilities for determining whether or not to compress
"""
from __future__ import print_function, division, absolute_import

import logging
import random

from toolz import identity, partial

from ..config import config
from ..utils import ignoring, ensure_bytes


compressions = {None: {'compress': identity,
                       'decompress': identity}}

default_compression = None


logger = logging.getLogger(__file__)


with ignoring(ImportError):
    import zlib
    compressions['zlib'] = {'compress': zlib.compress,
                            'decompress': zlib.decompress}

with ignoring(ImportError):
    import snappy
    compressions['snappy'] = {'compress': snappy.compress,
                              'decompress': snappy.decompress}
    default_compression = 'snappy'

with ignoring(ImportError):
    import lz4
    compressions['lz4'] = {'compress': lz4.LZ4_compress,
                           'decompress': lz4.LZ4_uncompress}
    default_compression = 'lz4'

with ignoring(ImportError):
    import blosc
    compressions['blosc'] = {'compress': partial(blosc.compress, clevel=5,
                                                 cname='lz4'),
                             'decompress': blosc.decompress}


default = config.get('compression', 'auto')
if default != 'auto':
    if default in compressions:
        default_compression = default
    else:
        raise ValueError("Default compression '%s' not found.\n"
                "Choices include auto, %s" % (
                    default, ', '.join(sorted(map(str, compressions)))))


def byte_sample(b, size, n):
    """ Sample a bytestring from many locations

    Parameters
    ----------
    b: bytes or memoryview
    size: int
        size of each sample to collect
    n: int
        number of samples to collect
    """
    starts = [random.randint(0, len(b) - size) for j in range(n)]
    ends = []
    for i, start in enumerate(starts[:-1]):
        ends.append(min(start + size, starts[i + 1]))
    ends.append(starts[-1] + size)

    parts = [b[start:end] for start, end in zip(starts, ends)]
    return b''.join(map(ensure_bytes, parts))


def maybe_compress(payload, compression=default_compression, min_size=1e4,
                   sample_size=1e4, nsamples=5):
    """
    Maybe compress payload

    1.  We don't compress small messages
    2.  We sample the payload in a few spots, compress that, and if it doesn't
        do any good we return the original
    3.  We then compress the full original, it it doesn't compress well then we
        return the original
    4.  We return the compressed result
    """
    if not compression:
        return None, payload
    if len(payload) < min_size:
        return None, payload
    if len(payload) > 2**31:
        return None, payload

    min_size = int(min_size)
    sample_size = int(sample_size)

    compress = compressions[compression]['compress']

    # Compress a sample, return original if not very compressed
    sample = byte_sample(payload, sample_size, nsamples)
    if len(compress(sample)) > 0.9 * len(sample):  # not very compressible
        return None, payload

    compressed = compress(ensure_bytes(payload))
    if len(compressed) > 0.9 * len(payload):  # not very compressible
        return None, payload
    else:
        return compression, compressed

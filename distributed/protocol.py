"""
The distributed message protocol consists of the following parts:

1.  The length of the header, stored as a uint32
2.  The header, stored as msgpack.
    If there are no fields in the header then we skip it entirely.
3.  The payload, stored as possibly compressed msgpack
4.  A sentinel value

**Header**

The Header contains the following fields:

* **compression**: string, optional
    One of the following: ``'snappy', 'lz4', 'zlib'`` or missing for None

**Payload**

The payload is any msgpack serializable value.  It may be compressed based
on the header.

**Sentinel**

We often terminate each message with a sentinel value.  This happens
outside of this module though and is not baked in.
"""
from __future__ import print_function, division, absolute_import

import random
import struct

try:
    import pandas.msgpack as msgpack
except ImportError:
    import msgpack

from toolz import first, keymap, identity, merge

from .utils import ignoring
from .compatibility import unicode


compressions = {None: {'compress': identity,
                       'decompress': identity}}

default_compression = None


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


def dumps(msg):
    """ Transform Python value to bytestream suitable for communication """
    small_header = {}

    if isinstance(msg, dict):
        big = {k: v for k, v in msg.items()
                    if isinstance(v, bytes) and len(v) > 1e6}
    else:
        big = False
    if big:
        small = {k: v for k, v in msg.items() if k not in big}
    else:
        small = msg

    frames = dumps_msgpack(small)
    if big:
        frames += dumps_big_byte_dict(big)

    return frames


def loads(frames):
    """ Transform bytestream back into Python value """
    header, payload, frames = frames[0], frames[1], frames[2:]
    msg = loads_msgpack(header, payload)

    if frames:
        big = loads_big_byte_dict(*frames)
        msg.update(big)

    return msg


def byte_sample(b, size, n):
    """ Sample a bytestring from many locations """
    starts = [random.randint(0, len(b) - size) for j in range(n)]
    ends = []
    for i, start in enumerate(starts[:-1]):
        ends.append(min(start + size, starts[i + 1]))
    ends.append(starts[-1] + size)

    return b''.join([b[start:end] for start, end in zip(starts, ends)])


def maybe_compress(payload, compression=default_compression, min_size=1e4,
        sample_size=1e4, nsamples=5):
    """ Maybe compress payload

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

    min_size = int(min_size)
    sample_size = int(sample_size)

    compress = compressions[compression]['compress']

    # Compress a sample, return original if not very compressed
    sample = byte_sample(payload, sample_size, nsamples)
    if len(compress(sample)) > 0.9 * len(sample):  # not very compressible
        return None, payload

    compressed = compress(payload)
    if len(compressed) > 0.9 * len(payload):  # not very compressible
        return None, payload

    return compression, compress(payload)


def dumps_msgpack(msg):
    """ Dump msg into header and payload, both bytestrings

    All of the message must be msgpack encodable

    See Also:
        loads_msgpack
    """
    header = {}
    payload = msgpack.dumps(msg, use_bin_type=True)

    fmt, payload = maybe_compress(payload)
    if fmt:
        header['compression'] = fmt

    if header:
        header_bytes = msgpack.dumps(header, use_bin_type=True)
    else:
        header_bytes = b''

    return [header_bytes, payload]


def loads_msgpack(header, payload):
    """ Read msgpack header and payload back to Python object

    See Also:
        dumps_msgpack
    """
    if header:
        header = msgpack.loads(header, encoding='utf8')
    else:
        header = {}

    if header.get('compression'):
        try:
            decompress = compressions[header['compression']]['decompress']
            payload = decompress(payload)
        except KeyError:
            raise ValueError("Data is compressed as %s but we don't have this"
                    " installed" % header['compression'].decode())

    return msgpack.loads(payload, encoding='utf8')


def dumps_big_byte_dict(d):
    """ Serialize large byte dictionary to sequence of frames

    The input must be a dictionary and all values of that dictionary must be
    bytestrings.  These should probably be large.

    Returns a sequence of frames, one header followed by each of the values

    See Also:
        loads_big_byte_dict
    """
    assert isinstance(d, dict) and all(isinstance(v, bytes) for v in d.values())
    keys, values = zip(*d.items())

    compress = compressions[default_compression]['compress']
    compression = []
    values2 = []
    for v in values:
        fmt, vv = maybe_compress(v)
        compression.append(fmt)
        values2.append(vv)

    header = {'encoding': 'big-byte-dict',
              'keys': keys,
              'compression': compression}

    return [msgpack.dumps(header, use_bin_type=True)] + values2


def loads_big_byte_dict(header, *values):
    """ Deserialize big-byte frames to large byte dictionary

    See Also:
        dumps_big_byte_dict
    """
    header = msgpack.loads(header, encoding='utf8')

    values2 = [compressions[c]['decompress'](v)
               for c, v in zip(header['compression'], values)]
    return dict(zip(header['keys'], values2))

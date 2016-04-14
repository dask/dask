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


def dumps_msgpack(msg):
    """ Dump msg into header and payload, both bytestrings

    All of the message must be msgpack encodable

    See Also:
        loads_msgpack
    """
    header = {}
    payload = msgpack.dumps(msg, use_bin_type=True)

    if len(payload) > 1e3 and default_compression:
        compressed = compressions[default_compression]['compress'](payload)
        if len(compressed) < 0.9 * len(payload):  # significant compression
            header['compression'] = default_compression
            payload = compressed

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
        compressed = compress(v)
        if len(compressed) < 0.9 * len(v):
            compression.append(default_compression)
            values2.append(compressed)
        else:
            compression.append(None)
            values2.append(v)

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

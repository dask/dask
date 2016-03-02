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
from io import BytesIO
import struct

try:
    import pandas.msgpack as msgpack
except ImportError:
    import msgpack

from toolz import first, keymap

from .utils import ignoring
from .compatibility import unicode


compressions = {}

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
    header = {}

    payload = msgpack.dumps(msg, use_bin_type=True)

    if len(payload) > 1e3 and default_compression:
        payload = compressions[default_compression]['compress'](payload)
        header['compression'] = default_compression

    if header:
        header_bytes = msgpack.dumps(header, use_bin_type=True)
    else:
        header_bytes = b''
    out = BytesIO()
    out.write(struct.pack('I', len(header_bytes)))
    out.write(header_bytes)
    out.write(payload)

    out.seek(0)
    return out.read()


def loads(b):
    """ Transform bytestream back into Python value """
    header_length, = struct.unpack('I', b[:4])
    if header_length:
        header = msgpack.loads(b[4: header_length + 4], encoding='utf8')
    else:
        header = {}
    payload = b[header_length + 4:]

    if header.get('compression'):
        try:
            decompress = compressions[header['compression']]['decompress']
            payload = decompress(payload)
        except KeyError:
            raise ValueError("Data is compressed as %s but we don't have this"
                    " installed" % header['compression'].decode())

    msg = msgpack.loads(payload, encoding='utf8')

    if header.get('decode'):
        if isinstance(msg, dict) and msg:
            msg = keymap(bytes.decode, msg)
        elif isinstance(msg, bytes):
            msg = msg.decode()
        else:
            raise TypeError("Asked to decode a %s" % type(msg).__name__)

    return msg

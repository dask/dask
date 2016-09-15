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
from copy import deepcopy

try:
    import pandas.msgpack as msgpack
except ImportError:
    import msgpack

from toolz import identity, get_in

from .utils import ignoring


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


from .config import config
default = config.get('compression', 'auto')
if default != 'auto':
    if default in compressions:
        default_compression = default
    else:
        raise ValueError("Default compression '%s' not found.\n"
                "Choices include auto, %s" % (
                    default, ', '.join(sorted(map(str, compressions)))))


BIG_BYTES_SIZE = 2**20
BIG_BYTES_SHARD_SIZE = 2**28


def extract_big_bytes(x):
    big = {}
    _extract_big_bytes(x, big)
    if big:
        x = deepcopy(x)
        for path in big:
            t = get_in(path[:-1], x)
            if isinstance(t, dict):
                del t[path[-1]]
            else:
                t[path[-1]] = None
    return x, big


def _extract_big_bytes(x, big, path=()):
    if type(x) is dict:
        for k, v in x.items():
            if isinstance(v, (list, dict)):
                _extract_big_bytes(v, big, path + (k,))
            elif type(v) is bytes and len(v) >= BIG_BYTES_SIZE:
                big[path + (k,)] = v
    elif type(x) is list:
        for k, v in enumerate(x):
            if isinstance(v, (list, dict)):
                _extract_big_bytes(v, big, path + (k,))
            elif type(v) is bytes and len(v) >= BIG_BYTES_SIZE:
                big[path + (k,)] = v


def dumps(msg):
    """ Transform Python value to bytestream suitable for communication """
    big = {}
    # Only lists and dicts can contain big values
    if isinstance(msg, (list, dict)):
        msg, big = extract_big_bytes(msg)
    small_header, small_payload = dumps_msgpack(msg)
    if not big:
        return small_header, small_payload

    # Shard the big segments
    shards = []
    res = {}
    for k, v in list(big.items()):
        L = []
        for i, j in enumerate(range(0, len(v), BIG_BYTES_SHARD_SIZE)):
            key = '.shard-%d-%s' % (i, k)
            res[key] = v[j: j + BIG_BYTES_SHARD_SIZE]
            L.append(key)
        shards.append((k, L))

    keys, values = zip(*res.items())

    compression = []
    values2 = []
    for v in values:
        fmt, vv = maybe_compress(v)
        compression.append(fmt)
        values2.append(vv)

    header = {'encoding': 'big-byte-dict',
              'keys': keys,
              'compression': compression,
              'shards': shards}

    return [small_header, small_payload,
            msgpack.dumps(header, use_bin_type=True)] + values2


def loads(frames):
    """ Transform bytestream back into Python value """
    small_header, small_payload, frames = frames[0], frames[1], frames[2:]
    msg = loads_msgpack(small_header, small_payload)

    if frames:
        header = msgpack.loads(frames[0], encoding='utf8')

        values2 = [compressions[c]['decompress'](v)
                   for c, v in zip(header['compression'], frames[1:])]
        lk = dict(zip(header['keys'], values2))

        for k, keys in header['shards']:
            v = b''.join(lk.pop(kk) for kk in keys)
            get_in(k[:-1], msg)[k[-1]] = v

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
    if len(payload) > 2**31:
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
    else:
        return compression, compressed


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

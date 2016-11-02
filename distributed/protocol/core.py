from __future__ import print_function, division, absolute_import

from copy import deepcopy
from functools import partial
import logging

try:
    import pandas.msgpack as msgpack
except ImportError:
    import msgpack

from toolz import identity, get_in, valmap

from .compression import compressions, maybe_compress
from .serialize import (serialize, deserialize, Serialize, Serialized,
        to_serialize)
from .utils import frame_split_size, merge_frames

from ..utils import ignoring

_deserialize = deserialize


logger = logging.getLogger(__file__)


def dumps(msg):
    """ Transform Python message to bytestream suitable for communication """
    try:
        data = {}
        # Only lists and dicts can contain serialized values
        if isinstance(msg, (list, dict)):
            msg, data = extract_serialize(msg)
        small_header, small_payload = dumps_msgpack(msg)

        if not data:  # fast path without serialized data
            return small_header, small_payload

        pre = {key: (value.header, value.frames)
               for key, value in data.items()
               if type(value) is Serialized}

        data = {key: serialize(value.data)
                     for key, value in data.items()
                     if type(value) is Serialize}

        header = {'headers': {},
                  'keys': []}
        out_frames = []

        for key, (head, frames) in data.items():
            if 'lengths' not in head:
                head['lengths'] = list(map(len, frames))
            if 'compression' not in head:
                frames = frame_split_size(frames)
                compression, frames = zip(*map(maybe_compress, frames))
                head['compression'] = compression
            head['count'] = len(frames)
            header['headers'][key] = head
            header['keys'].append(key)
            out_frames.extend(frames)

        for key, (head, frames) in pre.items():
            if 'lengths' not in head:
                head['lengths'] = list(map(len, frames))
            head['count'] = len(frames)
            header['headers'][key] = head
            header['keys'].append(key)
            out_frames.extend(frames)

        out_frames = [bytes(f) for f in out_frames]

        return [small_header, small_payload,
                msgpack.dumps(header, use_bin_type=True)] + out_frames
    except Exception as e:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


def loads(frames, deserialize=True):
    """ Transform bytestream back into Python value """
    try:
        small_header, small_payload, frames = frames[0], frames[1], frames[2:]
        msg = loads_msgpack(small_header, small_payload)
        if not frames:
            return msg

        header, frames = frames[0], frames[1:]
        header = msgpack.loads(header, encoding='utf8', use_list=False)
        keys = header['keys']
        headers = header['headers']

        for key in keys:
            head = headers[key]
            lengths = head['lengths']
            count = head['count']
            fs, frames = frames[:count], frames[count:]

            if deserialize:
                fs = decompress(head, fs)
                fs = merge_frames(head, fs)
                value = _deserialize(head, fs)
            else:
                value = Serialized(head, fs)

            get_in(key[:-1], msg)[key[-1]] = value

        return msg
    except Exception as e:
        logger.critical("Failed to deerialize", exc_info=True)
        raise


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
                             " installed" % str(header['compression']))

    return msgpack.loads(payload, encoding='utf8')


def extract_serialize(x):
    """ Pull out Serialize objects from message

    Examples
    --------
    >>> from distributed.protocol import to_serialize
    >>> msg = {'op': 'update', 'data': to_serialize(123)}
    >>> extract_serialize(msg)
    ({'op': 'update'}, {('data',): <Serialize: 123>})
    """
    ser = {}
    _extract_serialize(x, ser)
    if ser:
        x = deepcopy(x)
        for path in ser:
            t = get_in(path[:-1], x)
            if isinstance(t, dict):
                del t[path[-1]]
            else:
                t[path[-1]] = None

    return x, ser


def _extract_serialize(x, ser, path=()):
    if type(x) is dict:
        for k, v in x.items():
            if isinstance(v, (list, dict)):
                _extract_serialize(v, ser, path + (k,))
            elif type(v) is Serialize or type(v) is Serialized:
                ser[path + (k,)] = v
    elif type(x) is list:
        for k, v in enumerate(x):
            if isinstance(v, (list, dict)):
                _extract_serialize(v, ser, path + (k,))
            elif type(v) is Serialize or type(v) is Serialized:
                ser[path + (k,)] = v


def decompress(header, frames):
    """ Decompress frames according to information in the header """
    return [compressions[c]['decompress'](frame)
            for c, frame in zip(header['compression'], frames)]

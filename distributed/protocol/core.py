from __future__ import print_function, division, absolute_import

import logging
import operator

import msgpack

try:
    from cytoolz import reduce
except ImportError:
    from toolz import reduce

from .compression import compressions, maybe_compress, decompress
from .serialize import serialize, deserialize, Serialize, Serialized, extract_serialize
from .utils import frame_split_size, merge_frames, msgpack_opts
from ..utils import nbytes

_deserialize = deserialize


logger = logging.getLogger(__name__)


def dumps(msg, serializers=None, on_error="message", context=None):
    """ Transform Python message to bytestream suitable for communication """
    try:
        data = {}
        # Only lists and dicts can contain serialized values
        if isinstance(msg, (list, dict)):
            msg, data, bytestrings = extract_serialize(msg)
        small_header, small_payload = dumps_msgpack(msg)

        if not data:  # fast path without serialized data
            return small_header, small_payload

        pre = {
            key: (value.header, value.frames)
            for key, value in data.items()
            if type(value) is Serialized
        }

        data = {
            key: serialize(
                value.data, serializers=serializers, on_error=on_error, context=context
            )
            for key, value in data.items()
            if type(value) is Serialize
        }

        header = {"headers": {}, "keys": [], "bytestrings": list(bytestrings)}

        out_frames = []

        for key, (head, frames) in data.items():
            if "lengths" not in head:
                head["lengths"] = tuple(map(nbytes, frames))
            if "compression" not in head:
                frames = frame_split_size(frames)
                if frames:
                    compression, frames = zip(*map(maybe_compress, frames))
                else:
                    compression = []
                head["compression"] = compression
            head["count"] = len(frames)
            header["headers"][key] = head
            header["keys"].append(key)
            out_frames.extend(frames)

        for key, (head, frames) in pre.items():
            if "lengths" not in head:
                head["lengths"] = tuple(map(nbytes, frames))
            head["count"] = len(frames)
            header["headers"][key] = head
            header["keys"].append(key)
            out_frames.extend(frames)

        for i, frame in enumerate(out_frames):
            if type(frame) is memoryview and frame.strides != (1,):
                try:
                    frame = frame.cast("b")
                except TypeError:
                    frame = frame.tobytes()
                out_frames[i] = frame

        return [
            small_header,
            small_payload,
            msgpack.dumps(header, use_bin_type=True),
        ] + out_frames
    except Exception:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


def loads(frames, deserialize=True, deserializers=None):
    """ Transform bytestream back into Python value """
    frames = frames[::-1]  # reverse order to improve pop efficiency
    if not isinstance(frames, list):
        frames = list(frames)
    try:
        small_header = frames.pop()
        small_payload = frames.pop()
        msg = loads_msgpack(small_header, small_payload)
        if not frames:
            return msg

        header = frames.pop()
        header = msgpack.loads(header, use_list=False, **msgpack_opts)
        keys = header["keys"]
        headers = header["headers"]
        bytestrings = set(header["bytestrings"])

        for key in keys:
            head = headers[key]
            count = head["count"]
            if count:
                fs = frames[-count::][::-1]
                del frames[-count:]
            else:
                fs = []

            if deserialize or key in bytestrings:
                if "compression" in head:
                    fs = decompress(head, fs)
                fs = merge_frames(head, fs)
                value = _deserialize(head, fs, deserializers=deserializers)
            else:
                value = Serialized(head, fs)

            def put_in(keys, coll, val):
                """Inverse of get_in, but does type promotion in the case of lists"""
                if keys:
                    holder = reduce(operator.getitem, keys[:-1], coll)
                    if isinstance(holder, tuple):
                        holder = list(holder)
                        coll = put_in(keys[:-1], coll, holder)
                    holder[keys[-1]] = val
                else:
                    coll = val
                return coll

            msg = put_in(key, msg, value)

        return msg
    except Exception:
        logger.critical("Failed to deserialize", exc_info=True)
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
        header["compression"] = fmt

    if header:
        header_bytes = msgpack.dumps(header, use_bin_type=True)
    else:
        header_bytes = b""

    return [header_bytes, payload]


def loads_msgpack(header, payload):
    """ Read msgpack header and payload back to Python object

    See Also:
        dumps_msgpack
    """
    if header:
        header = msgpack.loads(header, use_list=False, **msgpack_opts)
    else:
        header = {}

    if header.get("compression"):
        try:
            decompress = compressions[header["compression"]]["decompress"]
            payload = decompress(payload)
        except KeyError:
            raise ValueError(
                "Data is compressed as %s but we don't have this"
                " installed" % str(header["compression"])
            )

    return msgpack.loads(payload, use_list=False, **msgpack_opts)

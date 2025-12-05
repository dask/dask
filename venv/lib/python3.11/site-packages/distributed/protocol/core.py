from __future__ import annotations

import logging

import msgpack

from distributed.protocol import pickle
from distributed.protocol.compression import decompress, maybe_compress
from distributed.protocol.serialize import (
    Pickled,
    Serialize,
    Serialized,
    ToPickle,
    _is_msgpack_serializable,
    merge_and_deserialize,
    msgpack_decode_default,
    msgpack_encode_default,
    serialize_and_split,
)
from distributed.protocol.utils import msgpack_opts
from distributed.utils import ensure_memoryview

logger = logging.getLogger(__name__)


def dumps(  # type: ignore[no-untyped-def]
    msg, serializers=None, on_error="message", context=None, frame_split_size=None
) -> list:
    """Transform Python message to bytestream suitable for communication

    Developer Notes
    ---------------
    The approach here is to use `msgpack.dumps()` to serialize `msg` and
    write the result to the first output frame. If `msgpack.dumps()`
    encounters an object it cannot serialize like a NumPy array, it is handled
    out-of-band by `_encode_default()` and appended to the output frame list.
    """
    try:
        if context and "compression" in context:
            compress_opts = {"compression": context["compression"]}
        else:
            compress_opts = {}

        def _inplace_compress_frames(header, frames):
            compression = list(header.get("compression", [None] * len(frames)))

            for i in range(len(frames)):
                if compression[i] is None:
                    compression[i], frames[i] = maybe_compress(
                        frames[i], **compress_opts
                    )

            header["compression"] = tuple(compression)

        def create_serialized_sub_frames(obj: Serialized | Serialize) -> list:
            if isinstance(obj, Serialized):
                sub_header, sub_frames = obj.header, obj.frames
            else:
                sub_header, sub_frames = serialize_and_split(
                    obj,
                    serializers=serializers,
                    on_error=on_error,
                    context=context,
                    size=frame_split_size,
                )
                _inplace_compress_frames(sub_header, sub_frames)
            sub_header["num-sub-frames"] = len(sub_frames)
            sub_header = msgpack.dumps(
                sub_header, default=msgpack_encode_default, use_bin_type=True
            )
            return [sub_header] + sub_frames

        def create_pickled_sub_frames(obj: Pickled | ToPickle) -> list:
            if isinstance(obj, Pickled):
                sub_header, sub_frames = obj.header, obj.frames
            else:
                sub_frames = []
                sub_header = {
                    "pickled-obj": pickle.dumps(
                        obj.data,
                        # In to support len() and slicing, we convert `PickleBuffer`
                        # objects to memoryviews of bytes.
                        buffer_callback=lambda x: sub_frames.append(
                            ensure_memoryview(x)
                        ),
                    )
                }
                _inplace_compress_frames(sub_header, sub_frames)

            sub_header["num-sub-frames"] = len(sub_frames)
            sub_header = msgpack.dumps(sub_header)
            return [sub_header] + sub_frames

        frames = [None]

        def _encode_default(obj):
            if isinstance(obj, (Serialize, Serialized)):
                offset = len(frames)
                frames.extend(create_serialized_sub_frames(obj))
                return {"__Serialized__": offset}
            elif isinstance(obj, (ToPickle, Pickled)):
                offset = len(frames)
                frames.extend(create_pickled_sub_frames(obj))
                return {"__Pickled__": offset}
            else:
                return msgpack_encode_default(obj)

        try:
            frames[0] = msgpack.dumps(msg, default=_encode_default, use_bin_type=True)
        except TypeError as e:
            logger.info(
                f"Failed to serialize ({e}); falling back to pickle. "
                "Be aware that this may degrade performance."
            )

            def _encode_default_safe(obj):
                encoded = _encode_default(obj)
                if encoded is not obj or _is_msgpack_serializable(obj):
                    return encoded

                obj = ToPickle(obj)
                offset = len(frames)
                frames.extend(create_pickled_sub_frames(obj))
                return {"__Pickled__": offset}

            # If possible, we want to avoid the performance penalty from the checks
            # implemented in _encode_default_safe to fall back to pickle, so we
            # try to serialize the data without the fallback first assuming that
            # this succeeds in the overwhelming majority of cases.
            frames[0] = msgpack.dumps(
                msg, default=_encode_default_safe, use_bin_type=True
            )
        return frames

    except Exception:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


def loads(frames, deserialize=True, deserializers=None):
    """Transform bytestream back into Python value"""

    try:

        def _decode_default(obj):
            offset = obj.get("__Serialized__", 0)
            if offset > 0:
                sub_header = msgpack.loads(
                    frames[offset],
                    object_hook=msgpack_decode_default,
                    use_list=False,
                    **msgpack_opts,
                )
                offset += 1
                sub_frames = frames[offset : offset + sub_header["num-sub-frames"]]
                if deserialize:
                    if "compression" in sub_header:
                        sub_frames = decompress(sub_header, sub_frames)
                    return merge_and_deserialize(
                        sub_header, sub_frames, deserializers=deserializers
                    )
                else:
                    return Serialized(sub_header, sub_frames)

            offset = obj.get("__Pickled__", 0)
            if offset > 0:
                sub_header = msgpack.loads(frames[offset])
                offset += 1
                sub_frames = frames[offset : offset + sub_header["num-sub-frames"]]
                if "compression" in sub_header:
                    sub_frames = decompress(sub_header, sub_frames)
                return pickle.loads(sub_header["pickled-obj"], buffers=sub_frames)
            return msgpack_decode_default(obj)

        return msgpack.loads(
            frames[0], object_hook=_decode_default, use_list=False, **msgpack_opts
        )

    except Exception:
        logger.critical("Failed to deserialize", exc_info=True)
        raise

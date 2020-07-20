import struct
import msgpack

from ..utils import nbytes

BIG_BYTES_SHARD_SIZE = 2 ** 26


msgpack_opts = {
    ("max_%s_len" % x): 2 ** 31 - 1 for x in ["str", "bin", "array", "map", "ext"]
}
msgpack_opts["strict_map_key"] = False

try:
    msgpack.loads(msgpack.dumps(""), raw=False, **msgpack_opts)
    msgpack_opts["raw"] = False
except TypeError:
    # Backward compat with old msgpack (prior to 0.5.2)
    msgpack_opts["encoding"] = "utf-8"


def frame_split_size(frame, n=BIG_BYTES_SHARD_SIZE) -> list:
    """
    Split a frame into a list of frames of maximum size

    This helps us to avoid passing around very large bytestrings.

    Examples
    --------
    >>> frame_split_size([b'12345', b'678'], n=3)  # doctest: +SKIP
    [b'123', b'45', b'678']
    """
    if nbytes(frame) <= n:
        return [frame]

    if nbytes(frame) > n:
        if isinstance(frame, (bytes, bytearray)):
            frame = memoryview(frame)
        try:
            itemsize = frame.itemsize
        except AttributeError:
            itemsize = 1

        return [
            frame[i : i + n // itemsize]
            for i in range(0, nbytes(frame) // itemsize, n // itemsize)
        ]


def merge_frames(header, frames):
    """ Merge frames into original lengths

    Examples
    --------
    >>> merge_frames({'lengths': [3, 3]}, [b'123456'])
    [b'123', b'456']
    >>> merge_frames({'lengths': [6]}, [b'123', b'456'])
    [b'123456']
    """
    lengths = list(header["lengths"])
    frames = list(map(memoryview, frames))

    assert sum(lengths) == sum(map(nbytes, frames))

    if not all(len(f) == l for f, l in zip(frames, lengths)):
        frames = frames[::-1]
        lengths = lengths[::-1]

        out = []
        while lengths:
            l = lengths.pop()
            L = []
            while l:
                frame = frames.pop()
                if nbytes(frame) <= l:
                    L.append(frame)
                    l -= nbytes(frame)
                else:
                    L.append(frame[:l])
                    frames.append(frame[l:])
                    l = 0
            if len(L) == 1:  # no work necessary
                out.append(L[0])
            else:
                out.append(memoryview(bytearray().join(L)))
        frames = out

    frames = [memoryview(bytearray(f)) if f.readonly else f for f in frames]

    return frames


def pack_frames_prelude(frames):
    nframes = len(frames)
    nbytes_frames = map(nbytes, frames)
    return struct.pack(f"Q{nframes}Q", nframes, *nbytes_frames)


def pack_frames(frames):
    """ Pack frames into a byte-like object

    This prepends length information to the front of the bytes-like object

    See Also
    --------
    unpack_frames
    """
    prelude = [pack_frames_prelude(frames)]

    if not isinstance(frames, list):
        frames = list(frames)

    return b"".join(prelude + frames)


def unpack_frames(b):
    """ Unpack bytes into a sequence of frames

    This assumes that length information is at the front of the bytestring,
    as performed by pack_frames

    See Also
    --------
    pack_frames
    """
    (n_frames,) = struct.unpack("Q", b[:8])

    frames = []
    start = 8 + n_frames * 8
    for i in range(n_frames):
        (length,) = struct.unpack("Q", b[(i + 1) * 8 : (i + 2) * 8])
        frame = b[start : start + length]
        frames.append(frame)
        start += length

    return frames

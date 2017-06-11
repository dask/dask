from __future__ import print_function, division, absolute_import

import struct

from ..utils import ensure_bytes, nbytes

BIG_BYTES_SHARD_SIZE = 2**28


def frame_split_size(frames, n=BIG_BYTES_SHARD_SIZE):
    """
    Split a list of frames into a list of frames of maximum size

    This helps us to avoid passing around very large bytestrings.

    Examples
    --------
    >>> frame_split_size([b'12345', b'678'], n=3)  # doctest: +SKIP
    [b'123', b'45', b'678']
    """
    if not frames:
        return frames

    if max(map(len, frames)) <= n:
        return frames

    out = []
    for frame in frames:
        if len(frame) > n or type(frame) is memoryview and frame.nbytes > n:
            if isinstance(frame, bytes):
                frame = memoryview(frame)
            try:
                itemsize = frame.itemsize
            except AttributeError:
                itemsize = 1
            for i in range(0, nbytes(frame), n // itemsize):
                out.append(frame[i: i + n])
        else:
            out.append(frame)
    return out


def merge_frames(header, frames):
    """ Merge frames into original lengths

    Examples
    --------
    >>> merge_frames({'lengths': [3, 3]}, [b'123456'])
    [b'123', b'456']
    >>> merge_frames({'lengths': [6]}, [b'123', b'456'])
    [b'123456']
    """
    lengths = list(header['lengths'])

    if not frames:
        return frames

    assert sum(lengths) == sum(map(len, frames))

    if all(len(f) == l for f, l in zip(frames, lengths)):
        return frames

    frames = frames[::-1]
    lengths = lengths[::-1]

    out = []
    while lengths:
        l = lengths.pop()
        L = []
        while l:
            frame = frames.pop()
            if len(frame) <= l:
                L.append(frame)
                l -= len(frame)
            else:
                mv = memoryview(frame)
                L.append(mv[:l])
                frames.append(mv[l:])
                l = 0
        out.append(b''.join(map(ensure_bytes, L)))
    return out


def pack_frames_prelude(frames):
    lengths = [len(f) for f in frames]
    lengths = ([struct.pack('Q', len(frames))] +
               [struct.pack('Q', nbytes(frame)) for frame in frames])
    return b''.join(lengths)


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

    return b''.join(prelude + frames)


def unpack_frames(b):
    """ Unpack bytes into a sequence of frames

    This assumes that length information is at the front of the bytestring,
    as performed by pack_frames

    See Also
    --------
    pack_frames
    """
    (n_frames,) = struct.unpack('Q', b[:8])

    frames = []
    start = 8 + n_frames * 8
    for i in range(n_frames):
        (length,) = struct.unpack('Q', b[(i + 1) * 8: (i + 2) * 8])
        frame = b[start: start + length]
        frames.append(frame)
        start += length

    return frames

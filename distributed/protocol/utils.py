from __future__ import print_function, division, absolute_import

from ..utils import ensure_bytes

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
        if len(frame) > n:
            if isinstance(frame, bytes):
                frame = memoryview(frame)
            for i in range(0, len(frame), n):
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
    lengths = list(header['lengths'])[::-1]
    frames = list(frames)[::-1]

    if not frames:
        return frames

    assert sum(lengths) == sum(map(len, frames))

    if all(len(f) == l for f, l in zip(frames, lengths)):
        return frames

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

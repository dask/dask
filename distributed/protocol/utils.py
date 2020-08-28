import struct

from ..utils import nbytes

BIG_BYTES_SHARD_SIZE = 2 ** 26


msgpack_opts = {
    ("max_%s_len" % x): 2 ** 31 - 1 for x in ["str", "bin", "array", "map", "ext"]
}
msgpack_opts["strict_map_key"] = False
msgpack_opts["raw"] = False


def frame_split_size(frame, n=BIG_BYTES_SHARD_SIZE) -> list:
    """
    Split a frame into a list of frames of maximum size

    This helps us to avoid passing around very large bytestrings.

    Examples
    --------
    >>> frame_split_size([b'12345', b'678'], n=3)  # doctest: +SKIP
    [b'123', b'45', b'678']
    """
    frame = memoryview(frame)

    if frame.nbytes <= n:
        return [frame]

    nitems = frame.nbytes // frame.itemsize
    items_per_shard = n // frame.itemsize

    return [frame[i : i + items_per_shard] for i in range(0, nitems, items_per_shard)]


def merge_frames(header, frames):
    """Merge frames into original lengths

    Examples
    --------
    >>> merge_frames({'lengths': [3, 3]}, [b'123456'])
    [b'123', b'456']
    >>> merge_frames({'lengths': [6]}, [b'123', b'456'])
    [b'123456']
    """
    lengths = list(header["lengths"])
    writeables = list(header["writeable"])

    assert len(lengths) == len(writeables)
    assert sum(lengths) == sum(map(nbytes, frames))

    if all(len(f) == l for f, l in zip(frames, lengths)):
        return [
            (bytearray(f) if w else bytes(f)) if w == memoryview(f).readonly else f
            for w, f in zip(header["writeable"], frames)
        ]

    frames = frames[::-1]
    lengths = lengths[::-1]
    writeables = writeables[::-1]

    out = []
    while lengths:
        l = lengths.pop()
        w = writeables.pop()
        L = []
        while l:
            frame = frames.pop()
            if nbytes(frame) <= l:
                L.append(frame)
                l -= nbytes(frame)
            else:
                frame = memoryview(frame)
                L.append(frame[:l])
                frames.append(frame[l:])
                l = 0
        if len(L) == 1 and w != memoryview(L[0]).readonly:  # no work necessary
            out.extend(L)
        elif w:
            out.append(bytearray().join(L))
        else:
            out.append(bytes().join(L))

    return out


def pack_frames_prelude(frames):
    nframes = len(frames)
    nbytes_frames = map(nbytes, frames)
    return struct.pack(f"Q{nframes}Q", nframes, *nbytes_frames)


def pack_frames(frames):
    """Pack frames into a byte-like object

    This prepends length information to the front of the bytes-like object

    See Also
    --------
    unpack_frames
    """
    return b"".join([pack_frames_prelude(frames), *frames])


def unpack_frames(b):
    """Unpack bytes into a sequence of frames

    This assumes that length information is at the front of the bytestring,
    as performed by pack_frames

    See Also
    --------
    pack_frames
    """
    b = memoryview(b)

    fmt = "Q"
    fmt_size = struct.calcsize(fmt)

    (n_frames,) = struct.unpack_from(fmt, b)
    lengths = struct.unpack_from(f"{n_frames}{fmt}", b, fmt_size)

    frames = []
    start = fmt_size * (1 + n_frames)
    for length in lengths:
        end = start + length
        frame = b[start:end]
        frames.append(frame)
        start = end

    return frames

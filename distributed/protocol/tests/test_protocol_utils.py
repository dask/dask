import pytest

from distributed.protocol.utils import merge_frames, pack_frames, unpack_frames
from distributed.utils import ensure_bytes, is_writeable


@pytest.mark.parametrize(
    "lengths,writeable,frames",
    [
        ([3], [False], [b"123"]),
        ([3], [True], [b"123"]),
        ([3], [None], [b"123"]),
        ([3], [False], [bytearray(b"123")]),
        ([3], [True], [bytearray(b"123")]),
        ([3], [None], [bytearray(b"123")]),
        ([3, 3], [False, False], [b"123", b"456"]),
        ([2, 3, 2], [False, True, None], [b"12345", b"67"]),
        ([2, 3, 2], [False, True, None], [bytearray(b"12345"), bytearray(b"67")]),
        ([5, 2], [False, True], [b"123", b"45", b"67"]),
        ([3, 4], [None, False], [b"12", b"34", b"567"]),
    ],
)
def test_merge_frames(lengths, writeable, frames):
    header = {"lengths": lengths, "writeable": writeable}
    result = merge_frames(header, frames)

    data = b"".join(frames)
    expected = []
    for i in lengths:
        expected.append(data[:i])
        data = data[i:]

    writeables = list(map(is_writeable, result))
    assert (r == e for r, e in zip(writeables, header["writeable"]) if e is not None)
    assert list(map(ensure_bytes, result)) == expected


def test_pack_frames():
    frames = [b"123", b"asdf"]
    b = pack_frames(frames)
    assert isinstance(b, bytes)
    frames2 = unpack_frames(b)

    assert frames == frames2

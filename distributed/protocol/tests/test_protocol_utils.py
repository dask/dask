import pytest

from distributed.protocol.utils import merge_frames, pack_frames, unpack_frames
from distributed.utils import ensure_bytes


@pytest.mark.parametrize(
    "lengths,frames",
    [
        ([3], [b"123"]),
        ([3, 3], [b"123", b"456"]),
        ([2, 3, 2], [b"12345", b"67"]),
        ([5, 2], [b"123", b"45", b"67"]),
        ([3, 4], [b"12", b"34", b"567"]),
    ],
)
def test_merge_frames(lengths, frames):
    header = {"lengths": lengths}
    result = merge_frames(header, frames)

    data = b"".join(frames)
    expected = []
    for i in lengths:
        expected.append(data[:i])
        data = data[i:]

    assert all(isinstance(f, memoryview) for f in result)
    assert all(not f.readonly for f in result)
    assert list(map(ensure_bytes, result)) == expected


def test_pack_frames():
    frames = [b"123", b"asdf"]
    b = pack_frames(frames)
    assert isinstance(b, bytes)
    frames2 = unpack_frames(b)

    assert frames == frames2

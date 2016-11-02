from __future__ import print_function, division, absolute_import

from distributed.protocol.utils import merge_frames
from distributed.utils import ensure_bytes

def test_merge_frames():
    result = merge_frames({'lengths': [3, 4]}, [b'12', b'34', b'567'])
    expected = [b'123', b'4567']

    assert list(map(ensure_bytes, result)) == expected

    b = b'123'
    assert merge_frames({'lengths': [3]}, [b])[0] is b

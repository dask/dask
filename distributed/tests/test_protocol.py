from __future__ import print_function, division, absolute_import

from distributed.protocol import loads, dumps, msgpack
import pytest

def test_protocol():
    for msg in [1, 'a', b'a', {'x': 1}, {b'x': 1}, {}]:
        assert loads(*dumps(msg)) == msg


def test_compression():
    pytest.importorskip('lz4')
    np = pytest.importorskip('numpy')
    x = np.ones(1000000)
    header, payload = dumps(x.tobytes())
    assert len(payload) < x.nbytes
    y = loads(header, payload)
    assert x.tobytes() == y


def test_compression():
    pytest.importorskip('lz4')
    np = pytest.importorskip('numpy')
    x = np.random.random(10000)
    header, payload = dumps(x.tobytes())
    assert (not header or
            not msgpack.loads(header, encoding='utf8').get('compression'))


def test_small():
    assert sum(map(len, dumps(b''))) < 10
    assert sum(map(len, dumps(1))) < 10

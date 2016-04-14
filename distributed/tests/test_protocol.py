from __future__ import print_function, division, absolute_import

from distributed.protocol import (loads, dumps, dumps_msgpack, loads_msgpack,
        dumps_big_byte_dict, loads_big_byte_dict, msgpack, maybe_compress)
import pytest

def test_protocol():
    for msg in [1, 'a', b'a', {'x': 1}, {b'x': 1}, {'x': b''}, {}]:
        assert loads(dumps(msg)) == msg


def test_compression():
    pytest.importorskip('lz4')
    np = pytest.importorskip('numpy')
    x = np.ones(1000000)
    header, payload = dumps(x.tobytes())
    assert len(payload) < x.nbytes
    y = loads([header, payload])
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


def test_small_and_big():
    d = {'x': [1, 2, 3], 'y': b'0' * 10000000}
    L = dumps(d)
    assert loads(L) == d
    # assert loads([small_header, small]) == {'x': [1, 2, 3]}
    # assert loads([big_header, big]) == {'y': d['y']}


def test_big_bytes():
    d = {'x': b'123', b'y': b'4567'}
    L = dumps_big_byte_dict(d)
    assert isinstance(L, (tuple, list))
    assert all(isinstance(item, bytes) for item in L)
    dd = loads_big_byte_dict(*L)
    assert dd == d


def test_big_bytes_protocol():
    np = pytest.importorskip('numpy')
    data = np.random.randint(0, 255, dtype='u1', size=2000000).tobytes()
    d = {'x': data, 'y': b'1' * 2000000}
    L = dumps(d)
    assert d['x'] in L
    dd = loads(L)
    assert dd == d


def test_maybe_compress():
    import zlib
    payload = b'123'
    assert maybe_compress(payload, None) == (None, payload)
    assert maybe_compress(payload, 'zlib') == (None, payload)

    assert maybe_compress(b'111', 'zlib') == (None, b'111')

    payload = b'0' * 10000
    assert maybe_compress(payload, 'zlib') == ('zlib', zlib.compress(payload))


def test_maybe_compress_sample():
    np = pytest.importorskip('numpy')
    lz4 = pytest.importorskip('lz4')
    payload = np.random.randint(0, 255, dtype='u1', size=10000).tobytes()
    fmt, compressed = maybe_compress(payload, 'lz4')
    assert fmt == None
    assert compressed == payload

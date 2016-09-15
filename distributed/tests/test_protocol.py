from __future__ import print_function, division, absolute_import

from distributed.protocol import loads, dumps, msgpack, maybe_compress
import pytest


def test_protocol():
    for msg in [1, 'a', b'a', {'x': 1}, {b'x': 1}, {'x': b''}, {}]:
        assert loads(dumps(msg)) == msg


def test_compression_1():
    pytest.importorskip('lz4')
    np = pytest.importorskip('numpy')
    x = np.ones(1000000)
    header, payload = dumps(x.tobytes())
    assert len(payload) < x.nbytes
    y = loads([header, payload])
    assert x.tobytes() == y


def test_compression_2():
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


def test_big_bytes_protocol():
    np = pytest.importorskip('numpy')
    data = np.random.randint(0, 255, dtype='u1', size=2**21).tobytes()

    d = {'x': data, 'y': 'foo'}
    frames = dumps(d)
    assert len(frames) == 4     # Only `data` is extracted
    assert data is frames[3]    # `data` isn't sharded as it's too short
    dd = loads(frames)
    assert dd == d

    d = {'x': [data], 'y': 'foo'}
    frames = dumps(d)
    assert len(frames) == 4
    assert data is frames[3]
    dd = loads(frames)
    assert dd == d

    d = {'x': {'z': [data, 'small_data']}, 'y': 'foo'}
    frames = dumps(d)
    assert len(frames) == 4
    assert data is frames[3]
    dd = loads(frames)
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


def test_large_messages():
    psutil = pytest.importorskip('psutil')
    pytest.importorskip('lz4')
    if psutil.virtual_memory().total < 8e9:
        return

    def f(n):
        """
        Want to avoid compiling b'0' * 2**31 as a constant during
        setup.py install, so we turn this into a function and call it in
        the next line

        Otherwise this takes up 2 GB of memory during install
        """
        return b'0' * (2**n + 10)
    big_bytes = f(31)
    msg = {'x': [big_bytes, b'small_bytes'],
           'y': {'a': big_bytes, 'b': b'small_bytes'}}

    b = dumps(msg)
    msg2 = loads(b)
    assert msg == msg2

    assert len(b) >= 2
    big_header = msgpack.loads(b[2], encoding='utf8')
    assert len(big_header['shards']) == 2
    assert len(big_header['keys']) + 2 + 1 == len(b)

    msg = [big_bytes, {'x': big_bytes, 'y': b'small_bytes'}]
    b = dumps(msg)
    msg2 = loads(b)
    assert msg == msg2

    assert len(b) >= 2
    big_header = msgpack.loads(b[2], encoding='utf8')
    assert len(big_header['shards']) == 2
    assert len(big_header['keys']) + 2 + 1 == len(b)

from distributed.protocol import loads, dumps
import pytest

def test_protocol():
    for msg in [1, 'a', b'a', {'x': 1}, {b'x': 1}, {}]:
        assert loads(dumps(msg)) == msg


def test_compression():
    pytest.importorskip('lz4')
    np = pytest.importorskip('numpy')
    x = np.ones(1000000)
    b = dumps(x.tobytes())
    assert len(b) < x.nbytes
    y = loads(b)
    assert x.tobytes() == y


def test_small():
    assert len(dumps(b'')) < 10
    assert len(dumps(1)) < 10

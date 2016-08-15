import io

import pytest

from dask.bytes.utils import read_block, seek_delimiter


def test_read_block():
    delimiter = b'\n'
    data = delimiter.join([b'123', b'456', b'789'])
    f = io.BytesIO(data)

    assert read_block(f, 1, 2) == b'23'
    assert read_block(f, 0, 1, delimiter=b'\n') == b'123\n'
    assert read_block(f, 0, 2, delimiter=b'\n') == b'123\n'
    assert read_block(f, 0, 3, delimiter=b'\n') == b'123\n'
    assert read_block(f, 0, 5, delimiter=b'\n') == b'123\n456\n'
    assert read_block(f, 0, 8, delimiter=b'\n') == b'123\n456\n789'
    assert read_block(f, 0, 100, delimiter=b'\n') == b'123\n456\n789'
    assert read_block(f, 1, 1, delimiter=b'\n') == b''
    assert read_block(f, 1, 5, delimiter=b'\n') == b'456\n'
    assert read_block(f, 1, 8, delimiter=b'\n') == b'456\n789'

    for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)],
                [(0, 4), (4, 4), (8, 4)]]:
        out = [read_block(f, o, l, b'\n') for o, l in ols]
        assert b"".join(filter(None, out)) == data


def test_seek_delimiter_endline():
    f = io.BytesIO(b'123\n456\n789')

    # if at zero, stay at zero
    seek_delimiter(f, b'\n', 5)
    assert f.tell() == 0

    # choose the first block
    for bs in [1, 5, 100]:
        f.seek(1)
        seek_delimiter(f, b'\n', blocksize=bs)
        assert f.tell() == 4

    # handle long delimiters well, even with short blocksizes
    f = io.BytesIO(b'123abc456abc789')
    for bs in [1, 2, 3, 4, 5, 6, 10]:
        f.seek(1)
        seek_delimiter(f, b'abc', blocksize=bs)
        assert f.tell() == 6

    # End at the end
    f = io.BytesIO(b'123\n456')
    f.seek(5)
    seek_delimiter(f, b'\n', 5)
    assert f.tell() == 7


def test_ensure_protocol():
    try:
        import hdfs3  # noqa: F401
        pytest.skip()
    except ImportError:
        pass

    dd = pytest.importorskip('dask.dataframe')
    try:
        dd.read_csv('hdfs://data/*.csv')
    except RuntimeError as e:
        assert "hdfs3" in str(e)

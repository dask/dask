from io import BytesIO

import pytest
from dask.bytes.compression import compressors, decompressors, files

def test_compression():
    assert set(compressors) == set(decompressors)

    a = b'Hello, world!'
    for k in compressors:
        compress = compressors[k]
        decompress = decompressors[k]
        b = compress(a)
        c = decompress(b)
        assert a == c
        if k is not None:
            assert a != b


@pytest.mark.parametrize('fmt,File', files.items())
def test_files(fmt,File):
    data = b'1234'*1000
    out = BytesIO()
    f = File(out, mode='wb')
    f.write(data)
    f.close()

    out.seek(0)
    compressed = out.read()

    assert len(data) > len(compressed)

    g = File(BytesIO(compressed), mode='rb')
    data2 = g.read()
    g.close()
    assert data == data2

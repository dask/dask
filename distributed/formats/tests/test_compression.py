from distributed.formats.compression import compressors, decompressors

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

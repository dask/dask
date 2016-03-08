from __future__ import print_function, division, absolute_import

from distributed.compatibility import gzip_compress, gzip_decompress

def test_gzip():
    b = b'Hello, world!'
    c = gzip_compress(b)
    d = gzip_decompress(c)
    assert b == d

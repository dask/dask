from __future__ import print_function, division, absolute_import

from distributed.compatibility import (
    gzip_compress, gzip_decompress, finalize)


def test_gzip():
    b = b'Hello, world!'
    c = gzip_compress(b)
    d = gzip_decompress(c)
    assert b == d


def test_finalize():
    class C(object):
        pass

    l = []

    def cb(value):
        l.append(value)

    o = C()
    f = finalize(o, cb, 1)
    assert f in f._select_for_exit()
    f.atexit = False
    assert f not in f._select_for_exit()
    assert not l
    del o
    assert l.pop() == 1

    o = C()
    fin = finalize(o, cb, 2)
    assert fin.alive
    fin()
    assert not fin.alive
    assert l.pop() == 2
    del o
    assert not l

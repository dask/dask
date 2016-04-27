import io
import os

import numpy as np
import pytest

from dask.compatibility import BZ2File, GzipFile, LZMAFile, LZMA_AVAILABLE
from dask.utils import (textblock, filetext, takes_multiple_arguments,
                        Dispatch, tmpfile, different_seeds, file_size)


SKIP_XZ = pytest.mark.skipif(not LZMA_AVAILABLE, reason="no lzma library")
@pytest.mark.parametrize('myopen,compression',
                         [(open, None), (GzipFile, 'gzip'), (BZ2File, 'bz2'),
                          SKIP_XZ((LZMAFile, 'xz'))])
def test_textblock(myopen, compression):
    text = b'123 456 789 abc def ghi'.replace(b' ', os.linesep.encode())
    with filetext(text, open=myopen, mode='wb') as fn:
        text = ''.join(textblock(fn, 1, 11, compression)).encode()
        assert text == ('456 789 '.replace(' ', os.linesep)).encode()
        assert set(map(len, text.split())) == set([3])

        k = 3 + len(os.linesep)
        assert ''.join(textblock(fn, 0, k, compression)).encode() == ('123' + os.linesep).encode()
        assert ''.join(textblock(fn, k, k, compression)).encode() == b''


@pytest.mark.parametrize('myopen,compression',
                         [(open, None), (GzipFile, 'gzip'), (BZ2File, 'bz2'),
                          SKIP_XZ((LZMAFile, 'xz'))])
def test_filesize(myopen, compression):
    text = b'123 456 789 abc def ghi'.replace(b' ', os.linesep.encode())
    with filetext(text, open=myopen, mode='wb') as fn:
        assert file_size(fn, compression) == len(text)


def test_textblock_multibyte_linesep():
    text = b'12 34 56 78'.replace(b' ', b'\r\n')
    with filetext(text, mode='wb') as fn:
        text = [line.encode()
                for line in textblock(fn, 5, 13, linesep='\r\n', buffersize=2)]
        assert text == [line.encode() for line in ('56\r\n', '78')]


def test_takes_multiple_arguments():
    assert takes_multiple_arguments(map)
    assert not takes_multiple_arguments(sum)

    def multi(a, b, c):
        return a, b, c

    class Singular(object):
        def __init__(self, a):
            pass

    class Multi(object):
        def __init__(self, a, b):
            pass

    assert takes_multiple_arguments(multi)
    assert not takes_multiple_arguments(Singular)
    assert takes_multiple_arguments(Multi)


def test_dispatch():
    foo = Dispatch()
    foo.register(int, lambda a: a + 1)
    foo.register(float, lambda a: a - 1)
    foo.register(tuple, lambda a: tuple(foo(i) for i in a))
    foo.register(object, lambda a: a)

    class Bar(object):
        pass
    b = Bar()
    assert foo(1) == 2
    assert foo(1.0) == 0.0
    assert foo(b) == b
    assert foo((1, 2.0, b)) == (2, 1.0, b)


def test_gh606():
    encoding = 'utf-16-le'
    euro = u'\u20ac'
    yen = u'\u00a5'
    linesep = os.linesep

    bin_euro = u'\u20ac'.encode(encoding)
    bin_yen = u'\u00a5'.encode(encoding)
    bin_linesep = linesep.encode(encoding)

    data = (euro * 10) + linesep + (yen * 10) + linesep + (euro * 10)
    bin_data = data.encode(encoding)

    with tmpfile() as fn:
        with open(fn, 'wb') as f:
            f.write(bin_data)

        stop = len(bin_euro) * 10 + len(bin_linesep) + 1
        res = ''.join(textblock(fn, 1, stop, encoding=encoding)).encode(encoding)
        assert res == ((yen * 10) + linesep).encode(encoding)

        stop = len(bin_euro) * 10 + len(bin_linesep) + 1
        res = ''.join(textblock(fn, 0, stop, encoding=encoding)).encode(encoding)
        assert res == ((euro * 10) + linesep + (yen * 10) + linesep).encode(encoding)


def test_different_seeds():
    seed = 37
    state = np.random.RandomState(seed)
    n = 100000

    # Use an integer
    seeds = set(different_seeds(n, seed))
    assert len(seeds) == n

    # Use RandomState object
    seeds2 = set(different_seeds(n, state))
    assert seeds == seeds2

    # Should be sorted
    smallseeds = different_seeds(10, 1234)
    assert smallseeds == sorted(smallseeds)

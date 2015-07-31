from dask.utils import textblock
from dask.utils import filetext
from dask.utils import takes_multiple_arguments
import os

def test_textblock():
    text = b'123 456 789 abc def ghi'.replace(b' ', os.linesep.encode())
    with filetext(text, mode='wb') as fn:
        with open(fn, 'rb') as f:
            text = textblock(f, 1, 11)
            assert text == ('456 789 '.replace(' ', os.linesep)).encode()
            assert set(map(len, text.split())) == set([3])

            assert textblock(f, 1, 10) == textblock(fn, 1, 10)

            assert textblock(f, 0, 3) == ('123' + os.linesep).encode()
            assert textblock(f, 3 + len(os.linesep), 6) == ('456' + os.linesep).encode()


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

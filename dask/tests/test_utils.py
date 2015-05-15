from dask.utils import textblock
from dask.utils import filetext
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

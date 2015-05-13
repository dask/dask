from dask.utils import textblock
from dask.utils import filetext
import os

def test_textblock():
    text = '123 456 789 abc def ghi'.replace(' ', os.linesep)
    with filetext(text) as fn:
        with open(fn, 'rb') as f:
            text = textblock(f, 1, 10)
            assert text == '456 789 '.replace(' ', os.linesep)
            assert set(map(len, text.split())) == set([3])

            assert textblock(f, 1, 10) == textblock(fn, 1, 10)

            assert textblock(f, 0, 3) == '123' + os.linesep
            assert textblock(f, 3 + len(os.linesep), 6) == '456' + os.linesep

from dask.utils import textblock
from dask.utils import filetext

def test_textblock():
    with filetext('123\n456\n789\nabc\ndef\nghi') as fn:
        f = open(fn)
        text = textblock(f, 1, 10)
        assert text == '456\n789\n'
        assert set(map(len, text.split())) == set([3])

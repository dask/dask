from tempfile import TemporaryFile
from pbag.serialize import dump, load


data = [b'Hello\n', 1, b'world!', None]


def test_core():
    with TemporaryFile(mode='wb+') as f:
        dump(data, f)
        f.seek(0)
        data2 = load(f)

    assert data == data2


def test_multiple_dumps():
    with TemporaryFile(mode='wb+') as f:
        dump(1, f)
        dump(data, f)
        dump(2, f)

        f.seek(0)

        a = load(f)
        b = load(f)
        c = load(f)

    assert a == 1
    assert b == data
    assert c == 2



from into import into, chunks, TextFile
from into.utils import filetexts
from dask.bag.into import Bag, convert


def inc(x):
    return x + 1

dsk = {('x', 0): (range, 5),
       ('x', 1): (range, 5),
       ('x', 2): (range, 5)}

L = list(range(5)) * 3

b = Bag(dsk, 'x', 3)

def test_convert_bag_to_list():
    assert convert(list, b) == L

def test_convert_logfiles_to_bag():
    with filetexts({'a1.log': 'Hello\nWorld', 'a2.log': 'Hola\nMundo'}) as fns:
        logs = chunks(TextFile)(list(map(TextFile, fns)))
        b = convert(Bag, logs)
        assert isinstance(b, Bag)
        assert 'a1.log' in str(b.dask.values())
        assert convert(list, b) == convert(list, logs)


def test_sequence():
    b = into(Bag, [1, 2, 3])
    assert set(b.map(inc)) == set([2, 3, 4])

from dask.diagnostics.cache import cache
from cachey import Cache
from dask.threaded import get
from operator import add


flag = []

def inc(x):
    flag.append(x)
    return x + 1


def test_cache():
    c = Cache(10000)

    with cache(c):
        assert get({'x': (inc, 1)}, 'x') == 2

    assert flag == [1]
    assert c.data['x'] == 2

    while flag:
        flag.pop()
    dsk = {'x': (inc, 1), 'y': (inc, 2), 'z': (add, 'x', 'y')}
    with cache(c):
        assert get(dsk, 'z') == 5

    assert flag == [2]  # no x present

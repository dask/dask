from dask.diagnostics.cache import Cache
import cachey
from dask.threaded import get
from operator import add


flag = []

def inc(x):
    flag.append(x)
    return x + 1


def test_cache():
    c = cachey.Cache(10000)

    with Cache(c):
        assert get({'x': (inc, 1)}, 'x') == 2

    assert flag == [1]
    assert c.data['x'] == 2

    while flag:
        flag.pop()
    dsk = {'x': (inc, 1), 'y': (inc, 2), 'z': (add, 'x', 'y')}
    with Cache(c):
        assert get(dsk, 'z') == 5

    assert flag == [2]  # no x present


def test_cache_with_number():
    c = Cache(10000, limit=1)
    assert isinstance(c.cache, cachey.Cache)
    assert c.cache.available_bytes == 10000
    assert c.cache.limit == 1

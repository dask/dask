from dask.store import Store
from operator import add, mul
from dask.utils import raises


def inc(x):
    return x + 1


def test_basic():
    s = Store()
    s['x'] = 1
    s['y'] = (inc, 'x')
    s['z'] = (add, 'x', 'y')

    assert s.data == set(['x'])

    assert s['z'] == 3
    assert 'x' in s.data
    assert s.cache['z'] == 3
    assert s.cache['y'] == 2

    assert len(s.access_times['z']) == 1
    assert len(s.access_times['y']) == 1
    assert len(s.access_times['x']) == 2
    assert s.compute_time['z'] < 0.1

    cache = s.cache.copy()
    assert s['z'] == 3
    assert s.cache == cache
    assert len(s.access_times['z']) == 2
    assert len(s.access_times['y']) == 1
    assert len(s.access_times['x']) == 2

    assert s[5] == 5
    assert list(s[['x', 'y']]) == [s['x'], s['y']]

    def reassign():
        s['x'] = 2
    assert raises(Exception, reassign)


def test_update():
    s = Store()

    dsk = {'x': 1, 'y': (inc, 'x')}
    s.update(dsk)

    assert s['y'] == 2

    assert raises(Exception, lambda: s.update({'x': 2}))
    assert not raises(Exception, lambda: s.update({'x': 1}))

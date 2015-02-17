from dask.base import Base
from operator import add, mul
from dask.utils import raises


def inc(x):
    return x + 1


def test_basic():
    db = Base()
    db['x'] = 1
    db['y'] = (inc, 'x')
    db['z'] = (add, 'x', 'y')

    assert db.data == set(['x'])

    assert db['z'] == 3
    assert 'x' in db.data
    assert db.cache['z'] == 3
    assert db.cache['y'] == 2

    assert len(db.access_times['z']) == 1
    assert len(db.access_times['y']) == 1
    assert len(db.access_times['x']) == 2
    assert db.compute_time['z'] < 0.1

    cache = db.cache.copy()
    assert db['z'] == 3
    assert db.cache == cache
    assert len(db.access_times['z']) == 2
    assert len(db.access_times['y']) == 1
    assert len(db.access_times['x']) == 2

    assert db[5] == 5
    assert list(db[['x', 'y']]) == [db['x'], db['y']]

    def reassign():
        db['x'] = 2
    assert raises(Exception, reassign)

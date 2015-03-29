from dask.distributed import Worker

def inc(x):
    return x + 1

def add(x, y):
    return x + y

def test_get():
    warehouse = {'x': 1, 'y': 2, 'z': 3}  # local proxy for distributed store

    try:
        a = Worker('localhost', 5000, warehouse)
        b = Worker('localhost', 5001, warehouse)

        result = a.compute('foo', (add, (inc, 'x'), 'y'))
        assert result['status'] == 'OK'
        assert result['duration'] < 1
        assert warehouse['foo'] == 4

        result = b.compute('bar', (inc, 'foo'))
        assert result['status'] == 'OK'
        assert result['duration'] < 1
        assert warehouse['bar'] == 5

    finally:
        a.server.stop()
        b.server.stop()

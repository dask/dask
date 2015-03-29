from dask.distributed import Worker, get_distributed
from contextlib import contextmanager
from functools import partial


@contextmanager
def worker_farm(n, data=None):
    if data is None:
        data = dict()
    workers = []
    for i in range(n):
        worker = Worker('localhost', 5100 + i, data)
        workers.append(worker)

    try:
        yield {worker.server.url: worker for worker in workers}
    finally:
        for w in workers:
            w.server.stop()

def test_worker_farm():
    with worker_farm(3) as urls:
        assert all('tcp://localhost:5' in url for url in urls)
        assert len(urls) == 3


def inc(x):
    return x + 1

def add(x, y):
    return x + y


def test_get_distributed():
    dsk = {'a': 1, 'b': 2, 'x': (inc, 'a'),
            'y': (add, 'x', 'a'),
            'z': (add, 'x', 'b')}

    data = dict()
    with worker_farm(3, data) as urls:
        get = partial(get_distributed, list(urls), data)

        assert get(dsk, ['y', 'z']) == (3, 4)
        assert get(dsk, ['z', 'x']) == (4, 2)

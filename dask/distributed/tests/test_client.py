from dask.distributed import Worker, Scheduler, Client
from contextlib import contextmanager
from operator import add

def inc(x):
    return x + 1

@contextmanager
def scheduler_and_workers(n=2):
    s = Scheduler()
    workers = [Worker(s.address_to_workers) for i in range(n)]
    try:
        yield s, workers
    finally:
        s.close()
        for w in workers:
            w.close()


def test_get():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        dsk = {'x': 1, 'y': (add, 'x', 'x'), 'z': (inc, 'y')}
        keys = ['y', 'z']

        assert c.get(dsk, keys) == [2, 3]

def test_status():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        assert c.scheduler_status() == 'OK'

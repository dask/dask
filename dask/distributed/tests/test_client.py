from dask.distributed import Worker, Scheduler, Client
from contextlib import contextmanager
from operator import add
from time import sleep
from multiprocessing.pool import ThreadPool

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


def test_get_with_dill():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        dsk = {'x': 1, 'y': (lambda x: x + 1, 'x')}
        keys = 'y'

        assert c.get(dsk, keys) == 2


def test_multiple_clients():
    with scheduler_and_workers(1) as (s, (a,)):
        c = Client(s.address_to_clients)
        d = Client(s.address_to_clients)

        assert c.get({'x': (inc, 1)}, 'x') == d.get({'x': (inc, 1)}, 'x')

        def sleep_inc(x):
            sleep(0.5)
            return x + 1

        pool = ThreadPool(2)

        future1 = pool.apply_async(c.get,
                                   args=({'x': 1, 'y': (sleep_inc, 'x')}, 'y'))
        future2 = pool.apply_async(d.get,
                                   args=({'a': 1, 'b': (sleep_inc, 'a')}, 'b'))

        assert future1.get() == future2.get()

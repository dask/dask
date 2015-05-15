from dask.distributed import Worker, Scheduler, Client
from dask.utils import raises
from contextlib import contextmanager
from operator import add
from time import sleep
from multiprocessing.pool import ThreadPool
from toolz import partial
import pickle

def inc(x):
    return x + 1

@contextmanager
def scheduler_and_workers(n=2, **kwargs):
    s = Scheduler(**kwargs)
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
        c.close()


def test_status():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        assert c.scheduler_status() == 'OK'
        c.close()


def test_get_with_dill():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        dsk = {'x': 1, 'y': (partial(add, 1), 'x')}
        assert c.get(dsk, 'y') == 2
        c.close()


def test_error():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        assert raises(TypeError,
                lambda: c.get({'x': 1, 'y': (inc, 'x', 'x')}, 'y'))
        assert 'y' not in s.data
        c.close()


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
        c.close()
        d.close()


def test_register_collections():
    try:
        import dask.bag as db
    except ImportError:
        return
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        b = db.from_sequence(range(5), npartitions=2).map(inc)
        assert not s.collections
        c.set_collection('mybag', b)
        assert 'mybag' in s.collections

        d = Client(s.address_to_clients)
        b2 = d.get_collection('mybag')

        assert (type(b) == type(b2) and
                b.npartitions == b2.npartitions)
        assert list(b) == list(b2)

        c.close()
        d.close()


def test_cloudpickle():
    try:
        import cloudpickle
    except ImportError:
        return
    with scheduler_and_workers(loads=pickle.loads, dumps=cloudpickle.dumps) as (s, (a, b)):
        c = Client(s.address_to_clients, loads=pickle.loads,
                   dumps=cloudpickle.dumps)

        dsk = {'x': 1, 'y': (partial(add, 1), 'x')}
        assert c.get(dsk, 'y') == 2

        dsk = {'x': 1, 'y': (lambda x: x + 1, 'x')}
        assert c.get(dsk, 'y') == 2
        c.close()

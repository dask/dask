import os

import pytest
pytest.importorskip('zmq')
pytest.importorskip('dill')

from dask.distributed import Worker, Scheduler, Client
from dask.utils import raises
from contextlib import contextmanager
from operator import add
from time import sleep
from multiprocessing.pool import ThreadPool
from toolz import partial

def inc(x):
    return x + 1

@contextmanager
def scheduler_and_workers(n=2):
    s = Scheduler(hostname='127.0.0.1')
    workers = [Worker(s.address_to_workers, hostname='127.0.0.1') for i in range(n)]
    while len(s.workers) < n:
        sleep(1e-6)
    try:
        yield s, workers
    finally:
        for w in workers:
            w.close()
        s.close()


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
        keys = 'y'

        assert c.get(dsk, keys) == 2
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

        pool = ThreadPool(2)

        future1 = pool.apply_async(c.get,
                                   args=({'x': 1, 'y': (inc, 'x')}, 'y'))
        future2 = pool.apply_async(d.get,
                                   args=({'a': 1, 'b': (inc, 'a')}, 'b'))

        while not (future1.ready() and future2.ready()):
            sleep(1e-6)

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


def test_register_with_scheduler():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)
        pid = os.getpid()
        assert s.clients[c.address]['pid'] == os.getpid()

        assert set(c.registered_workers) == set([a.address, b.address])
        assert c.registered_workers[a.address]['pid'] == pid
        assert c.registered_workers[b.address]['pid'] == pid


def test_get_workers():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)
        pid = os.getpid()

        workers = c.get_registered_workers()
        assert set(workers) == set([a.address, b.address])
        assert workers[a.address]['pid'] == pid
        assert workers[b.address]['pid'] == pid

        s.close_workers()
        assert c.get_registered_workers() == {}


def test_keep_results():
    with scheduler_and_workers() as (s, (a, b)):
        c = Client(s.address_to_clients)

        assert c.get({'x': (inc, 1)}, 'x', keep_results=True) == 2

        assert 'x' in a.data or 'x' in b.data

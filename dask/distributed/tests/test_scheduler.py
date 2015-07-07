import pytest
pytest.importorskip('zmq')
pytest.importorskip('dill')

from dask.distributed.scheduler import Scheduler
from dask.distributed.worker import Worker
import multiprocessing
import itertools
from datetime import datetime
from contextlib import contextmanager
from toolz import take
from time import sleep
import dill
import pickle
import re

import zmq

context = zmq.Context()


@contextmanager
def scheduler():
    s = Scheduler()
    try:
        yield s
    finally:
        s.close()


def test_status_worker():
    with scheduler() as s:
        sock = context.socket(zmq.DEALER)
        try:
            sock.setsockopt(zmq.IDENTITY, b'worker1')
            sock.connect(s.address_to_workers)

            header = {'address': b'worker1', 'jobid': 1, 'function': 'status'}
            payload = {'function': 'status'}
            sock.send_multipart([pickle.dumps(header), pickle.dumps(payload)])

            header2, payload2 = sock.recv_multipart()
            header2 = pickle.loads(header2)
            assert header2['address'] == s.address_to_workers
            assert header2['jobid'] == header.get('jobid')
            assert isinstance(header2['timestamp'], (datetime, str))
            assert pickle.loads(payload2) == 'OK'
        finally:
            sock.close(1)


def test_status_client():
    with scheduler() as s:
        sock = context.socket(zmq.DEALER)
        try:
            sock.setsockopt(zmq.IDENTITY, b'client-1')
            sock.connect(s.address_to_clients)

            header = {'address': b'client-1', 'jobid': 2, 'function': 'status'}
            payload = {'function': 'status'}
            sock.send_multipart([pickle.dumps(header), pickle.dumps(payload)])

            header2, payload2 = sock.recv_multipart()
            header2 = pickle.loads(header2)
            assert header2['address'] == s.address_to_clients
            assert header2['jobid'] == header.get('jobid')
            assert isinstance(header2['timestamp'], (datetime, str))
            assert pickle.loads(payload2) == 'OK'
        finally:
            sock.close(1)


@contextmanager
def scheduler_and_workers(n=2, heartbeat=5):
    with scheduler() as s:
        workers = [Worker(s.address_to_workers, heartbeat=heartbeat) for i in range(n)]

        # wait for workers to register
        while(len(s.workers) < n):
            sleep(0.01)
        # wait until scheduler sees all heartbeats
        while not all('last-seen' in s.workers[workers[i].address] for i in range(n)):
            sleep(0.01)
        try:
            yield s, workers
        finally:
            for w in workers:
                w.close()


def test_cluster():
    with scheduler_and_workers() as (s, (a, b)):
        assert a.address in s.workers
        assert b.address in s.workers
        assert a.scheduler == s.address_to_workers
        assert b.scheduler == s.address_to_workers


def test_pid():
    with scheduler_and_workers() as (s, (a, b)):
        assert isinstance(s.workers[a.address]['pid'], int)
        assert isinstance(s.workers[b.address]['pid'], int)


def inc(x):
    return x + 1

def add(x, y):
    return x + y


def test_compute_cycle():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        assert s.available_workers.qsize() == 2

        dsk = {'a': (add, 1, 2), 'b': (inc, 'a')}
        s.trigger_task(dsk, 'a', 'queue-key')
        sleep(0.1)

        assert 'a' in s.who_has
        assert 'a' in a.data or 'a' in b.data
        assert a.data.get('a') == 3 or b.data.get('a') == 3
        assert a.address in s.worker_has or b.address in s.worker_has
        assert s.available_workers.qsize() == 2

        s.trigger_task(dsk, 'b', 'queue-key')
        sleep(0.1)

        assert 'b' in s.who_has
        assert 'b' in a.data or 'b' in b.data
        assert a.data.get('b') == 4 or b.data.get('b') == 4
        assert s.available_workers.qsize() == 2


def test_send_release_data():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        s.send_data('x', 1, a.address)
        assert a.data['x'] == 1
        assert a.address in s.who_has['x']
        assert 'x' in s.worker_has[a.address]

        s.release_key('x')
        sleep(0.05)
        assert 'x' not in a.data
        assert a.address not in s.who_has['x']
        assert 'x' not in s.worker_has[a.address]

def test_scatter():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        data = {'x': 1, 'y': 2, 'z': 3}
        sleep(0.05)  # make sure all workers come in before scatter
        s.scatter(data)

        assert all(k in a.data or k in b.data for k in data)
        assert set([len(a.data), len(b.data)]) == set([1, 2])  # fair

def test_schedule():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        dsk = {'x': (add, 1, 2), 'y': (inc, 'x'), 'z': (add, 'y', 'x')}

        result = s.schedule(dsk, ['y'])
        assert result == [4]

        result = s.schedule(dsk, [['z'], 'y'])
        assert result == [[7], 4]

        # No worker still has the unnecessary intermediate variable
        assert not s.who_has['x']

        # Neither worker has the result after computation
        # We don't have a way to block on worker completion here
        # Instead we poll and sleep.  This is a bit of a hack
        for i in range(10):
            if a.data or b.data:
                sleep(0.1)
            else:
                break
        assert not a.data and not b.data


def test_gather():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        s.send_data('x', 1, a.address)
        s.send_data('y', 2, b.address)

        sleep(0.05)
        result = s.gather(['x', 'y'])
        assert result == [1, 2]


def test_random_names():
    s = Scheduler()

    try:
        assert s.address_to_workers
        assert s.address_to_clients
        assert s.address_to_clients != s.address_to_workers
        assert re.match('\w+://.+:\d+', s.address_to_workers.decode('utf-8'))
    finally:
        s.close()


def test_close_workers():
    with scheduler_and_workers(n=2) as (s, (a, b)):
        assert a.status != 'closed'

        s.close_workers()
        assert not s.workers
        for i in range(100):
            if a.status == 'closed' and b.status == 'closed':
                break
            else:
                sleep(0.01)
        assert a.status == 'closed'
        assert b.status == 'closed'


def test_heartbeats():
    with scheduler_and_workers(n=1) as (s, (a,)):
        for i in range(100):
            if 'last-seen' in s.workers[a.address]:
                break
            else:
                sleep(0.01)
        last_seen = s.workers[a.address]['last-seen']
        now = datetime.utcnow()
        assert abs(last_seen - now).seconds < 0.1


def test_close_scheduler():
    s = Scheduler()
    s.close()
    assert s.pool._state == multiprocessing.pool.CLOSE
    assert not s._listen_to_clients_thread.is_alive()
    assert not s._listen_to_workers_thread.is_alive()
    assert s.context.closed


def test_prune_workers():
    """
    We close a worker, then make sure prune workers notices this and removes
    it from the scheduler. This is "correcting the schedulers' state".
    """
    with scheduler_and_workers(heartbeat=0.1) as (s, (w1, w2)):
        assert w1.address in s.workers
        assert w2.address in s.workers

        w2.close()
        sleep(0.2)
        assert w2.address in s.prune_workers(timeout=0.2)
        assert w1.address in s.workers
        assert w2.address not in s.workers


def test_prune_and_notify():
    with scheduler_and_workers(heartbeat=0.1) as (s, (w1, w2)):
        assert w1.address in s.workers
        assert w2.address in s.workers

        # Oh no! A worker died!
        w2.close()
        sleep(0.1)  # sleep to make sure worker is closed

        # But the living worker gets into a bad state by trying to collect data
        # from the dead worker! Double oh no!
        def bad_collect():
            # This won't work and should be interrupted.
            w1.collect({'x': [w2.address]})

        result = w1.pool.apply_async(w1.collect, args=({'x': [w2.address]},))
        sleep(0.1)  # some sleeping to give show the function is hanging
        assert result.ready() is False

        # The scheduler notices, and corrects it state.
        s.prune_and_notify(timeout=0.2)
        assert w1.address in s.workers
        assert w2.address not in s.workers

        # But the sheduler notified the workers about the death
        sleep(0.1)
        assert result.ready() is True

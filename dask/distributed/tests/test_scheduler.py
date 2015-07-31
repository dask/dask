import pytest
pytest.importorskip('zmq')
pytest.importorskip('dill')

import multiprocessing
import pickle
import re
from datetime import datetime
from contextlib import contextmanager
from time import sleep

import zmq
import dill

from dask.distributed.scheduler import Scheduler
from dask.distributed.worker import Worker

context = zmq.Context()


@contextmanager
def scheduler(kwargs={}):
    s = Scheduler(hostname='127.0.0.1', **kwargs)
    try:
        yield s
    finally:
        s.close()


@contextmanager
def scheduler_and_workers(n=2, scheduler_kwargs={}, worker_kwargs={}):
    with scheduler(scheduler_kwargs) as s:
        workers = [Worker(s.address_to_workers, hostname='127.0.0.1', nthreads=50, **worker_kwargs) for i in range(n)]

        # wait for workers to register
        while(len(s.workers) < n):
            sleep(1e-6)
        try:
            yield s, workers
        finally:
            for w in workers:
                w.close()


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
    with scheduler_and_workers() as (s, (a, b)):
        assert s.available_workers.qsize() == 2

        dsk = {'a': (add, 1, 2), 'b': (inc, 'a')}
        s.trigger_task('a', dsk['a'], set([]), 'queue-key')
        sleep(0.1)

        assert 'a' in s.who_has
        assert 'a' in a.data or 'a' in b.data
        assert a.data.get('a') == 3 or b.data.get('a') == 3
        assert a.address in s.worker_has or b.address in s.worker_has
        assert s.available_workers.qsize() == 2

        s.trigger_task('b', dsk['b'], set(['a']), 'queue-key')
        sleep(0.1)

        assert 'b' in s.who_has
        assert 'b' in a.data or 'b' in b.data
        assert a.data.get('b') == 4 or b.data.get('b') == 4
        assert s.available_workers.qsize() == 2


def test_send_release_data():
    with scheduler_and_workers() as (s, (a, b)):
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
    with scheduler_and_workers() as (s, (a, b)):
        data = {'x': 1, 'y': 2, 'z': 3}
        sleep(0.05)  # make sure all workers come in before scatter
        s.scatter(data)

        assert all(k in a.data or k in b.data for k in data)
        assert set([len(a.data), len(b.data)]) == set([1, 2])  # fair


def test_schedule():
    with scheduler_and_workers() as (s, (a, b)):
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
    with scheduler_and_workers() as (s, (a, b)):
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
    with scheduler_and_workers() as (s, (a, b)):
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


def test_prune_and_notify():
    with scheduler_and_workers(worker_kwargs={'heartbeat': 0.001}) as (s, (w1, w2)):
        # Oh no! A worker died!
        w2.close()
        while w2.status != 'closed':
            sleep(1e-6)

        # worker 1 tries to collect data from the dead worker.
        result = w1.pool.apply_async(w1.collect, args=({'x': [w2.address]},))
        sleep(0.01)  # sleep to show w1.collect hangs
        assert result.ready() is False

        # The scheduler notices, and corrects it state.
        while w2.address in s.workers:
            s.prune_and_notify(timeout=0.1)

        # But the sheduler notified the workers about the death
        while not result.ready():
            sleep(1e-6)
        assert result.ready() is True


def test_workers_reregister():
    with scheduler_and_workers(worker_kwargs={'heartbeat': 0.001}) as (s, (w1, w2)):
        assert w1.address in s.workers

        assert s.workers.pop(w1.address)
        assert w1.address not in s.workers  # removed

        while w1.address not in s.workers:
            sleep(1e-6)
        assert w1.address in s.workers  # but now its back


def test_collect_retry():
    """
    This tests that when a worker tries to "collect" data from a dead worker,
    it will try other available workers.
    """
    with scheduler_and_workers(n=3, worker_kwargs={'heartbeat': 0.001}) as (s, (w1, w2, w3)):
        w3.data['x'] = 42
        w2.close()
        while w2.status != 'closed':  # make sure closed
            sleep(1e-6)
        result = w1.pool.apply_async(w1.collect,
                                     args=({'x': [w2.address, w3.address]},))
        # sometimes the above^ call to w1.collect will get called *after*,
        # prune_and_notify below. Causing a hang. I'm not sure what to do
        # about this, so here is a sleep.
        sleep(0.1)
        while w2.address in s.workers:
            s.prune_and_notify(timeout=0.1)

        # make sure prune_and_notify didn't remove these
        assert w1.address in s.workers
        assert w3.address in s.workers

        while not result.ready():  # make sure collect finishes
            sleep(1e-6)
        assert result.ready() == True
        assert 'x' in w1.data
        assert w1.data['x'] == 42


def test_monitor_workers():
    with scheduler_and_workers(scheduler_kwargs={'worker_timeout': 0.01},
                               worker_kwargs={'heartbeat': 0.001}) as (s, (w1, w2)):
        w2.close()
        while w2.status != 'closed':  # wait to close
            sleep(1e-6)
        while w2.address in s.workers:  # wait to be removed
            sleep(1e-6)
        assert w2.address not in s.workers


def test_scheduler_reuses_worker_state():
    with scheduler_and_workers() as (s, (a, b)):
        assert s.schedule({'x': (inc, 1)}, 'x') == 2

        s.send_data('x', 10, address=a.address)  # send x to a worker
        assert a.data['x'] == 10
        assert s.schedule({'x': (inc, 1)}, 'x') == 10  # recall, don't compute
        sleep(0.1)
        assert a.data['x'] == 10
        assert s.schedule({'x': (inc, 1), 'y': (lambda x: x, 'x')}, 'y') == 10

def test_scheduler_keeps_preexisting_data():
    with scheduler_and_workers() as (s, (a, b)):
        s.send_data('x', 10, address=a.address)
        dsk = {'x': (inc, 1), 'y': (lambda x: x, 'x')}
        assert s.schedule(dsk, 'y') == 10
        sleep(0.01)
        assert a.data['x'] == 10


def test_keep_results():
    with scheduler_and_workers() as (s, (a, b)):
        assert s.schedule({'x': (inc, 1)}, 'x', keep_results=True) == 2
        sleep(0.1)
        assert a.data.get('x') == 2 or b.data.get('x') == 2


def test_cull_redundant_data():
    with scheduler_and_workers() as (s, (a, b)):
        s.send_data('x', 10, address=a.address)  # send x to a worker
        s.send_data('x', 10, address=b.address)  # send x to a worker

        s.cull_redundant_data(1)

        while 'x' in a.data and 'x' in b.data:
            sleep(0.01)

        assert ('x' in a.data and 'x' not in b.data or
                'x' in b.data and 'x' not in a.data)

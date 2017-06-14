from __future__ import print_function, division, absolute_import

from operator import add, sub
from time import sleep

import pytest
pytest.importorskip('bokeh')
import sys
from toolz import first
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from distributed.client import _wait
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, dec, slowinc
from distributed.bokeh.worker import Counters, BokehWorker
from distributed.bokeh.scheduler import (BokehScheduler, StateTable,
        SystemMonitor, Occupancy, StealingTimeSeries, StealingEvents, Events,
        TaskStream, TaskProgress, MemoryUse, CurrentLoad, ProcessingHistogram,
        NBytesHistogram, WorkerTable)

from distributed.bokeh import scheduler

scheduler.PROFILING = False


@pytest.mark.skipif(sys.version_info[0] == 2,
                    reason='https://github.com/bokeh/bokeh/issues/5494')
@gen_cluster(client=True,
             scheduler_kwargs={'services': {('bokeh', 0):  BokehScheduler}})
def test_simple(c, s, a, b):
    assert isinstance(s.services['bokeh'], BokehScheduler)

    future = c.submit(sleep, 1)
    yield gen.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in ['system', 'counters', 'workers', 'status', 'tasks', 'stealing']:
        response = yield http_client.fetch('http://localhost:%d/%s'
                                           % (s.services['bokeh'].port, suffix))
        assert 'bokeh' in response.body.decode().lower()


@gen_cluster(client=True, worker_kwargs=dict(services={'bokeh': BokehWorker}))
def test_basic(c, s, a, b):
    for component in [SystemMonitor, StateTable, Occupancy, StealingTimeSeries]:
        ss = component(s)

        ss.update()
        data = ss.source.data
        assert len(first(data.values()))
        if component is Occupancy:
            assert all(addr.startswith('127.0.0.1:')
                       for addr in data['bokeh_address'])


@gen_cluster(client=True)
def test_counters(c, s, a, b):
    pytest.importorskip('crick')
    while 'tick-duration' not in s.digests:
        yield gen.sleep(0.01)
    ss = Counters(s)

    ss.update()
    yield gen.sleep(0.1)
    ss.update()

    start = time()
    while not len(ss.digest_sources['tick-duration'][0].data['x']):
        yield gen.sleep(1)
        assert time() < start + 5


@gen_cluster(client=True)
def test_stealing_events(c, s, a, b):
    se = StealingEvents(s)

    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address,
                    allow_other_workers=True)

    while not b.task_state:  # will steal soon
        yield gen.sleep(0.01)

    se.update()

    assert len(first(se.source.data.values()))


@gen_cluster(client=True)
def test_events(c, s, a, b):
    e = Events(s, 'all')

    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address,
                    allow_other_workers=True)

    while not b.task_state:
        yield gen.sleep(0.01)

    e.update()
    d = dict(e.source.data)
    assert sum(a == 'add-worker' for a in d['action']) == 2


@gen_cluster(client=True)
def test_task_stream(c, s, a, b):
    ts = TaskStream(s)

    futures = c.map(slowinc, range(10), delay=0.001)

    yield _wait(futures)

    ts.update()
    d = dict(ts.source.data)

    assert all(len(L) == 10 for L in d.values())
    assert min(d['start']) == 0  # zero based

    ts.update()
    d = dict(ts.source.data)
    assert all(len(L) == 10 for L in d.values())

    total = c.submit(sum, futures)
    yield _wait(total)

    ts.update()
    d = dict(ts.source.data)
    assert len(set(map(len, d.values()))) == 1


@gen_cluster(client=True)
def test_task_stream_n_rectangles(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10)
    futures = c.map(slowinc, range(10), delay=0.001)
    yield _wait(futures)
    ts.update()

    assert len(ts.source.data['start']) == 10


@gen_cluster(client=True)
def test_task_stream_second_plugin(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10, clear_interval=10)
    ts.update()
    futures = c.map(inc, range(10))
    yield _wait(futures)
    ts.update()

    ts2 = TaskStream(s, n_rectangles=5, clear_interval=10)
    ts2.update()



@gen_cluster(client=True)
def test_task_stream_clear_interval(c, s, a, b):
    ts = TaskStream(s, clear_interval=200)

    yield _wait(c.map(inc, range(10)))
    ts.update()
    yield gen.sleep(0.010)
    yield _wait(c.map(dec, range(10)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data['name'].count('inc') == 10
    assert ts.source.data['name'].count('dec') == 10

    yield gen.sleep(0.300)
    yield _wait(c.map(inc, range(10, 20)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data['name'].count('inc') == 10
    assert ts.source.data['name'].count('dec') == 0


@gen_cluster(client=True)
def test_TaskProgress(c, s, a, b):
    tp = TaskProgress(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    yield _wait(futures)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 1 for L in d.values())
    assert d['name'] == ['slowinc']

    futures2 = c.map(dec, range(5))
    yield _wait(futures2)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 2 for L in d.values())
    assert d['name'] == ['slowinc', 'dec']

    del futures, futures2

    while s.task_state:
        yield gen.sleep(0.01)

    tp.update()
    assert not tp.source.data['all']


@gen_cluster(client=True)
def test_TaskProgress_empty(c, s, a, b):
    tp = TaskProgress(s)
    tp.update()

    futures = [c.submit(inc, i, key='f-' + 'a' * i) for i in range(20)]
    yield _wait(futures)
    tp.update()

    del futures
    while s.tasks:
        yield gen.sleep(0.01)
    tp.update()

    assert not any(len(v) for v in tp.source.data.values())


@gen_cluster(client=True)
def test_MemoryUse(c, s, a, b):
    mu = MemoryUse(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    yield _wait(futures)

    mu.update()
    d = dict(mu.source.data)
    assert all(len(L) == 1 for L in d.values())
    assert d['name'] == ['slowinc']


@gen_cluster(client=True)
def test_CurrentLoad(c, s, a, b):
    cl = CurrentLoad(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    yield _wait(futures)

    cl.update()
    d = dict(cl.source.data)

    assert all(len(L) == 2 for L in d.values())
    assert all(d['nbytes'])


@gen_cluster(client=True)
def test_ProcessingHistogram(c, s, a, b):
    ph = ProcessingHistogram(s)
    ph.update()
    assert (ph.source.data['top'] != 0).sum() == 1

    futures = c.map(slowinc, range(10), delay=0.050)
    yield gen.sleep(0.100)

    ph.update()
    assert ph.source.data['right'][-1] > 2


@gen_cluster(client=True)
def test_NBytesHistogram(c, s, a, b):
    nh = NBytesHistogram(s)
    nh.update()
    assert (nh.source.data['top'] != 0).sum() == 1

    futures = c.map(inc, range(10))
    yield _wait(futures)

    nh.update()
    assert nh.source.data['right'][-1] > 5 * 20


@gen_cluster(client=True)
def test_WorkerTable(c, s, a, b):
    wt = WorkerTable(s)
    wt.update()
    assert all(wt.source.data.values())
    assert all(len(v) == 1 for v in wt.source.data.values())

from __future__ import print_function, division, absolute_import

from operator import add, sub
from time import sleep

import pytest
import sys
from toolz import first
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from distributed.client import _wait
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, dec, slowinc
from distributed.bokeh.worker import Counters
from distributed.bokeh.scheduler import (BokehScheduler, StateTable,
        SystemMonitor, Occupancy, StealingTimeSeries, StealingEvents)


@pytest.mark.skipif(sys.version_info[0] == 2,
                    reason='https://github.com/bokeh/bokeh/issues/5494')
@gen_cluster(client=True,
             scheduler_kwargs={'services': {('bokeh', 0):  BokehScheduler}})
def test_simple(c, s, a, b):
    assert isinstance(s.services['bokeh'], BokehScheduler)

    future = c.submit(sleep, 1)
    yield gen.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in ['system', 'counters', 'workers']:
        response = yield http_client.fetch('http://localhost:%d/%s'
                                           % (s.services['bokeh'].port, suffix))
        assert 'bokeh' in response.body.decode().lower()


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    for component in [SystemMonitor, StateTable, Occupancy, StealingTimeSeries]:
        ss = component(s)

        ss.update()
        assert len(first(ss.source.data.values()))


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

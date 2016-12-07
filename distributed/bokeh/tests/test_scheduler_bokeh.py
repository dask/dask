from __future__ import print_function, division, absolute_import

from operator import add, sub
from time import sleep

import pytest
import sys
from toolz import first
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from distributed.client import _wait
from distributed.utils_test import gen_cluster, inc, dec
from distributed.bokeh.scheduler import (BokehScheduler, StateTable,
        SystemMonitor, Occupancy)


@pytest.mark.skipif(sys.version_info[0] == 2,
                    reason='https://github.com/bokeh/bokeh/issues/5494')
@gen_cluster(client=True,
             scheduler_kwargs={'services': {('bokeh', 0):  BokehScheduler}})
def test_simple(c, s, a, b):
    assert isinstance(s.services['bokeh'], BokehScheduler)

    future = c.submit(sleep, 1)
    yield gen.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in ['system']:
        response = yield http_client.fetch('http://localhost:%d/%s'
                                           % (s.services['bokeh'].port, suffix))
        assert 'bokeh' in response.body.decode().lower()


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    for component in [SystemMonitor, StateTable, Occupancy]:
        ss = component(s)

        ss.update()
        assert len(first(ss.source.data.values()))

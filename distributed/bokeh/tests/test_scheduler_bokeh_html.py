from __future__ import print_function, division, absolute_import

import json
import xml.etree.ElementTree

import pytest
pytest.importorskip('bokeh')

from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient

from distributed.utils_test import gen_cluster, inc, slowinc
from distributed.bokeh.scheduler import BokehScheduler


@gen_cluster(client=True,
             scheduler_kwargs={'services': {('bokeh', 0):  BokehScheduler}})
def test_connect(c, s, a, b):
    future = c.submit(inc, 1)
    x = c.submit(slowinc, 1, delay=1)
    yield future
    http_client = AsyncHTTPClient()
    for suffix in ['info/workers.html',
                   'info/worker/' + url_escape(a.address) + '.html',
                   'info/task/' + url_escape(future.key) + '.html',
                   'info/logs.html',
                   'info/logs/' + url_escape(a.address) + '.html',
                   'info/call-stack/' + url_escape(x.key) + '.html',
                   'info/call-stacks/' + url_escape(a.address) + '.html',
                   'json/counts.json',
                   'json/identity.json',
                   'json/index.html',
                   ]:
        response = yield http_client.fetch('http://localhost:%d/%s'
                                           % (s.services['bokeh'].port, suffix))
        assert response.code == 200
        body = response.body.decode()
        if suffix.endswith('.json'):
            json.loads(body)
        else:
            assert xml.etree.ElementTree.fromstring(body) is not None

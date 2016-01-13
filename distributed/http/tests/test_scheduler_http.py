import json

from tornado.ioloop import IOLoop
from tornado import web
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from distributed import Scheduler
from distributed.utils_test import gen_cluster, gen_test
from distributed.http.scheduler import HTTPScheduler


@gen_cluster()
def test_simple(s, a, b):
    server = HTTPScheduler(s)
    server.listen(0)
    client = AsyncHTTPClient()

    response = yield client.fetch('http://localhost:%d/info.json' % server.port)
    response = json.loads(response.body.decode())
    ncores = {tuple(k): v for k, v in response['ncores']}
    assert ncores == s.ncores
    assert response['status'] == a.status


@gen_test()
def test_services():
    s = Scheduler(services={'http': HTTPScheduler})
    assert isinstance(s.services['http'], HTTPServer)
    assert s.services['http'].port

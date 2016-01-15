import json

from tornado.ioloop import IOLoop
from tornado import web
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from distributed.utils_test import gen_cluster, gen_test
from distributed import Worker
from distributed.http.worker import HTTPWorker


@gen_cluster()
def test_simple(s, a, b):
    port = 9898
    server = HTTPWorker(a)
    server.listen(port)
    client = AsyncHTTPClient()

    response = yield client.fetch('http://localhost:%d/info.json' % port)
    response = json.loads(response.body.decode())
    assert response['ncores'] == a.ncores
    assert response['status'] == a.status

    response = yield client.fetch('http://localhost:%d/resources.json' % port)
    response = json.loads(response.body.decode())
    assert 0 < response['memory_percent'] < 100


@gen_cluster()
def test_services(s, a, b):
    c = Worker(s.ip, s.port, ncores=1, ip='127.0.0.1',
               services={'http': HTTPWorker})
    yield c._start()
    assert isinstance(c.services['http'], HTTPServer)
    assert c.service_ports['http'] == c.services['http'].port
    assert s.worker_services[c.address]['http'] == c.service_ports['http']

from __future__ import print_function, division, absolute_import

import json
import tornado

from tornado.ioloop import IOLoop
from tornado import web
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from distributed.utils_test import gen_cluster, gen_test
from distributed import Worker
from distributed.http.worker import HTTPWorker
from distributed.sizeof import sizeof


@gen_cluster()
def test_simple(s, a, b):
    server = HTTPWorker(a)
    server.listen(0)
    client = AsyncHTTPClient()

    response = yield client.fetch('http://localhost:%d/info.json' % server.port)
    response = json.loads(response.body.decode())
    assert response['ncores'] == a.ncores
    assert response['status'] == a.status

    response = yield client.fetch('http://localhost:%d/resources.json' %
            server.port)
    response = json.loads(response.body.decode())

    a.data['x'] = 1

    try:
        import psutil
        assert 0 < response['memory_percent'] < 100
    except ImportError:
        assert response == {}

    endpoints = ['/files.json', '/processing.json', '/nbytes.json',
                 '/nbytes-summary.json']
    for endpoint in endpoints:
        response = yield client.fetch(('http://localhost:%d' % server.port)
                                      + endpoint)
        response = json.loads(response.body.decode())
        assert response

    server.stop()


@gen_cluster()
def test_services(s, a, b):
    c = Worker(s.ip, s.port, ncores=1, ip='127.0.0.1',
               services={'http': HTTPWorker})
    yield c._start()
    assert isinstance(c.services['http'], HTTPServer)
    assert c.service_ports['http'] == c.services['http'].port
    assert s.worker_info[c.address]['services']['http'] == c.service_ports['http']

    yield c._close()


@gen_cluster()
def test_services_port(s, a, b):
    c = Worker(s.ip, s.port, ncores=1, ip='127.0.0.1',
               services={('http', 9898): HTTPWorker})
    yield c._start()
    assert isinstance(c.services['http'], HTTPServer)
    assert (c.service_ports['http']
         == c.services['http'].port
         == s.worker_info[c.address]['services']['http']
         == 9898)

    c.services['http'].stop()
    yield c._close()


@gen_cluster()
def test_nbytes(s, a, b):
    server = HTTPWorker(a)
    server.listen(0)
    client = AsyncHTTPClient()

    a.data['x-1'] = 1
    a.data['x-2'] = 1

    nbytes = yield client.fetch('http://localhost:%d/nbytes.json' % server.port)
    nbytes = json.loads(nbytes.body.decode())
    summary = yield client.fetch('http://localhost:%d/nbytes-summary.json' % server.port)
    summary = json.loads(summary.body.decode())

    assert nbytes == {'x-1': sizeof(1), 'x-2': sizeof(2)}
    assert summary == {'x': sizeof(1) * 2}

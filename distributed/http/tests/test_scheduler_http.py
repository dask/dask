import json

from tornado.ioloop import IOLoop
from tornado import web
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from distributed import Scheduler
from distributed.utils_test import gen_cluster, gen_test
from distributed.http.scheduler import HTTPScheduler
from distributed.http.worker import HTTPWorker


@gen_cluster()
def test_simple(s, a, b):
    server = HTTPScheduler(s)
    server.listen(0)
    client = AsyncHTTPClient()


    response = yield client.fetch('http://localhost:%d/info.json' % server.port)
    response = json.loads(response.body.decode())
    assert response['ncores'] == {'%s:%d' % k: v for k, v in s.ncores.items()}
    assert response['status'] == a.status

@gen_cluster()
def test_processing(s, a, b):
    server = HTTPScheduler(s)
    server.listen(0)
    client = AsyncHTTPClient()

    s.processing[a.address].add(('foo-1', 1))

    response = yield client.fetch('http://localhost:%d/processing.json' % server.port)
    response = json.loads(response.body.decode())
    assert response == {'%s:%d' % a.address: ['foo'], '%s:%d' % b.address: []}


@gen_cluster()
def test_proxy(s, a, b):
    server = HTTPScheduler(s)
    server.listen(0)
    worker = HTTPWorker(a)
    worker.listen(0)
    client = AsyncHTTPClient()

    c_response = yield client.fetch('http://localhost:%d/info.json' % worker.port)
    s_response = yield client.fetch('http://localhost:%d/proxy/%s:%d/info.json'
                                    % (server.port, a.ip, worker.port))
    assert s_response.body.decode() == c_response.body.decode()


@gen_cluster()
def test_broadcast(s, a, b):
    ss = HTTPScheduler(s)
    ss.listen(0)
    s.services['http'] = ss

    aa = HTTPWorker(a)
    aa.listen(0)
    a.services['http'] = aa
    a.service_ports['http'] = aa.port
    s.worker_services[a.address]['http'] = aa.port

    bb = HTTPWorker(b)
    bb.listen(0)
    b.services['http'] = bb
    b.service_ports['http'] = bb.port
    s.worker_services[b.address]['http'] = bb.port

    client = AsyncHTTPClient()

    a_response = yield client.fetch('http://localhost:%d/info.json' % aa.port)
    b_response = yield client.fetch('http://localhost:%d/info.json' % bb.port)
    s_response = yield client.fetch('http://localhost:%d/broadcast/info.json'
                                    % ss.port)
    assert (json.loads(s_response.body.decode()) ==
            {'%s:%d' % a.address: json.loads(a_response.body.decode()),
             '%s:%d' % b.address: json.loads(b_response.body.decode())})


@gen_test()
def test_services():
    s = Scheduler(services={'http': HTTPScheduler})
    assert isinstance(s.services['http'], HTTPServer)
    assert s.services['http'].port

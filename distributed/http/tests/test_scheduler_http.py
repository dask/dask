from __future__ import print_function, division, absolute_import

import json
import sys

from tornado.ioloop import IOLoop
from tornado import web, gen
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from distributed import Scheduler, Client
from distributed.client import _wait
from distributed.sizeof import getsizeof
from distributed.utils_test import gen_cluster, gen_test, inc, div
from distributed.http.scheduler import HTTPScheduler
from distributed.http.worker import HTTPWorker


@gen_cluster()
def test_simple(s, a, b):
    server = HTTPScheduler(s)
    server.listen(0)
    client = AsyncHTTPClient()

    response = yield client.fetch('http://localhost:%d/info.json' % server.port)
    response = json.loads(response.body.decode())
    assert response == {'ncores': s.ncores,
                        'status': s.status}

    server.stop()


@gen_cluster()
def test_processing(s, a, b):
    server = HTTPScheduler(s)
    server.listen(0)
    client = AsyncHTTPClient()

    s.processing[a.address][('foo-1', 1)] = 1

    response = yield client.fetch('http://localhost:%d/processing.json' % server.port)
    response = json.loads(response.body.decode())
    assert response == {a.address: ['foo'], b.address: []}

    server.stop()


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
    server.stop()
    worker.stop()


@gen_cluster()
def test_broadcast(s, a, b):
    ss = HTTPScheduler(s)
    ss.listen(0)
    s.services['http'] = ss

    aa = HTTPWorker(a)
    aa.listen(0)
    a.services['http'] = aa
    a.service_ports['http'] = aa.port
    s.worker_info[a.address]['services']['http'] = aa.port

    bb = HTTPWorker(b)
    bb.listen(0)
    b.services['http'] = bb
    b.service_ports['http'] = bb.port
    s.worker_info[b.address]['services']['http'] = bb.port

    client = AsyncHTTPClient()

    a_response = yield client.fetch('http://localhost:%d/info.json' % aa.port)
    b_response = yield client.fetch('http://localhost:%d/info.json' % bb.port)
    s_response = yield client.fetch('http://localhost:%d/broadcast/info.json'
                                    % ss.port)
    assert (json.loads(s_response.body.decode()) ==
            {a.address: json.loads(a_response.body.decode()),
             b.address: json.loads(b_response.body.decode())})

    ss.stop()
    aa.stop()
    bb.stop()


@gen_test()
def test_services():
    s = Scheduler(services={'http': HTTPScheduler})
    s.start()
    try:
        assert isinstance(s.services['http'], HTTPServer)
        assert s.services['http'].port > 0
    finally:
        s.close()


@gen_test()
def test_services_with_port():
    s = Scheduler(services={('http', 9999): HTTPScheduler})
    s.start()
    try:
        assert isinstance(s.services['http'], HTTPServer)
        assert s.services['http'].port == 9999
    finally:
        s.close()


@gen_cluster(client=True)
def test_with_data(e, s, a, b):
    ss = HTTPScheduler(s)
    ss.listen(0)

    L = e.map(inc, [1, 2, 3])
    L2 = yield e._scatter(['Hello', 'world!'])
    yield _wait(L)

    client = AsyncHTTPClient()
    response = yield client.fetch('http://localhost:%d/memory-load.json' %
                                  ss.port)
    out = json.loads(response.body.decode())

    assert all(isinstance(v, int) for v in out.values())
    assert set(out) == {a.address, b.address}
    assert sum(out.values()) == sum(map(getsizeof,
                                        [1, 2, 3, 'Hello', 'world!']))

    response = yield client.fetch('http://localhost:%s/memory-load-by-key.json'
                                  % ss.port)
    out = json.loads(response.body.decode())
    assert set(out) == {a.address, b.address}
    assert all(isinstance(v, dict) for v in out.values())
    assert all(k in {'inc', 'data'} for d in out.values() for k in d)
    assert all(isinstance(v, int) for d in out.values() for v in d.values())

    assert sum(v for d in out.values() for v in d.values()) == \
            sum(map(getsizeof, [1, 2, 3, 'Hello', 'world!']))

    ss.stop()


@gen_cluster(client=True)
def test_with_status(e, s, a, b):
    ss = HTTPScheduler(s)
    ss.listen(0)

    client = AsyncHTTPClient()
    response = yield client.fetch('http://localhost:%d/tasks.json' %
                                  ss.port)
    out = json.loads(response.body.decode())
    assert out['total'] == 0
    assert out['processing'] == 0
    assert out['failed'] == 0
    assert out['in-memory'] == 0
    assert out['waiting'] == 0

    L = e.map(div, range(10), range(10))
    yield _wait(L)

    client = AsyncHTTPClient()
    response = yield client.fetch('http://localhost:%d/tasks.json' %
                                  ss.port)
    out = json.loads(response.body.decode())
    assert out['failed'] == 1
    assert out['in-memory'] == 9
    assert out['total'] == 10
    assert out['waiting'] == 0

    ss.stop()



@gen_cluster(client=True)
def test_basic(e, s, a, b):
    ss = HTTPScheduler(s)
    ss.listen(0)

    client = AsyncHTTPClient()
    for name in ['tasks', 'workers']:
        response = yield client.fetch('http://localhost:%d/%s.json' %
                                      (ss.port, name))
        data = json.loads(response.body.decode())
        assert isinstance(data, dict)

from __future__ import print_function, division, absolute_import

from datetime import datetime
import os
import sys
from time import time

import pytest
from toolz import valmap
from tornado.tcpclient import TCPClient
from tornado.iostream import StreamClosedError
from tornado import gen

from distributed import Nanny, rpc, Scheduler
from distributed.core import connect, read, write, dumps, loads
from distributed.utils import ignoring
from distributed.utils_test import gen_cluster
from distributed.nanny import isalive


@gen_cluster(ncores=[])
def test_nanny(s):
    n = Nanny(s.ip, s.port, ncores=2, ip='127.0.0.1', loop=s.loop)

    yield n._start(0)
    with rpc(ip=n.ip, port=n.port) as nn:
        assert isalive(n.process)  # alive
        assert s.ncores[n.worker_address] == 2

        assert s.worker_info[n.worker_address]['services']['nanny'] > 1024

        yield nn.kill()
        assert not n.process
        assert n.worker_address not in s.ncores
        assert n.worker_address not in s.worker_info

        yield nn.kill()
        assert n.worker_address not in s.ncores
        assert n.worker_address not in s.worker_info
        assert not n.process

        yield nn.instantiate()
        assert isalive(n.process)
        assert s.ncores[n.worker_address] == 2
        assert s.worker_info[n.worker_address]['services']['nanny'] > 1024

        yield nn.terminate()
        assert not n.process

    yield n._close()


@gen_cluster(ncores=[], timeout=20)
def test_nanny_process_failure(s):
    n = Nanny(s.ip, s.port, ncores=2, ip='127.0.0.1', loop=s.loop)
    yield n._start()
    first_dir = n.worker_dir

    assert os.path.exists(first_dir)

    original_process = n.process
    ww = rpc(ip=n.ip, port=n.worker_port)
    yield ww.update_data(data=valmap(dumps, {'x': 1, 'y': 2}))
    with ignoring(StreamClosedError):
        yield ww.compute(function=dumps(sys.exit),
                         args=dumps((0,)),
                         key='z')

    start = time()
    while n.process is original_process:  # wait while process dies
        yield gen.sleep(0.01)
        assert time() - start < 5

    start = time()
    while not isalive(n.process):  # wait while process comes back
        yield gen.sleep(0.01)
        assert time() - start < 5

    start = time()
    while n.worker_address not in s.ncores or n.worker_dir is None:
        yield gen.sleep(0.01)
        assert time() - start < 5

    second_dir = n.worker_dir

    yield n._close()
    assert not os.path.exists(second_dir)
    assert not os.path.exists(first_dir)
    assert first_dir != n.worker_dir
    ww.close_rpc()
    s.stop()


@gen_cluster(ncores=[])
def test_monitor_resources(s):
    pytest.importorskip('psutil')
    n = Nanny(s.ip, s.port, ncores=2, ip='127.0.0.1', loop=s.loop)

    yield n._start()
    assert isalive(n.process)
    d = n.resource_collect()
    assert {'cpu_percent', 'memory_percent'}.issubset(d)

    assert 'timestamp' in d

    stream = yield connect(ip=n.ip, port=n.port)
    yield write(stream, {'op': 'monitor_resources', 'interval': 0.01})

    for i in range(3):
        msg = yield read(stream)
        assert isinstance(msg, dict)
        assert {'cpu_percent', 'memory_percent'}.issubset(msg)

    stream.close()
    yield n._close()
    s.stop()


@gen_cluster(ncores=[])
def test_run(s):
    pytest.importorskip('psutil')
    n = Nanny(s.ip, s.port, ncores=2, ip='127.0.0.1', loop=s.loop)
    yield n._start()

    with rpc(n.address) as nn:
        response = yield nn.run(function=dumps(lambda: 1))
        assert response['status'] == 'OK'
        assert loads(response['result']) == 1

    yield n._close()

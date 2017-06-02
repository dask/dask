from __future__ import print_function, division, absolute_import

from datetime import datetime
import os
import random
import sys

import numpy as np

import pytest
from toolz import valmap
from tornado.tcpclient import TCPClient
from tornado import gen

from distributed import Nanny, rpc, Scheduler
from distributed.core import connect, CommClosedError
from distributed.metrics import time
from distributed.protocol.pickle import dumps, loads
from distributed.utils import ignoring
from distributed.utils_test import gen_cluster, gen_test, slow
from distributed.nanny import isalive


@gen_cluster(ncores=[])
def test_nanny(s):
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)

    yield n._start(0)
    with rpc(n.address) as nn:
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


@gen_cluster(Worker=Nanny)
def test_str(s, a, b):
    assert a.worker_address in str(a)
    assert a.worker_address in repr(a)
    assert str(a.ncores) in str(a)
    assert str(a.ncores) in repr(a)


@gen_cluster(ncores=[], timeout=20, client=True)
def test_nanny_process_failure(c, s):
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
    yield n._start()
    first_dir = n.worker_dir

    assert os.path.exists(first_dir)

    original_process = n.process
    ww = rpc(n.worker_address)
    yield ww.update_data(data=valmap(dumps, {'x': 1, 'y': 2}))
    with ignoring(CommClosedError):
        yield c._run(sys.exit, 0, workers=[n.worker_address])

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


def test_nanny_no_port():
    _ = str(Nanny('127.0.0.1', 8786))


@gen_cluster(ncores=[])
def test_monitor_resources(s):
    pytest.importorskip('psutil')
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)

    yield n._start()
    assert isalive(n.process)
    d = n.resource_collect()
    assert {'cpu_percent', 'memory_percent'}.issubset(d)

    assert 'timestamp' in d

    comm = yield connect(n.address)
    yield comm.write({'op': 'monitor_resources', 'interval': 0.01})

    for i in range(3):
        msg = yield comm.read()
        assert isinstance(msg, dict)
        assert {'cpu_percent', 'memory_percent'}.issubset(msg)

    yield comm.close()
    yield n._close()
    s.stop()


@gen_cluster(ncores=[])
def test_run(s):
    pytest.importorskip('psutil')
    n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
    yield n._start()

    with rpc(n.address) as nn:
        response = yield nn.run(function=dumps(lambda: 1))
        assert response['status'] == 'OK'
        assert response['result'] == 1

    yield n._close()



@gen_cluster(Worker=Nanny,
             ncores=[('127.0.0.1', 1)],
             worker_kwargs={'reconnect': False})
def test_close_on_disconnect(s, w):
    yield s.close()

    start = time()
    while w.status != 'closed':
        yield gen.sleep(0.01)
        assert time() < start + 9


@slow
@gen_cluster(client=False, ncores=[])
def test_nanny_death_timeout(s):
    yield s.close()
    w = Nanny(s.address, death_timeout=1)
    yield w._start()

    yield gen.sleep(3)
    assert w.status == 'closed'


@gen_cluster(client=True, Worker=Nanny)
def test_random_seed(c, s, a, b):
    @gen.coroutine
    def check_func(func):
        x = c.submit(func, 0, 2**31, pure=False, workers=a.worker_address)
        y = c.submit(func, 0, 2**31, pure=False, workers=b.worker_address)
        assert x.key != y.key
        x = yield x
        y = yield y
        assert x != y

    yield check_func(lambda a, b: random.randint(a, b))
    yield check_func(lambda a, b: np.random.randint(a, b))

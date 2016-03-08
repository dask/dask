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

from distributed import Nanny, Center, rpc
from distributed.core import connect, read, write, dumps
from distributed.utils import ignoring
from distributed.utils_test import gen_test


@gen_test()
def test_nanny():
    c = Center('127.0.0.1')
    c.listen(0)
    n = Nanny(c.ip, c.port, ncores=2, ip='127.0.0.1')

    yield n._start(0)
    nn = rpc(ip=n.ip, port=n.port)
    assert n.process.is_alive()
    assert c.ncores[n.worker_address] == 2
    assert c.worker_info[n.worker_address]['services']['nanny'] > 1024

    yield nn.kill()
    assert n.worker_address not in c.ncores
    assert n.worker_address not in c.worker_info
    assert not n.process

    yield nn.kill()
    assert n.worker_address not in c.ncores
    assert n.worker_address not in c.worker_info
    assert not n.process

    yield nn.instantiate()
    assert n.process.is_alive()
    assert c.ncores[n.worker_address] == 2
    assert c.worker_info[n.worker_address]['services']['nanny'] > 1024

    yield nn.terminate()
    assert not n.process

    if n.process:
        n.process.terminate()

    yield n._close()
    c.stop()


@gen_test()
def test_nanny_process_failure():
    c = Center('127.0.0.1')
    c.listen(0)
    n = Nanny(c.ip, c.port, ncores=2, ip='127.0.0.1')
    yield n._start()
    nn = rpc(ip=n.ip, port=n.port)
    first_dir = n.worker_dir

    assert os.path.exists(first_dir)

    ww = rpc(ip=n.ip, port=n.worker_port)
    yield ww.update_data(data=valmap(dumps, {'x': 1, 'y': 2}))
    with ignoring(StreamClosedError):
        yield ww.compute(function=dumps(sys.exit),
                         args=dumps((0,)),
                         key='z')

    start = time()
    while n.process.is_alive():  # wait while process dies
        yield gen.sleep(0.01)
        assert time() - start < 2

    start = time()
    while not n.process.is_alive():  # wait while process comes back
        yield gen.sleep(0.01)
        assert time() - start < 2

    start = time()
    while n.worker_address not in c.ncores or n.worker_dir is None:
        yield gen.sleep(0.01)
        assert time() - start < 2

    second_dir = n.worker_dir

    yield n._close()
    assert not os.path.exists(second_dir)
    assert not os.path.exists(first_dir)
    assert first_dir != n.worker_dir
    nn.close_streams()
    c.stop()


@gen_test()
def test_monitor_resources():
    pytest.importorskip('psutil')
    c = Center(ip='127.0.0.1')
    c.listen(0)
    n = Nanny(c.ip, c.port, ncores=2, ip='127.0.0.1')

    yield n._start()
    nn = rpc(ip=n.ip, port=n.port)
    assert n.process.is_alive()
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
    c.stop()

from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import os
import requests
import signal
from time import time, sleep
from toolz import first

from distributed import Scheduler, Client
from distributed.core import rpc
from distributed.nanny import isalive
from distributed.utils import sync, ignoring
from distributed.utils_test import (loop, popen, slow, terminate_process,
                                    wait_for_port)


def test_nanny_worker_ports(loop):
    with popen(['dask-scheduler', '--port', '8989']) as sched:
        with popen(['dask-worker', '127.0.0.1:8989', '--host', '127.0.0.1',
                    '--worker-port', '8788', '--nanny-port', '8789']) as worker:
            with Client('127.0.0.1:8989', loop=loop) as c:
                start = time()
                while True:
                    d = sync(c.loop, c.scheduler.identity)
                    if d['workers']:
                        break
                    else:
                        assert time() - start < 5
                        sleep(0.1)
                assert d['workers']['127.0.0.1:8788']['services']['nanny'] == 8789


def test_memory_limit(loop):
    with popen(['dask-scheduler']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--memory-limit', '2e9']) as worker:
            with Client('127.0.0.1:8786', loop=loop) as c:
                while not c.ncores():
                    sleep(0.1)
                info = c.scheduler_info()
                d = first(info['workers'].values())
                assert isinstance(d['memory_limit'], float)
                assert d['memory_limit'] == 2e9


def test_no_nanny(loop):
    with popen(['dask-scheduler']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--no-nanny']) as worker:
            assert any(b'Registered' in worker.stderr.readline()
                       for i in range(10))


@slow
@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_no_reconnect(nanny, loop):
    with popen(['dask-scheduler']) as sched:
        wait_for_port('127.0.0.1:8786')
        with popen(['dask-worker', '127.0.0.1:8786', '--no-reconnect', nanny]) as worker:
            sleep(2)
            terminate_process(sched)
        start = time()
        while isalive(worker):
            sleep(0.1)
            assert time() < start + 10


def test_resources(loop):
    with popen(['dask-scheduler']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786',
                    '--resources', 'A=1 B=2,C=3']) as worker:
            with Client('127.0.0.1:8786', loop=loop) as c:
                while not c.scheduler_info()['workers']:
                    sleep(0.1)
                info = c.scheduler_info()
                worker = list(info['workers'].values())[0]
                assert worker['resources'] == {'A': 1, 'B': 2, 'C': 3}

from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import os
import requests
import signal
from time import sleep
from toolz import first

from distributed import Scheduler, Client
from distributed.core import rpc
from distributed.metrics import time
from distributed.utils import sync, ignoring, tmpfile
from distributed.utils_test import (loop, popen, slow, terminate_process,
                                    wait_for_port)


def test_nanny_worker_ports(loop):
    with popen(['dask-scheduler', '--port', '9359', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:9359', '--host', '127.0.0.1',
                    '--worker-port', '9684', '--nanny-port', '5273',
                    '--no-bokeh']) as worker:
            with Client('127.0.0.1:9359', loop=loop) as c:
                start = time()
                while True:
                    d = sync(c.loop, c.scheduler.identity)
                    if d['workers']:
                        break
                    else:
                        assert time() - start < 5
                        sleep(0.1)
                assert d['workers']['tcp://127.0.0.1:9684']['services']['nanny'] == 5273


def test_memory_limit(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--memory-limit', '2e9',
                    '--no-bokeh']) as worker:
            with Client('127.0.0.1:8786', loop=loop) as c:
                while not c.ncores():
                    sleep(0.1)
                info = c.scheduler_info()
                d = first(info['workers'].values())
                assert isinstance(d['memory_limit'], float)
                assert d['memory_limit'] == 2e9


def test_no_nanny(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--no-nanny',
                    '--no-bokeh']) as worker:
            assert any(b'Registered' in worker.stderr.readline()
                       for i in range(15))


@slow
@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_no_reconnect(nanny, loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        wait_for_port(('127.0.0.1', 8786))
        with popen(['dask-worker', 'tcp://127.0.0.1:8786', '--no-reconnect', nanny,
                    '--no-bokeh']) as worker:
            sleep(2)
            terminate_process(sched)
        start = time()
        while worker.poll() is None:
            sleep(0.1)
            assert time() < start + 10


def test_resources(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', 'tcp://127.0.0.1:8786', '--no-bokeh',
                    '--resources', 'A=1 B=2,C=3']) as worker:
            with Client('127.0.0.1:8786', loop=loop) as c:
                while not c.scheduler_info()['workers']:
                    sleep(0.1)
                info = c.scheduler_info()
                worker = list(info['workers'].values())[0]
                assert worker['resources'] == {'A': 1, 'B': 2, 'C': 3}


@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_local_directory(loop, nanny):
    with tmpfile() as fn:
        with popen(['dask-scheduler', '--no-bokeh']) as sched:
            with popen(['dask-worker', '127.0.0.1:8786', nanny,
                        '--no-bokeh', '--local-directory', fn]) as worker:
                with Client('127.0.0.1:8786', loop=loop, timeout=10) as c:
                    start = time()
                    while not c.scheduler_info()['workers']:
                        sleep(0.1)
                        assert time() < start + 8
                    info = c.scheduler_info()
                    worker = list(info['workers'].values())[0]
                    assert worker['local_directory'].startswith(fn)


@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_scheduler_file(loop, nanny):
    with tmpfile() as fn:
        with popen(['dask-scheduler', '--no-bokeh', '--scheduler-file', fn]) as sched:
            with popen(['dask-worker', '--scheduler-file', fn, nanny, '--no-bokeh']):
                with Client(scheduler_file=fn, loop=loop) as c:
                    start = time()
                    while not c.scheduler_info()['workers']:
                        sleep(0.1)
                        assert time() < start + 10

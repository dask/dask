from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import sys
from time import sleep
from toolz import first

from distributed import Client
from distributed.metrics import time
from distributed.utils import sync, tmpfile
from distributed.utils_test import (popen, slow, terminate_process,
                                    wait_for_port)
from distributed.utils_test import loop  # flake8: noqa


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
        with popen(['dask-worker', '127.0.0.1:8786', '--memory-limit', '2e3MB',
                    '--no-bokeh']) as worker:
            with Client('127.0.0.1:8786', loop=loop) as c:
                while not c.ncores():
                    sleep(0.1)
                info = c.scheduler_info()
                d = first(info['workers'].values())
                assert isinstance(d['memory_limit'], int)
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


def test_nprocs_requires_nanny(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--nprocs=2',
                    '--no-nanny']) as worker:
            assert any(b'Failed to launch worker' in worker.stderr.readline()
                       for i in range(15))


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
@pytest.mark.parametrize('listen_address', [
    'tcp://0.0.0.0:39837',
    'tcp://127.0.0.2:39837'])
def test_contact_listen_address(loop, nanny, listen_address):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786',
                    nanny, '--no-bokeh',
                    '--contact-address', 'tcp://127.0.0.2:39837',
                    '--listen-address', listen_address]) as worker:
            with Client('127.0.0.1:8786') as client:
                while not client.ncores():
                    sleep(0.1)
                info = client.scheduler_info()
                assert 'tcp://127.0.0.2:39837' in info['workers']

                # roundtrip works
                assert client.submit(lambda x: x + 1, 10).result() == 11

                def func(dask_worker):
                    return dask_worker.listener.listen_address

                assert client.run(func) == {'tcp://127.0.0.2:39837': listen_address}


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
@pytest.mark.parametrize('host', ['127.0.0.2', '0.0.0.0'])
def test_respect_host_listen_address(loop, nanny, host):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786',
                    nanny, '--no-bokeh',
                    '--host', host]) as worker:
            with Client('127.0.0.1:8786') as client:
                while not client.ncores():
                    sleep(0.1)
                info = client.scheduler_info()

                # roundtrip works
                assert client.submit(lambda x: x + 1, 10).result() == 11

                def func(dask_worker):
                    return dask_worker.listener.listen_address

                listen_addresses = client.run(func)
                assert all(host in v for v in listen_addresses.values())

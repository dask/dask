from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

from contextlib import contextmanager
import os
import requests
import signal
import socket
import sys
from time import sleep, time


from distributed import Scheduler, Executor
from distributed.utils import get_ip, ignoring
from distributed.utils_test import loop, popen


def test_defaults(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as proc:
        with Executor('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as e:
            response = requests.get('http://127.0.0.1:9786/info.json')
            assert response.ok
            assert response.json()['status'] == 'running'


def test_hostport(loop):
    with popen(['dask-scheduler', '--no-bokeh', '--host', '127.0.0.1:8978']):
        with Executor('127.0.0.1:8978', loop=loop) as e:
            assert len(e.ncores()) == 0


def test_no_bokeh(loop):
    pytest.importorskip('bokeh')
    with popen(['dask-scheduler', '--no-bokeh']) as proc:
        with Executor('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as e:
            for i in range(3):
                line = proc.stderr.readline()
                assert b'bokeh' not in line.lower()


def test_bokeh(loop):
    pytest.importorskip('bokeh')

    with popen(['dask-scheduler']) as proc:
        with Executor('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as e:
            pass

        while True:
            line = proc.stderr.readline()
            if b'Bokeh UI' in line:
                break

        start = time()
        while True:
            try:
                for name in [socket.gethostname(), 'localhost', '127.0.0.1', get_ip()]:
                    response = requests.get('http://%s:8787/status/' % name)
                    assert response.ok
                break
            except Exception as f:
                print(f)
                sleep(0.1)
                assert time() < start + 10


def test_bokeh_non_standard_ports(loop):
    pytest.importorskip('bokeh')

    with popen(['dask-scheduler',
                '--port', '3448',
                '--http-port', '4824',
                '--bokeh-port', '4832']) as proc:
        with Executor('127.0.0.1:3448', loop=loop) as e:
            pass

        while True:
            line = proc.stderr.readline()
            if b'Bokeh UI' in line:
                break

        start = time()
        while True:
            try:
                response = requests.get('http://localhost:4832/status/')
                assert response.ok
                break
            except:
                sleep(0.1)
                assert time() < start + 20


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
def test_bokeh_whitelist(loop):
    pytest.importorskip('bokeh')
    with pytest.raises(Exception):
        requests.get('http://localhost:8787/status/').ok

    with popen(['dask-scheduler', '--bokeh-whitelist', '127.0.0.2',
                                  '--bokeh-whitelist', '127.0.0.3']) as proc:
        with Executor('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as e:
            pass

        while True:
            line = proc.stderr.readline()
            if b'Bokeh UI' in line:
                break

        start = time()
        while True:
            try:
                for name in ['127.0.0.2', '127.0.0.3']:
                    response = requests.get('http://%s:8787/status/' % name)
                    assert response.ok
                break
            except Exception as f:
                print(f)
                sleep(0.1)
                assert time() < start + 20


def test_multiple_workers(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as s:
        with popen(['dask-worker', 'localhost:8786']) as a:
            with popen(['dask-worker', 'localhost:8786']) as b:
                with Executor('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as e:
                    start = time()
                    while len(e.ncores()) < 2:
                        sleep(0.1)
                        assert time() < start + 10

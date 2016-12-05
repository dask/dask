from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

from contextlib import contextmanager
import os
import requests
import signal
import socket
import sys
from time import sleep


from distributed import Scheduler, Client
from distributed.utils import get_ip, ignoring, tmpfile
from distributed.utils_test import loop, popen
from distributed.metrics import time


def test_defaults(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as proc:
        with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
            response = requests.get('http://127.0.0.1:9786/info.json')
            assert response.ok
            assert response.json()['status'] == 'running'
    with pytest.raises(Exception):
        response = requests.get('http://127.0.0.1:9786/info.json')
    with pytest.raises(Exception):
        requests.get('http://127.0.0.1:8787/status/')


def test_hostport(loop):
    with popen(['dask-scheduler', '--no-bokeh', '--host', '127.0.0.1:8978']):
        with Client('127.0.0.1:8978', loop=loop) as c:
            assert len(c.ncores()) == 0


def test_no_bokeh(loop):
    pytest.importorskip('bokeh')
    with popen(['dask-scheduler', '--no-bokeh']) as proc:
        with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
            for i in range(3):
                line = proc.stderr.readline()
                assert b'bokeh' not in line.lower()
            with pytest.raises(Exception):
                requests.get('http://127.0.0.1:8787/status/')


def test_bokeh(loop):
    pytest.importorskip('bokeh')

    with popen(['dask-scheduler']) as proc:
        with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
            pass

        names = ['localhost', '127.0.0.1', get_ip()]
        if 'linux' in sys.platform:
            names.append(socket.gethostname())

        start = time()
        while True:
            try:
                for name in names:
                    response = requests.get('http://%s:8787/status/' % name)
                    assert response.ok
                break
            except Exception as f:
                print(f)
                sleep(0.1)
                assert time() < start + 10
    with pytest.raises(Exception):
        requests.get('http://127.0.0.1:8787/status/')


def test_bokeh_non_standard_ports(loop):
    pytest.importorskip('bokeh')

    with popen(['dask-scheduler',
                '--port', '3448',
                '--http-port', '4824',
                '--bokeh-port', '4832']) as proc:
        with Client('127.0.0.1:3448', loop=loop) as c:
            pass

        start = time()
        while True:
            try:
                response = requests.get('http://localhost:4832/status/')
                assert response.ok
                break
            except:
                sleep(0.1)
                assert time() < start + 20
    with pytest.raises(Exception):
        requests.get('http://localhost:4832/status/')


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
def test_bokeh_whitelist(loop):
    pytest.importorskip('bokeh')
    with pytest.raises(Exception):
        requests.get('http://localhost:8787/status/').ok

    with popen(['dask-scheduler', '--bokeh-whitelist', '127.0.0.2:8787',
                                  '--bokeh-whitelist', '127.0.0.3:8787']) as proc:
        with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
            pass

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
        with popen(['dask-worker', 'localhost:8786', '--no-bokeh']) as a:
            with popen(['dask-worker', 'localhost:8786', '--no-bokeh']) as b:
                with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
                    start = time()
                    while len(c.ncores()) < 2:
                        sleep(0.1)
                        assert time() < start + 10


def test_pid_file(loop):
    def check_pidfile(proc, pidfile):
        while not os.path.exists(pidfile):
            sleep(0.01)
        text = False
        while not text:
            sleep(0.01)
            with open(pidfile) as f:
                text = f.read()
        pid = int(text)
        if sys.platform.startswith('win'):
            # On Windows, `dask-XXX` invokes the dask-XXX.exe
            # shim, but the PID is written out by the child Python process
            assert pid
        else:
            assert proc.pid == pid

    with tmpfile() as s:
        with popen(['dask-scheduler', '--pid-file', s, '--no-bokeh']) as sched:
            check_pidfile(sched, s)

        with tmpfile() as w:
            with popen(['dask-worker', '127.0.0.1:8786', '--pid-file', w,
                        '--no-bokeh']) as worker:
                check_pidfile(worker, w)

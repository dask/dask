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


from distributed import Scheduler, Client
from distributed.utils import get_ip, ignoring, tmpfile
from distributed.utils_test import loop, popen


def test_defaults(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as proc:
        with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
            response = requests.get('http://127.0.0.1:9786/info.json')
            assert response.ok
            assert response.json()['status'] == 'running'
    with pytest.raises(Exception):
        requests.get('http://127.0.0.1:9786/info.json')
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

        while True:
            line = proc.stderr.readline()
            if b'Bokeh UI' in line:
                break

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
    with pytest.raises(Exception):
        requests.get('http://127.0.0.1:4832/status/')


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
                with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
                    start = time()
                    while len(c.ncores()) < 2:
                        sleep(0.1)
                        assert time() < start + 10


def test_pid_file(loop):
    with tmpfile() as s:
        with popen(['dask-scheduler', '--pid-file', s]) as sched:
            while not os.path.exists(s):
                sleep(0.01)
            text = False
            while not text:
                sleep(0.01)
                with open(s) as f:
                    text = f.read()
            pid = int(text)

            assert sched.pid == pid

        with tmpfile() as w:
            with popen(['dask-worker', '127.0.0.1:8786', '--pid-file', w]) as worker:
                while not os.path.exists(w):
                    sleep(0.01)
                text = False
                while not text:
                    sleep(0.01)
                    with open(w) as f:
                        text = f.read()

                pid = int(text)

                assert worker.pid == pid

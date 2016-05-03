from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import os
import requests
import signal
import socket
from subprocess import Popen, PIPE
import sys
from time import sleep, time


from distributed import Scheduler, Executor
from distributed.utils import get_ip, ignoring


def test_defaults():
    try:
        proc = Popen(['dscheduler', '--no-bokeh'], stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:%d' % Scheduler.default_port)

        response = requests.get('http://127.0.0.1:9786/info.json')
        assert response.ok
        assert response.json()['status'] == 'running'
    finally:
        e.shutdown()
        os.kill(proc.pid, signal.SIGINT)


def test_no_bokeh():
    pytest.importorskip('bokeh')

    try:
        proc = Popen(['dscheduler', '--no-bokeh'], stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:%d' % Scheduler.default_port)
        for i in range(3):
            line = proc.stderr.readline()
            assert b'bokeh' not in line.lower()
    finally:
        with ignoring(Exception):
            e.shutdown()
        with ignoring(Exception):
            os.kill(proc.pid, signal.SIGINT)


def test_bokeh():
    pytest.importorskip('bokeh')

    try:
        proc = Popen(['dscheduler'], stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:%d' % Scheduler.default_port)

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
            except:
                sleep(0.1)
                assert time() < start + 5

    finally:
        with ignoring(Exception):
            e.shutdown()
        with ignoring(Exception):
            os.kill(proc.pid, signal.SIGINT)


def test_bokeh_non_standard_ports():
    pytest.importorskip('bokeh')

    try:
        proc = Popen(['dscheduler',
                      '--port', '3448',
                      '--http-port', '4824',
                      '--bokeh-port', '4832'], stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:3448')

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
                assert time() < start + 5

    finally:
        with ignoring(Exception):
            e.shutdown()
        with ignoring(Exception):
            os.kill(proc.pid, signal.SIGINT)


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
def test_bokeh_whitelist():
    pytest.importorskip('bokeh')

    try:
        proc = Popen(['dscheduler', '--bokeh-whitelist', '127.0.0.2',
                                    '--bokeh-whitelist', '127.0.0.3'],
                     stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:%d' % Scheduler.default_port)

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
            except:
                sleep(0.1)
                assert time() < start + 5

    finally:
        with ignoring(Exception):
            e.shutdown()
        with ignoring(Exception):
            os.kill(proc.pid, signal.SIGINT)

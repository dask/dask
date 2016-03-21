from __future__ import print_function, division, absolute_import

import socket
from subprocess import Popen, PIPE
import requests

import pytest

from distributed import Scheduler, Executor
from distributed.utils import get_ip


def test_defaults():
    try:
        proc = Popen(['dscheduler'], stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:%d' % Scheduler.default_port)

        response = requests.get('http://127.0.0.1:9786/info.json')
        assert response.ok
        assert response.json()['status'] == 'running'
    finally:
        e.shutdown()
        proc.kill()


def test_bokeh():
    pytest.importorskip('bokeh')

    try:
        proc = Popen(['dscheduler'], stdout=PIPE, stderr=PIPE)
        e = Executor('127.0.0.1:%d' % Scheduler.default_port)

        for name in [socket.gethostname(), 'localhost', '127.0.0.1', get_ip()]:
            response = requests.get('http://%s:8787/status/' % name)
            assert response.ok
    finally:
        e.shutdown()
        proc.kill()

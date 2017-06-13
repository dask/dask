from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import os
import requests
import socket
import shutil
import sys
import tempfile
from time import sleep

from tornado import gen

from distributed import Scheduler, Client
from distributed.compatibility import WINDOWS
from distributed.utils import get_ip, get_ip_interface, ignoring, tmpfile
from distributed.utils_test import (loop, popen,
                                    assert_can_connect_from_everywhere_4,
                                    assert_can_connect_from_everywhere_4_6,
                                    assert_can_connect_locally_4,
                                    )
from distributed.metrics import time


def test_defaults(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as proc:

        @gen.coroutine
        def f():
            # Default behaviour is to listen on all addresses
            yield [
                assert_can_connect_from_everywhere_4_6(8786, 2.0),  # main port
                assert_can_connect_from_everywhere_4_6(9786, 2.0),  # HTTP port
                ]

        loop.run_sync(f)

        with Client('127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
            response = requests.get('http://127.0.0.1:9786/info.json')
            assert response.ok
            assert response.json()['status'] == 'running'

    with pytest.raises(Exception):
        response = requests.get('http://127.0.0.1:9786/info.json')
    with pytest.raises(Exception):
        requests.get('http://127.0.0.1:8787/status/')


def test_hostport(loop):
    with popen(['dask-scheduler', '--no-bokeh', '--host', '127.0.0.1:8978',
                '--http-port', '8979']):
        @gen.coroutine
        def f():
            yield [
                # The scheduler's main port can't be contacted from the outside
                assert_can_connect_locally_4(8978, 2.0),
                # ... but its HTTP port can
                assert_can_connect_from_everywhere_4_6(8979, 2.0),
                ]

        loop.run_sync(f)

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
                # All addresses should respond
                for name in names:
                    uri = 'http://%s:8787/status/' % name
                    response = requests.get(uri)
                    assert response.ok
                break
            except Exception as f:
                print('got error on %r: %s' % (uri, f))
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


def test_interface(loop):
    psutil = pytest.importorskip('psutil')
    if_names = sorted(psutil.net_if_addrs())
    for if_name in if_names:
        try:
            ipv4_addr = get_ip_interface(if_name)
        except ValueError:
            pass
        else:
            if ipv4_addr == '127.0.0.1':
                break
    else:
        pytest.skip("Could not find loopback interface. "
                    "Available interfaces are: %s." % (if_names,))

    with popen(['dask-scheduler', '--no-bokeh', '--interface', if_name]) as s:
        with popen(['dask-worker', '127.0.0.1:8786', '--no-bokeh', '--interface', if_name]) as a:
            with Client('tcp://127.0.0.1:%d' % Scheduler.default_port, loop=loop) as c:
                start = time()
                while not len(c.ncores()):
                    sleep(0.1)
                    assert time() - start < 5
                info = c.scheduler_info()
                assert 'tcp://127.0.0.1' in info['address']
                assert all('127.0.0.1' == d['host']
                           for d in info['workers'].values())


def test_pid_file(loop):
    def check_pidfile(proc, pidfile):
        start = time()
        while not os.path.exists(pidfile):
            sleep(0.01)
            assert time() < start + 5

        text = False
        start = time()
        while not text:
            sleep(0.01)
            assert time() < start + 5
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


def test_scheduler_port_zero(loop):
    with tmpfile() as fn:
        with popen(['dask-scheduler', '--no-bokeh', '--scheduler-file', fn,
                    '--port', '0']) as sched:
            with Client(scheduler_file=fn, loop=loop) as c:
                assert c.scheduler.port
                assert c.scheduler.port != 8786


def test_bokeh_port_zero(loop):
    pytest.importorskip('bokeh')
    with tmpfile() as fn:
        with popen(['dask-scheduler', '--bokeh-port', '0']) as proc:
            count = 0
            while count < 1:
                line = proc.stderr.readline()
                if b'bokeh' in line.lower() or b'web' in line.lower():
                    sleep(0.01)
                    count += 1
                    assert b':0' not in line


PRELOAD_TEXT = """
_scheduler_info = {}

def dask_setup(scheduler):
    _scheduler_info['address'] = scheduler.address

def get_scheduler_address():
    return _scheduler_info['address']
"""


def test_preload_file(loop):

    def check_scheduler():
        import scheduler_info
        return scheduler_info.get_scheduler_address()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, 'scheduler_info.py')
        with open(path, 'w') as f:
            f.write(PRELOAD_TEXT)
        with tmpfile() as fn:
            with popen(['dask-scheduler', '--scheduler-file', fn,
                        '--preload', path]):
                with Client(scheduler_file=fn, loop=loop) as c:
                    assert c.run_on_scheduler(check_scheduler) == \
                           c.scheduler.address
    finally:
        shutil.rmtree(tmpdir)


def test_preload_module(loop):

    def check_scheduler():
        import scheduler_info
        return scheduler_info.get_scheduler_address()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, 'scheduler_info.py')
        with open(path, 'w') as f:
            f.write(PRELOAD_TEXT)
        with tmpfile() as fn:
            with popen(['dask-scheduler', '--scheduler-file', fn,
                        '--preload', 'scheduler_info'],
                       env=dict(os.environ, PYTHONPATH=tmpdir)):
                with Client(scheduler_file=fn, loop=loop) as c:
                    assert c.run_on_scheduler(check_scheduler) == \
                           c.scheduler.address
    finally:
        shutil.rmtree(tmpdir)

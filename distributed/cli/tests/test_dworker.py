from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import os
import requests
from subprocess import Popen, PIPE
import signal
from time import time, sleep

from distributed import Scheduler, Client
from distributed.core import rpc
from distributed.utils import sync, ignoring
from distributed.utils_test import loop, popen


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


def test_no_nanny(loop):
    with popen(['dask-scheduler']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--no-nanny']) as worker:
            assert any(b'Registered' in worker.stderr.readline()
                       for i in range(10))

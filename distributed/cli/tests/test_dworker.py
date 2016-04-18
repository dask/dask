from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import os
import requests
from subprocess import Popen, PIPE
import signal
from time import time, sleep

from distributed import Scheduler, Executor
from distributed.core import rpc
from distributed.utils import sync, ignoring
from distributed.utils_test import loop


def test_nanny_worker_ports(loop):
    try:
        worker = Popen(['dworker', '127.0.0.1:8989', '--host', '127.0.0.1',
                        '--worker-port', '8788', '--nanny-port', '8789'],
                        stdout=PIPE, stderr=PIPE)
        sched = Popen(['dscheduler', '--port', '8989'], stdout=PIPE, stderr=PIPE)
        with Executor('127.0.0.1:8989', loop=loop) as e:
            start = time()
            while True:
                d = sync(e.loop, e.scheduler.identity)
                if d['workers']:
                    break
                else:
                    assert time() - start < 5
                    sleep(0.1)
            assert d['workers']['127.0.0.1:8788']['services']['nanny'] == 8789
    finally:
        with ignoring(Exception):
            w = rpc('127.0.0.1:8789')
            sync(loop, w.terminate)

        with ignoring(Exception):
            os.kill(sched.pid, signal.SIGINT)

        with ignoring(Exception):
            worker.kill()

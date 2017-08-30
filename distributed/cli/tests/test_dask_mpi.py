from __future__ import print_function, division, absolute_import

import subprocess
from time import sleep

import pytest
pytest.importorskip('mpi4py')

from distributed import Client
from distributed.utils import tmpfile
from distributed.metrics import time
from distributed.utils_test import popen
from distributed.utils_test import loop  # flake8: noqa


def test_basic(loop):
    with tmpfile() as fn:
        with popen(['mpirun', '--np', '4', 'dask-mpi', '--scheduler-file', fn],
                   stdin=subprocess.DEVNULL):
            with Client(scheduler_file=fn) as c:

                start = time()
                while len(c.scheduler_info()['workers']) != 3:
                    assert time() < start + 10
                    sleep(0.2)

                assert c.submit(lambda x: x + 1, 10, workers=1).result() == 11


def test_no_scheduler(loop):
    with tmpfile() as fn:
        with popen(['mpirun', '--np', '2', 'dask-mpi', '--scheduler-file', fn],
                   stdin=subprocess.DEVNULL):
            with Client(scheduler_file=fn) as c:

                start = time()
                while len(c.scheduler_info()['workers']) != 1:
                    assert time() < start + 10
                    sleep(0.2)

                assert c.submit(lambda x: x + 1, 10).result() == 11
                with popen(['mpirun', '--np', '1', 'dask-mpi',
                            '--scheduler-file', fn, '--no-scheduler']):

                    start = time()
                    while len(c.scheduler_info()['workers']) != 2:
                        assert time() < start + 10
                        sleep(0.2)

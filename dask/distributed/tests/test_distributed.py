"""Tests that require a running dask.distributed cluster"""

import time
from subprocess import Popen, PIPE
from distutils.version import StrictVersion

import pytest

np = pytest.importorskip('numpy')
IPython = pytest.importorskip('IPython')
if StrictVersion(IPython.__version__) < '4.0':
    ipp = pytest.importorskip('IPython.parallel')
else:
    ipp = pytest.importorskip('ipyparallel')

import numpy as np
from numpy.testing import assert_array_almost_equal

from contextlib import contextmanager

from dask.distributed import dask_client_from_ipclient
import dask.array as da
import dask.bag as db


def setup_cluster():
    start_cmd = ['ipcluster', 'start', '-n', '4']
    cp = Popen(start_cmd, stdout=PIPE, stderr=PIPE)
    tic = time.time()
    while True:
        line = cp.stderr.readline().decode()
        if 'Engines appear to have started' in line:
            break
        elif 'CRITICAL | Cluster is already running' in line:
            raise RuntimeWarning("Cluster is already running")
        elif time.time() - tic > 120:
            raise RuntimeError("Timeout waiting for cluster to start.")
        time.sleep(0.1)


def teardown_cluster():
    kill_cmd = ['ipcluster', 'stop']
    cp = Popen(kill_cmd, stdout=PIPE, stderr=PIPE)
    tic = time.time()
    while True:
        line = cp.stderr.readline().decode()
        if "Stopping cluster" in line:
            break
        elif time.time()-tic > 15:
            # probably stopped or something went wrong
            break
        time.sleep(0.1)

@contextmanager
def ipcluster():
    setup_cluster()
    try:
        c = ipp.Client()
        yield c
    finally:
        teardown_cluster()

@pytest.mark.xfail
@pytest.mark.slow
def test_dask_client_from_ipclient():
    with ipcluster() as c:
        dask_client = dask_client_from_ipclient(c)

        # data
        a = np.arange(100).reshape(10, 10)
        d = da.from_array(a, ((5, 5), (5, 5)))

        try:
            # test array.mean
            expected = a.mean(axis=0)
            d1 = d.mean(axis=0)
            result = d1.compute(get=dask_client.get)
            assert_array_almost_equal(result, expected)

            # test ghosting
            d2 = da.ghost.ghost(d, depth=1, boundary='reflect')
            d3 = da.ghost.trim_internal(d2, {0: 1, 1: 1})
            result1 = d3.compute(get=dask_client.get)
            assert_array_almost_equal(result1, a)

        finally:
            # close the workers
            dask_client.close(close_scheduler=True)


def test_gh_248():
    """bag.sum() and bag.count() would hang with the distributed scheduler"""
    a = db.from_sequence(range(50), npartitions=2)
    a.sum().compute()
    a.count().compute()

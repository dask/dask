
from functools import partial
import sys
from time import sleep, time
import unittest

import pytest

from distributed.deploy.local import LocalCluster
from distributed import Executor, Worker, Nanny
from distributed.utils_test import inc, loop, raises
from distributed.utils import ignoring

from distributed.deploy.utils_test import ClusterTest

def test_simple(loop):
    with LocalCluster(4, scheduler_port=0, nanny=False, silence_logs=False,
            loop=loop) as c:
        with Executor((c.scheduler.ip, c.scheduler.port), loop=loop) as e:
            x = e.submit(inc, 1)
            x.result()
            assert x.key in c.scheduler.tasks
            assert any(w.data == {x.key: 2} for w in c.workers)


@pytest.mark.skipif('sys.version_info[0] == 2', reason='multi-loop')
def test_procs(loop):
    with LocalCluster(2, scheduler_port=0, nanny=False, threads_per_worker=3,
            silence_logs=False) as c:
        assert len(c.workers) == 2
        assert all(isinstance(w, Worker) for w in c.workers)
        with Executor((c.scheduler.ip, c.scheduler.port), loop=loop) as e:
            assert all(w.ncores == 3 for w in c.workers)
        repr(c)

    with LocalCluster(2, scheduler_port=0, nanny=True, threads_per_worker=3,
            silence_logs=False) as c:
        assert len(c.workers) == 2
        assert all(isinstance(w, Nanny) for w in c.workers)
        with Executor((c.scheduler.ip, c.scheduler.port), loop=loop) as e:
            assert all(v == 3 for v in e.ncores().values())

            c.start_worker(nanny=False)
            assert isinstance(c.workers[-1], Worker)
        repr(c)


@pytest.mark.skipif('sys.version_info[0] == 2', reason='')
class LocalTest(ClusterTest, unittest.TestCase):
    Cluster = partial(LocalCluster, silence_logs=False)


def test_Executor_with_local(loop):
    with LocalCluster(1, scheduler_port=0, silence_logs=False, loop=loop) as c:
        with Executor(c, loop=loop) as e:
            assert len(e.ncores()) == len(c.workers)
            assert c.scheduler_address in repr(e)


def test_Executor_solo(loop):
    e = Executor(loop=loop)
    e.shutdown()


def test_defaults():
    from distributed.worker import _ncores

    with LocalCluster(scheduler_port=0, silence_logs=False) as c:
        assert sum(w.ncores for w in c.workers) == _ncores
        assert all(isinstance(w, Nanny) for w in c.workers)
        assert all(w.ncores == 1 for w in c.workers)

    with LocalCluster(nanny=False, scheduler_port=0, silence_logs=False) as c:
        assert sum(w.ncores for w in c.workers) == _ncores
        assert all(isinstance(w, Worker) for w in c.workers)
        assert len(c.workers) == 1


def test_cleanup():
    c = LocalCluster(2, scheduler_port=0, silence_logs=False)
    port = c.scheduler.port
    c.close()
    c2 = LocalCluster(2, scheduler_port=port, silence_logs=False)
    c.close()


def test_repeated():
    with LocalCluster(scheduler_port=8448, silence_logs=False) as c:
        pass
    with LocalCluster(scheduler_port=8448, silence_logs=False) as c:
        pass


def test_http(loop):
    from distributed.http import HTTPScheduler
    import requests
    with LocalCluster(scheduler_port=0, silence_logs=False,
            services={('http', 3485): HTTPScheduler}, loop=loop) as c:
        response = requests.get('http://127.0.0.1:3485/info.json')
        assert response.ok


def test_bokeh(loop):
    from distributed.http import HTTPScheduler
    import requests
    with LocalCluster(scheduler_port=0, silence_logs=False, loop=loop,
            diagnostics_port=4724, services={('http', 0): HTTPScheduler}) as c:
        start = time()
        while True:
            with ignoring(Exception):
                response = requests.get('http://127.0.0.1:%d/status/' %
                                        c.diagnostics.port)
                if response.ok:
                    break
            assert time() < start + 20
            sleep(0.01)

    start = time()
    while not raises(lambda: requests.get('http://127.0.0.1:%d/status/' % 4724)):
        assert time() < start + 10
        sleep(0.01)


def test_start_diagnostics(loop):
    from distributed.http import HTTPScheduler
    import requests
    with LocalCluster(scheduler_port=0, silence_logs=False, loop=loop) as c:
        c.start_diagnostics_server(show=False, port=3748)

        start = time()
        while True:
            with ignoring(Exception):
                response = requests.get('http://127.0.0.1:%d/status/' %
                                        c.diagnostics.port)
                if response.ok:
                    break
            assert time() < start + 20
            sleep(0.01)

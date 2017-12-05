from __future__ import print_function, division, absolute_import

from time import sleep

from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client
from distributed.deploy import Adaptive, LocalCluster
from distributed.utils_test import gen_cluster, gen_test, slowinc
from distributed.utils_test import loop, nodebug  # flake8: noqa
from distributed.metrics import time


def test_get_scale_up_kwargs(loop):
    with LocalCluster(0, scheduler_port=0, silence_logs=False,
                      diagnostics_port=None, loop=loop) as cluster:

        alc = Adaptive(cluster.scheduler, cluster, interval=100,
                       scale_factor=3)
        assert alc.get_scale_up_kwargs() == {'n': 1}

        with Client(cluster, loop=loop) as c:
            future = c.submit(lambda x: x + 1, 1)
            assert future.result() == 2
            assert c.ncores()
            assert alc.get_scale_up_kwargs() == {'n': 3}

@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test_simultaneous_scale_up_and_down(c, s, *workers):
    class TestAdaptive(Adaptive):
        def get_scale_up_kwargs(self):
            assert False

        def _retire_workers(self):
            assert False

    class TestCluster(object):
        def scale_up(self, n, **kwargs):
            assert False

        def scale_down(self, workers):
            assert False

    cluster = TestCluster()

    s.task_duration['a'] = 4
    s.task_duration['b'] = 4
    s.task_duration['c'] = 1

    future = c.map(slowinc, [1, 1, 1], key=['a-4', 'b-4', 'c-1'])

    while len(s.rprocessing) < 3:
        yield gen.sleep(0.001)

    ta = TestAdaptive(s, cluster, interval=100, scale_factor=2)

    yield gen.sleep(0.3)


def test_adaptive_local_cluster(loop):
    with LocalCluster(0, scheduler_port=0, silence_logs=False,
                      diagnostics_port=None, loop=loop) as cluster:
        alc = Adaptive(cluster.scheduler, cluster, interval=100)
        with Client(cluster, loop=loop) as c:
            assert not c.ncores()
            future = c.submit(lambda x: x + 1, 1)
            assert future.result() == 2
            assert c.ncores()

            sleep(0.1)
            assert c.ncores()  # still there after some time

            del future

            start = time()
            while cluster.scheduler.ncores:
                sleep(0.01)
                assert time() < start + 5

            assert not c.ncores()


@nodebug
@gen_test(timeout=30)
def test_adaptive_local_cluster_multi_workers():
    loop = IOLoop.current()
    cluster = LocalCluster(0, scheduler_port=0, silence_logs=False, processes=False,
                           diagnostics_port=None, loop=loop, start=False)
    cluster.scheduler.allowed_failures = 1000
    alc = Adaptive(cluster.scheduler, cluster, interval=100)
    c = yield Client(cluster, asynchronous=True, loop=loop)

    futures = c.map(slowinc, range(100), delay=0.01)

    start = time()
    while not cluster.scheduler.worker_info:
        yield gen.sleep(0.01)
        assert time() < start + 15

    yield c._gather(futures)
    del futures

    start = time()
    while cluster.workers:
        yield gen.sleep(0.01)
        assert time() < start + 5

    assert not cluster.workers
    assert not cluster.scheduler.workers
    yield gen.sleep(0.2)
    assert not cluster.workers
    assert not cluster.scheduler.workers

    futures = c.map(slowinc, range(100), delay=0.01)
    yield c._gather(futures)

    yield c._close()
    yield cluster._close()


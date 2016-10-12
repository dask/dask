from __future__ import print_function, division, absolute_import

from time import sleep, time

from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client
from distributed.deploy import Adaptive, LocalCluster
from distributed.utils_test import loop, slowinc, gen_test


def test_adaptive_local_cluster(loop):
    with LocalCluster(0, scheduler_port=0, silence_logs=False,
                      diagnostic_port=None, loop=loop) as cluster:
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


@gen_test(timeout=120)
def test_adaptive_local_cluster_multi_workers():
    loop = IOLoop.current()
    cluster = LocalCluster(0, scheduler_port=0, silence_logs=False, nanny=False,
                      diagnostic_port=None, loop=loop, start=False)
    cluster.scheduler.allowed_failures = 1000
    alc = Adaptive(cluster.scheduler, cluster, interval=100)
    c = Client(cluster, start=False, loop=loop)
    yield c._start()

    for i in range(20):
        futures = c.map(slowinc, range(100), delay=0.01)
        yield c._gather(futures)
        del futures
        yield gen.sleep(0.1)

    yield c._shutdown()
    yield cluster._close()

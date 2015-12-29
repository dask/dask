from distributed.utils_test import cluster, loop, scheduler
from distributed.core import rpc

def test_cluster(loop):
    with cluster() as (c, [a, b]):
        pass


def test_scheduler(loop):
    with cluster() as (c, [a, b]):
        with scheduler(c['port']) as sport:
            r = rpc(ip='127.0.0.1', port=sport)
            resp = loop.run_sync(r.ncores)
            assert len(resp) == 2

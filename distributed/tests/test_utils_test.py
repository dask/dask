from distributed.utils_test import (cluster, loop, scheduler, gen_cluster,
        gen_test, cluster_center)
from distributed.core import rpc
from distributed import Scheduler, Worker
from tornado import gen

def test_cluster(loop):
    with cluster() as (s, [a, b]):
        s = rpc(ip='127.0.0.1', port=s['port'])
        ident = loop.run_sync(s.identity)
        assert ident['type'] == 'Scheduler'
        assert len(ident['workers']) == 2


def test_scheduler(loop):
    with cluster_center() as (c, [a, b]):
        with scheduler(c['port']) as sport:
            r = rpc(ip='127.0.0.1', port=sport)
            resp = loop.run_sync(r.ncores)
            assert len(resp) == 2


@gen_cluster()
def test_gen_cluster(s, a, b):
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.ncores == {w.address: w.ncores for w in [a, b]}


@gen_test()
def test_gen_test():
    yield gen.sleep(0.01)

from __future__ import print_function, division, absolute_import

from distributed.utils_test import (cluster, loop, gen_cluster,
        gen_test)
from distributed.core import rpc
from distributed import Scheduler, Worker, Client
from tornado import gen

def test_cluster(loop):
    with cluster() as (s, [a, b]):
        with rpc(ip='127.0.0.1', port=s['port']) as s:
            ident = loop.run_sync(s.identity)
            assert ident['type'] == 'Scheduler'
            assert len(ident['workers']) == 2


@gen_cluster(client=True)
def test_gen_cluster(e, s, a, b):
    assert isinstance(e, Client)
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.ncores == {w.address: w.ncores for w in [a, b]}

@gen_cluster(client=False)
def test_gen_cluster_without_client(s, a, b):
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.ncores == {w.address: w.ncores for w in [a, b]}

@gen_test()
def test_gen_test():
    yield gen.sleep(0.01)

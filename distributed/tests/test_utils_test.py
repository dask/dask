from __future__ import print_function, division, absolute_import

from distributed.utils_test import (cluster, loop, gen_cluster,
        gen_test)
from distributed.core import rpc
from distributed import Scheduler, Worker, Executor
from tornado import gen

def test_cluster(loop):
    with cluster() as (s, [a, b]):
        s = rpc(ip='127.0.0.1', port=s['port'])
        ident = loop.run_sync(s.identity)
        assert ident['type'] == 'Scheduler'
        assert len(ident['workers']) == 2


@gen_cluster(executor=True)
def test_gen_cluster(e, s, a, b):
    assert isinstance(e, Executor)
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.ncores == {w.address: w.ncores for w in [a, b]}

@gen_cluster(executor=False)
def test_gen_cluster_without_executor(s, a, b):
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.ncores == {w.address: w.ncores for w in [a, b]}

@gen_test()
def test_gen_test():
    yield gen.sleep(0.01)

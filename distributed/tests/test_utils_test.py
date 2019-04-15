from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
import socket
import sys
import threading
from time import sleep

import pytest
from tornado import gen

from distributed import Scheduler, Worker, Client, config, default_client
from distributed.core import rpc
from distributed.metrics import time
from distributed.utils_test import (  # noqa: F401
    cluster,
    gen_cluster,
    inc,
    gen_test,
    wait_for_port,
    new_config,
)

from distributed.utils_test import (  # noqa: F401
    loop,
    tls_only_security,
    security,
    tls_client,
    tls_cluster,
)
from distributed.utils import get_ip


def test_bare_cluster(loop):
    with cluster(nworkers=10) as (s, _):
        pass


def test_cluster(loop):
    with cluster() as (s, [a, b]):
        with rpc(s["address"]) as s:
            ident = loop.run_sync(s.identity)
            assert ident["type"] == "Scheduler"
            assert len(ident["workers"]) == 2


@gen_cluster(client=True)
def test_gen_cluster(c, s, a, b):
    assert isinstance(c, Client)
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.ncores == {w.address: w.ncores for w in [a, b]}


@pytest.mark.skip(reason="This hangs on travis")
def test_gen_cluster_cleans_up_client(loop):
    import dask.context

    assert not dask.config.get("get", None)

    @gen_cluster(client=True)
    def f(c, s, a, b):
        assert dask.config.get("get", None)
        yield c.submit(inc, 1)

    f()

    assert not dask.config.get("get", None)


@gen_cluster(client=False)
def test_gen_cluster_without_client(s, a, b):
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.ncores == {w.address: w.ncores for w in [a, b]}


@gen_cluster(
    client=True,
    scheduler="tls://127.0.0.1",
    ncores=[("tls://127.0.0.1", 1), ("tls://127.0.0.1", 2)],
    security=tls_only_security(),
)
def test_gen_cluster_tls(e, s, a, b):
    assert isinstance(e, Client)
    assert isinstance(s, Scheduler)
    assert s.address.startswith("tls://")
    for w in [a, b]:
        assert isinstance(w, Worker)
        assert w.address.startswith("tls://")
    assert s.ncores == {w.address: w.ncores for w in [a, b]}


@gen_test()
def test_gen_test():
    yield gen.sleep(0.01)


@contextmanager
def _listen(delay=0):
    serv = socket.socket()
    serv.bind(("127.0.0.1", 0))
    e = threading.Event()

    def do_listen():
        e.set()
        sleep(delay)
        serv.listen(5)
        ret = serv.accept()
        if ret is not None:
            cli, _ = ret
            cli.close()
        serv.close()

    t = threading.Thread(target=do_listen)
    t.daemon = True
    t.start()
    try:
        e.wait()
        sleep(0.01)
        yield serv
    finally:
        t.join(5.0)


def test_wait_for_port():
    t1 = time()
    with pytest.raises(RuntimeError):
        wait_for_port((get_ip(), 9999), 0.5)
    t2 = time()
    assert t2 - t1 >= 0.5

    with _listen(0) as s1:
        t1 = time()
        wait_for_port(s1.getsockname())
        t2 = time()
        assert t2 - t1 <= 1.0

    with _listen(1) as s1:
        t1 = time()
        wait_for_port(s1.getsockname())
        t2 = time()
        assert t2 - t1 <= 2.0


def test_new_config():
    c = config.copy()
    with new_config({"xyzzy": 5}):
        config["xyzzy"] == 5

    assert config == c
    assert "xyzzy" not in config


def test_lingering_client():
    @gen_cluster()
    def f(s, a, b):
        c = yield Client(s.address, asynchronous=True)

    f()

    with pytest.raises(ValueError):
        default_client()


def test_lingering_client(loop):
    with cluster() as (s, [a, b]):
        client = Client(s["address"], loop=loop)


def test_tls_cluster(tls_client):
    tls_client.submit(lambda x: x + 1, 10).result() == 11
    assert tls_client.security


def test_tls_scheduler(security, loop):
    s = Scheduler(security=security, loop=loop)
    s.start("localhost")
    assert s.address.startswith("tls")
    s.close()


if sys.version_info >= (3, 5):
    from distributed.tests.py3_test_utils_tst import *  # noqa: F401, F403

import asyncio
import re
from operator import add, sub
from time import sleep

import pytest

pytest.importorskip("bokeh")
from tlz import first
from tornado.httpclient import AsyncHTTPClient

from distributed.client import wait
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, dec
from distributed.dashboard.components.worker import (
    StateTable,
    CrossFilter,
    CommunicatingStream,
    ExecutingTimeSeries,
    CommunicatingTimeSeries,
    SystemMonitor,
    Counters,
)


@gen_cluster(
    client=True,
    worker_kwargs={"dashboard": True},
    scheduler_kwargs={"dashboard": True},
)
async def test_routes(c, s, a, b):
    port = a.http_server.port

    future = c.submit(sleep, 1)
    await asyncio.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in ["status", "counters", "system", "profile", "profile-server"]:
        response = await http_client.fetch("http://localhost:%d/%s" % (port, suffix))
        body = response.body.decode()
        assert "bokeh" in body.lower()
        assert not re.search("href=./", body)  # no absolute links

    response = await http_client.fetch(
        "http://localhost:%d/info/main/workers.html" % s.http_server.port
    )

    assert str(port) in response.body.decode()


@gen_cluster(client=True, worker_kwargs={"dashboard": True})
async def test_simple(c, s, a, b):
    assert s.workers[a.address].services == {"dashboard": a.http_server.port}
    assert s.workers[b.address].services == {"dashboard": b.http_server.port}

    future = c.submit(sleep, 1)
    await asyncio.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in ["crossfilter", "system"]:
        response = await http_client.fetch(
            "http://localhost:%d/%s" % (a.http_server.port, suffix)
        )
        assert "bokeh" in response.body.decode().lower()


@gen_cluster(
    client=True, worker_kwargs={"dashboard": True},
)
async def test_services_kwargs(c, s, a, b):
    assert s.workers[a.address].services == {"dashboard": a.http_server.port}


@gen_cluster(client=True)
async def test_basic(c, s, a, b):
    for component in [
        StateTable,
        ExecutingTimeSeries,
        CommunicatingTimeSeries,
        CrossFilter,
        SystemMonitor,
    ]:

        aa = component(a)
        bb = component(b)

        xs = c.map(inc, range(10), workers=a.address)
        ys = c.map(dec, range(10), workers=b.address)

        def slowall(*args):
            sleep(1)

        x = c.submit(slowall, xs, ys, 1, workers=a.address)
        y = c.submit(slowall, xs, ys, 2, workers=b.address)
        await asyncio.sleep(0.1)

        aa.update()
        bb.update()

        assert len(first(aa.source.data.values())) and len(
            first(bb.source.data.values())
        )


@gen_cluster(client=True)
async def test_counters(c, s, a, b):
    pytest.importorskip("crick")
    while "tick-duration" not in a.digests:
        await asyncio.sleep(0.01)
    aa = Counters(a)

    aa.update()
    await asyncio.sleep(0.1)
    aa.update()

    start = time()
    while not len(aa.digest_sources["tick-duration"][0].data["x"]):
        await asyncio.sleep(1)
        assert time() < start + 5

    a.digests["foo"].add(1)
    a.digests["foo"].add(2)
    aa.add_digest_figure("foo")

    a.counters["bar"].add(1)
    a.counters["bar"].add(2)
    a.counters["bar"].add(2)
    aa.add_counter_figure("bar")

    for x in [aa.counter_sources.values(), aa.digest_sources.values()]:
        for y in x:
            for z in y.values():
                assert len(set(map(len, z.data.values()))) == 1


@gen_cluster(client=True)
async def test_CommunicatingStream(c, s, a, b):
    aa = CommunicatingStream(a)
    bb = CommunicatingStream(b)

    xs = c.map(inc, range(10), workers=a.address)
    ys = c.map(dec, range(10), workers=b.address)
    adds = c.map(add, xs, ys, workers=a.address)
    subs = c.map(sub, xs, ys, workers=b.address)

    await wait([adds, subs])

    aa.update()
    bb.update()

    assert len(first(aa.outgoing.data.values())) and len(
        first(bb.outgoing.data.values())
    )
    assert len(first(aa.incoming.data.values())) and len(
        first(bb.incoming.data.values())
    )


@gen_cluster(
    client=True, clean_kwargs={"threads": False}, worker_kwargs={"dashboard": True},
)
async def test_prometheus(c, s, a, b):
    pytest.importorskip("prometheus_client")

    http_client = AsyncHTTPClient()
    for suffix in ["metrics"]:
        response = await http_client.fetch(
            "http://localhost:%d/%s" % (a.http_server.port, suffix)
        )
        assert response.code == 200

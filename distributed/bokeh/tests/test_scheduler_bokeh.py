from __future__ import print_function, division, absolute_import

import json
import re
import sys
from time import sleep

import pytest

pytest.importorskip("bokeh")
from toolz import first
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from dask.core import flatten
from distributed.utils import tokey
from distributed.client import wait
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, dec, slowinc, div
from distributed.bokeh.worker import Counters, BokehWorker
from distributed.bokeh.scheduler import (
    BokehScheduler,
    SystemMonitor,
    Occupancy,
    StealingTimeSeries,
    StealingEvents,
    Events,
    TaskStream,
    TaskProgress,
    MemoryUse,
    CurrentLoad,
    ProcessingHistogram,
    NBytesHistogram,
    WorkerTable,
    GraphPlot,
    ProfileServer,
)

from distributed.bokeh import scheduler

scheduler.PROFILING = False


@pytest.mark.skipif(
    sys.version_info[0] == 2, reason="https://github.com/bokeh/bokeh/issues/5494"
)
@gen_cluster(client=True, scheduler_kwargs={"services": {("bokeh", 0): BokehScheduler}})
def test_simple(c, s, a, b):
    assert isinstance(s.services["bokeh"], BokehScheduler)
    port = s.services["bokeh"].port

    future = c.submit(sleep, 1)
    yield gen.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in [
        "system",
        "counters",
        "workers",
        "status",
        "tasks",
        "stealing",
        "graph",
        "individual-task-stream",
        "individual-progress",
        "individual-graph",
        "individual-nbytes",
        "individual-nprocessing",
        "individual-profile",
    ]:
        response = yield http_client.fetch("http://localhost:%d/%s" % (port, suffix))
        body = response.body.decode()
        assert "bokeh" in body.lower()
        assert not re.search("href=./", body)  # no absolute links

    response = yield http_client.fetch(
        "http://localhost:%d/individual-plots.json" % port
    )
    response = json.loads(response.body.decode())
    assert response


@gen_cluster(client=True, worker_kwargs=dict(services={"bokeh": BokehWorker}))
def test_basic(c, s, a, b):
    for component in [SystemMonitor, Occupancy, StealingTimeSeries]:
        ss = component(s)

        ss.update()
        data = ss.source.data
        assert len(first(data.values()))
        if component is Occupancy:
            assert all(addr.startswith("127.0.0.1:") for addr in data["bokeh_address"])


@gen_cluster(client=True)
def test_counters(c, s, a, b):
    pytest.importorskip("crick")
    while "tick-duration" not in s.digests:
        yield gen.sleep(0.01)
    ss = Counters(s)

    ss.update()
    yield gen.sleep(0.1)
    ss.update()

    start = time()
    while not len(ss.digest_sources["tick-duration"][0].data["x"]):
        yield gen.sleep(1)
        assert time() < start + 5


@gen_cluster(client=True)
def test_stealing_events(c, s, a, b):
    se = StealingEvents(s)

    futures = c.map(
        slowinc, range(100), delay=0.1, workers=a.address, allow_other_workers=True
    )

    while not b.task_state:  # will steal soon
        yield gen.sleep(0.01)

    se.update()

    assert len(first(se.source.data.values()))


@gen_cluster(client=True)
def test_events(c, s, a, b):
    e = Events(s, "all")

    futures = c.map(
        slowinc, range(100), delay=0.1, workers=a.address, allow_other_workers=True
    )

    while not b.task_state:
        yield gen.sleep(0.01)

    e.update()
    d = dict(e.source.data)
    assert sum(a == "add-worker" for a in d["action"]) == 2


@gen_cluster(client=True)
def test_task_stream(c, s, a, b):
    ts = TaskStream(s)

    futures = c.map(slowinc, range(10), delay=0.001)

    yield wait(futures)

    ts.update()
    d = dict(ts.source.data)

    assert all(len(L) == 10 for L in d.values())
    assert min(d["start"]) == 0  # zero based

    ts.update()
    d = dict(ts.source.data)
    assert all(len(L) == 10 for L in d.values())

    total = c.submit(sum, futures)
    yield wait(total)

    ts.update()
    d = dict(ts.source.data)
    assert len(set(map(len, d.values()))) == 1


@gen_cluster(client=True)
def test_task_stream_n_rectangles(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10)
    futures = c.map(slowinc, range(10), delay=0.001)
    yield wait(futures)
    ts.update()

    assert len(ts.source.data["start"]) == 10


@gen_cluster(client=True)
def test_task_stream_second_plugin(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10, clear_interval=10)
    ts.update()
    futures = c.map(inc, range(10))
    yield wait(futures)
    ts.update()

    ts2 = TaskStream(s, n_rectangles=5, clear_interval=10)
    ts2.update()


@gen_cluster(client=True)
def test_task_stream_clear_interval(c, s, a, b):
    ts = TaskStream(s, clear_interval=200)

    yield wait(c.map(inc, range(10)))
    ts.update()
    yield gen.sleep(0.010)
    yield wait(c.map(dec, range(10)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data["name"].count("inc") == 10
    assert ts.source.data["name"].count("dec") == 10

    yield gen.sleep(0.300)
    yield wait(c.map(inc, range(10, 20)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data["name"].count("inc") == 10
    assert ts.source.data["name"].count("dec") == 0


@gen_cluster(client=True)
def test_TaskProgress(c, s, a, b):
    tp = TaskProgress(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    yield wait(futures)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 1 for L in d.values())
    assert d["name"] == ["slowinc"]

    futures2 = c.map(dec, range(5))
    yield wait(futures2)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 2 for L in d.values())
    assert d["name"] == ["slowinc", "dec"]

    del futures, futures2

    while s.tasks:
        yield gen.sleep(0.01)

    tp.update()
    assert not tp.source.data["all"]


@gen_cluster(client=True)
def test_TaskProgress_empty(c, s, a, b):
    tp = TaskProgress(s)
    tp.update()

    futures = [c.submit(inc, i, key="f-" + "a" * i) for i in range(20)]
    yield wait(futures)
    tp.update()

    del futures
    while s.tasks:
        yield gen.sleep(0.01)
    tp.update()

    assert not any(len(v) for v in tp.source.data.values())


@gen_cluster(client=True)
def test_MemoryUse(c, s, a, b):
    mu = MemoryUse(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    yield wait(futures)

    mu.update()
    d = dict(mu.source.data)
    assert all(len(L) == 1 for L in d.values())
    assert d["name"] == ["slowinc"]


@gen_cluster(client=True)
def test_CurrentLoad(c, s, a, b):
    cl = CurrentLoad(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    yield wait(futures)

    cl.update()
    d = dict(cl.source.data)

    assert all(len(L) == 2 for L in d.values())
    assert all(d["nbytes"])


@gen_cluster(client=True)
def test_ProcessingHistogram(c, s, a, b):
    ph = ProcessingHistogram(s)
    ph.update()
    assert (ph.source.data["top"] != 0).sum() == 1

    futures = c.map(slowinc, range(10), delay=0.050)
    while not s.tasks:
        yield gen.sleep(0.01)

    ph.update()
    assert ph.source.data["right"][-1] > 2


@gen_cluster(client=True)
def test_NBytesHistogram(c, s, a, b):
    nh = NBytesHistogram(s)
    nh.update()
    assert (nh.source.data["top"] != 0).sum() == 1

    futures = c.map(inc, range(10))
    yield wait(futures)

    nh.update()
    assert nh.source.data["right"][-1] > 5 * 20


@gen_cluster(client=True)
def test_WorkerTable(c, s, a, b):
    wt = WorkerTable(s)
    wt.update()
    assert all(wt.source.data.values())
    assert all(len(v) == 2 for v in wt.source.data.values())

    ncores = wt.source.data["ncores"]
    assert all(ncores)


@gen_cluster(client=True)
def test_WorkerTable_custom_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    def metric_address(worker):
        return worker.address

    metrics = {"metric_port": metric_port, "metric_address": metric_address}

    for w in [a, b]:
        for name, func in metrics.items():
            w.metrics[name] = func

    yield [a.heartbeat(), b.heartbeat()]

    for w in [a, b]:
        assert s.workers[w.address].metrics["metric_port"] == w.port
        assert s.workers[w.address].metrics["metric_address"] == w.address

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    for name in metrics:
        assert name in data

    assert all(data.values())
    assert all(len(v) == 2 for v in data.values())
    my_index = data["worker"].index(a.address), data["worker"].index(b.address)
    assert [data["metric_port"][i] for i in my_index] == [a.port, b.port]
    assert [data["metric_address"][i] for i in my_index] == [a.address, b.address]


@gen_cluster(client=True)
def test_WorkerTable_different_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    b.metrics["metric_b"] = metric_port
    yield [a.heartbeat(), b.heartbeat()]

    assert s.workers[a.address].metrics["metric_a"] == a.port
    assert s.workers[b.address].metrics["metric_b"] == b.port

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    assert "metric_a" in data
    assert "metric_b" in data
    assert all(data.values())
    assert all(len(v) == 2 for v in data.values())
    my_index = data["worker"].index(a.address), data["worker"].index(b.address)
    assert [data["metric_a"][i] for i in my_index] == [a.port, None]
    assert [data["metric_b"][i] for i in my_index] == [None, b.port]


@gen_cluster(client=True)
def test_WorkerTable_metrics_with_different_metric_2(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    yield [a.heartbeat(), b.heartbeat()]

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    assert "metric_a" in data
    assert all(data.values())
    assert all(len(v) == 2 for v in data.values())
    my_index = data["worker"].index(a.address), data["worker"].index(b.address)
    assert [data["metric_a"][i] for i in my_index] == [a.port, None]


@gen_cluster(client=True, worker_kwargs={"metrics": {"my_port": lambda w: w.port}})
def test_WorkerTable_add_and_remove_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    b.metrics["metric_b"] = metric_port
    yield [a.heartbeat(), b.heartbeat()]

    assert s.workers[a.address].metrics["metric_a"] == a.port
    assert s.workers[b.address].metrics["metric_b"] == b.port

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" in wt.source.data
    assert "metric_b" in wt.source.data

    # Remove 'metric_b' from worker b
    del b.metrics["metric_b"]
    yield [a.heartbeat(), b.heartbeat()]

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" in wt.source.data

    del a.metrics["metric_a"]
    yield [a.heartbeat(), b.heartbeat()]

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" not in wt.source.data


@gen_cluster(client=True)
def test_WorkerTable_custom_metric_overlap_with_core_metric(c, s, a, b):
    def metric(worker):
        return -999

    a.metrics["executing"] = metric
    a.metrics["cpu"] = metric
    a.metrics["metric"] = metric
    yield [a.heartbeat(), b.heartbeat()]

    assert s.workers[a.address].metrics["executing"] != -999
    assert s.workers[a.address].metrics["cpu"] != -999
    assert s.workers[a.address].metrics["metric"] == -999


@gen_cluster(client=True)
def test_GraphPlot(c, s, a, b):
    gp = GraphPlot(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    yield total

    gp.update()
    assert set(map(len, gp.node_source.data.values())) == {6}
    assert set(map(len, gp.edge_source.data.values())) == {5}

    da = pytest.importorskip("dask.array")
    x = da.random.random((20, 20), chunks=(10, 10)).persist()
    y = (x + x.T) - x.mean(axis=0)
    y = y.persist()
    yield wait(y)

    gp.update()
    gp.update()

    yield c.compute((x + y).sum())

    gp.update()

    future = c.submit(inc, 10)
    future2 = c.submit(inc, future)
    yield wait(future2)
    key = future.key
    del future, future2
    while key in s.tasks:
        yield gen.sleep(0.01)

    assert "memory" in gp.node_source.data["state"]

    gp.update()
    gp.update()

    assert not all(x == "False" for x in gp.edge_source.data["visible"])


@gen_cluster(client=True)
def test_GraphPlot_clear(c, s, a, b):
    gp = GraphPlot(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    yield total

    gp.update()

    del total, futures

    while s.tasks:
        yield gen.sleep(0.01)

    gp.update()
    gp.update()

    start = time()
    while any(gp.node_source.data.values()) or any(gp.edge_source.data.values()):
        yield gen.sleep(0.1)
        gp.update()
        assert time() < start + 5


@gen_cluster(client=True, timeout=30)
def test_GraphPlot_complex(c, s, a, b):
    da = pytest.importorskip("dask.array")
    gp = GraphPlot(s)
    x = da.random.random((2000, 2000), chunks=(1000, 1000))
    y = ((x + x.T) - x.mean(axis=0)).persist()
    yield wait(y)
    gp.update()
    assert len(gp.layout.index) == len(gp.node_source.data["x"])
    assert len(gp.layout.index) == len(s.tasks)
    z = (x - y).sum().persist()
    yield wait(z)
    gp.update()
    assert len(gp.layout.index) == len(gp.node_source.data["x"])
    assert len(gp.layout.index) == len(s.tasks)
    del z
    yield gen.sleep(0.2)
    gp.update()
    assert len(gp.layout.index) == sum(
        v == "True" for v in gp.node_source.data["visible"]
    )
    assert len(gp.layout.index) == len(s.tasks)
    assert max(gp.layout.index.values()) < len(gp.node_source.data["visible"])
    assert gp.layout.next_index == len(gp.node_source.data["visible"])
    gp.update()
    assert set(gp.layout.index.values()) == set(range(len(gp.layout.index)))
    visible = gp.node_source.data["visible"]
    keys = list(map(tokey, flatten(y.__dask_keys__())))
    assert all(visible[gp.layout.index[key]] == "True" for key in keys)


@gen_cluster(client=True)
def test_GraphPlot_order(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(div, 1, 0)
    yield wait(y)

    gp = GraphPlot(s)
    gp.update()

    assert gp.node_source.data["state"][gp.layout.index[y.key]] == "erred"


@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.interval": "10ms",
        "distributed.worker.profile.cycle": "50ms",
    },
)
def test_profile_server(c, s, a, b):
    ptp = ProfileServer(s)
    ptp.trigger_update()
    yield gen.sleep(0.200)
    ptp.trigger_update()
    assert 2 < len(ptp.ts_source.data["time"]) < 20


@gen_cluster(client=True, scheduler_kwargs={"services": {("bokeh", 0): BokehScheduler}})
def test_root_redirect(c, s, a, b):
    http_client = AsyncHTTPClient()
    response = yield http_client.fetch(
        "http://localhost:%d/" % s.services["bokeh"].port
    )
    assert response.code == 200
    assert "/status" in response.effective_url

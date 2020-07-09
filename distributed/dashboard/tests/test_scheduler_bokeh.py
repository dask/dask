import asyncio
import json
import re
import ssl
import sys
from time import sleep

import pytest

pytest.importorskip("bokeh")
from bokeh.server.server import BokehTornado
from tlz import first
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

import dask
from dask.core import flatten
from distributed.utils import tokey, format_dashboard_link
from distributed.client import wait
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, dec, slowinc, div, get_cert
from distributed.dashboard.components.worker import Counters
from distributed.dashboard.scheduler import applications
from distributed.dashboard.components.scheduler import (
    SystemMonitor,
    Occupancy,
    StealingTimeSeries,
    StealingEvents,
    Events,
    TaskStream,
    TaskProgress,
    CurrentLoad,
    ProcessingHistogram,
    NBytesHistogram,
    WorkerTable,
    TaskGraph,
    ProfileServer,
    MemoryByKey,
    AggregateAction,
    ComputePerKey,
)
from distributed.dashboard import scheduler

scheduler.PROFILING = False


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_simple(c, s, a, b):
    port = s.http_server.port

    future = c.submit(sleep, 1)
    await asyncio.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in applications:
        response = await http_client.fetch("http://localhost:%d%s" % (port, suffix))
        body = response.body.decode()
        assert "bokeh" in body.lower()
        assert not re.search("href=./", body)  # no absolute links

    response = await http_client.fetch(
        "http://localhost:%d/individual-plots.json" % port
    )
    response = json.loads(response.body.decode())
    assert response


@gen_cluster(client=True, worker_kwargs={"dashboard": True})
async def test_basic(c, s, a, b):
    for component in [TaskStream, SystemMonitor, Occupancy, StealingTimeSeries]:
        ss = component(s)

        ss.update()
        data = ss.source.data
        assert len(first(data.values()))
        if component is Occupancy:
            assert all("127.0.0.1" in addr for addr in data["escaped_worker"])


@gen_cluster(client=True)
async def test_counters(c, s, a, b):
    pytest.importorskip("crick")
    while "tick-duration" not in s.digests:
        await asyncio.sleep(0.01)
    ss = Counters(s)

    ss.update()
    await asyncio.sleep(0.1)
    ss.update()

    start = time()
    while not len(ss.digest_sources["tick-duration"][0].data["x"]):
        await asyncio.sleep(1)
        assert time() < start + 5


@gen_cluster(client=True)
async def test_stealing_events(c, s, a, b):
    se = StealingEvents(s)

    futures = c.map(
        slowinc, range(100), delay=0.1, workers=a.address, allow_other_workers=True
    )

    while not b.task_state:  # will steal soon
        await asyncio.sleep(0.01)

    se.update()

    assert len(first(se.source.data.values()))


@gen_cluster(client=True)
async def test_events(c, s, a, b):
    e = Events(s, "all")

    futures = c.map(
        slowinc, range(100), delay=0.1, workers=a.address, allow_other_workers=True
    )

    while not b.task_state:
        await asyncio.sleep(0.01)

    e.update()
    d = dict(e.source.data)
    assert sum(a == "add-worker" for a in d["action"]) == 2


@gen_cluster(client=True)
async def test_task_stream(c, s, a, b):
    ts = TaskStream(s)

    futures = c.map(slowinc, range(10), delay=0.001)

    await wait(futures)

    ts.update()
    d = dict(ts.source.data)

    assert all(len(L) == 10 for L in d.values())
    assert min(d["start"]) == 0  # zero based

    ts.update()
    d = dict(ts.source.data)
    assert all(len(L) == 10 for L in d.values())

    total = c.submit(sum, futures)
    await wait(total)

    ts.update()
    d = dict(ts.source.data)
    assert len(set(map(len, d.values()))) == 1


@gen_cluster(client=True)
async def test_task_stream_n_rectangles(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10)
    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)
    ts.update()

    assert len(ts.source.data["start"]) == 10


@gen_cluster(client=True)
async def test_task_stream_second_plugin(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10, clear_interval=10)
    ts.update()
    futures = c.map(inc, range(10))
    await wait(futures)
    ts.update()

    ts2 = TaskStream(s, n_rectangles=5, clear_interval=10)
    ts2.update()


@gen_cluster(client=True)
async def test_task_stream_clear_interval(c, s, a, b):
    ts = TaskStream(s, clear_interval=200)

    await wait(c.map(inc, range(10)))
    ts.update()
    await asyncio.sleep(0.010)
    await wait(c.map(dec, range(10)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data["name"].count("inc") == 10
    assert ts.source.data["name"].count("dec") == 10

    await asyncio.sleep(0.300)
    await wait(c.map(inc, range(10, 20)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data["name"].count("inc") == 10
    assert ts.source.data["name"].count("dec") == 0


@gen_cluster(client=True)
async def test_TaskProgress(c, s, a, b):
    tp = TaskProgress(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 1 for L in d.values())
    assert d["name"] == ["slowinc"]

    futures2 = c.map(dec, range(5))
    await wait(futures2)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 2 for L in d.values())
    assert d["name"] == ["slowinc", "dec"]

    del futures, futures2

    while s.tasks:
        await asyncio.sleep(0.01)

    tp.update()
    assert not tp.source.data["all"]


@gen_cluster(client=True)
async def test_TaskProgress_empty(c, s, a, b):
    tp = TaskProgress(s)
    tp.update()

    futures = [c.submit(inc, i, key="f-" + "a" * i) for i in range(20)]
    await wait(futures)
    tp.update()

    del futures
    while s.tasks:
        await asyncio.sleep(0.01)
    tp.update()

    assert not any(len(v) for v in tp.source.data.values())


@gen_cluster(client=True)
async def test_CurrentLoad(c, s, a, b):
    cl = CurrentLoad(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)

    cl.update()
    d = dict(cl.source.data)

    assert all(len(L) == 2 for L in d.values())
    assert all(d["nbytes"])

    assert cl.cpu_figure.x_range.end == 200


@gen_cluster(client=True)
async def test_ProcessingHistogram(c, s, a, b):
    ph = ProcessingHistogram(s)
    ph.update()
    assert (ph.source.data["top"] != 0).sum() == 1

    futures = c.map(slowinc, range(10), delay=0.050)
    while not s.tasks:
        await asyncio.sleep(0.01)

    ph.update()
    assert ph.source.data["right"][-1] > 2


@gen_cluster(client=True)
async def test_NBytesHistogram(c, s, a, b):
    nh = NBytesHistogram(s)
    nh.update()
    assert (nh.source.data["top"] != 0).sum() == 1

    futures = c.map(inc, range(10))
    await wait(futures)

    nh.update()
    assert nh.source.data["right"][-1] > 5 * 20


@gen_cluster(client=True)
async def test_WorkerTable(c, s, a, b):
    wt = WorkerTable(s)
    wt.update()
    assert all(wt.source.data.values())
    assert all(
        not v or isinstance(v, (str, int, float))
        for L in wt.source.data.values()
        for v in L
    ), {type(v).__name__ for L in wt.source.data.values() for v in L}

    assert all(len(v) == 3 for v in wt.source.data.values())
    assert wt.source.data["name"][0] == "Total (2)"

    nthreads = wt.source.data["nthreads"]
    assert all(nthreads)
    assert nthreads[0] == nthreads[1] + nthreads[2]


@gen_cluster(client=True)
async def test_WorkerTable_custom_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    def metric_address(worker):
        return worker.address

    metrics = {"metric_port": metric_port, "metric_address": metric_address}

    for w in [a, b]:
        for name, func in metrics.items():
            w.metrics[name] = func

    await asyncio.gather(a.heartbeat(), b.heartbeat())

    for w in [a, b]:
        assert s.workers[w.address].metrics["metric_port"] == w.port
        assert s.workers[w.address].metrics["metric_address"] == w.address

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    for name in metrics:
        assert name in data

    assert all(data.values())
    assert all(len(v) == 3 for v in data.values())
    my_index = data["address"].index(a.address), data["address"].index(b.address)
    assert [data["metric_port"][i] for i in my_index] == [a.port, b.port]
    assert [data["metric_address"][i] for i in my_index] == [a.address, b.address]


@gen_cluster(client=True)
async def test_WorkerTable_different_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    b.metrics["metric_b"] = metric_port
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    assert s.workers[a.address].metrics["metric_a"] == a.port
    assert s.workers[b.address].metrics["metric_b"] == b.port

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    assert "metric_a" in data
    assert "metric_b" in data
    assert all(data.values())
    assert all(len(v) == 3 for v in data.values())
    my_index = data["address"].index(a.address), data["address"].index(b.address)
    assert [data["metric_a"][i] for i in my_index] == [a.port, None]
    assert [data["metric_b"][i] for i in my_index] == [None, b.port]


@gen_cluster(client=True)
async def test_WorkerTable_metrics_with_different_metric_2(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    assert "metric_a" in data
    assert all(data.values())
    assert all(len(v) == 3 for v in data.values())
    my_index = data["address"].index(a.address), data["address"].index(b.address)
    assert [data["metric_a"][i] for i in my_index] == [a.port, None]


@gen_cluster(client=True, worker_kwargs={"metrics": {"my_port": lambda w: w.port}})
async def test_WorkerTable_add_and_remove_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    b.metrics["metric_b"] = metric_port
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    assert s.workers[a.address].metrics["metric_a"] == a.port
    assert s.workers[b.address].metrics["metric_b"] == b.port

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" in wt.source.data
    assert "metric_b" in wt.source.data

    # Remove 'metric_b' from worker b
    del b.metrics["metric_b"]
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" in wt.source.data

    del a.metrics["metric_a"]
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" not in wt.source.data


@gen_cluster(client=True)
async def test_WorkerTable_custom_metric_overlap_with_core_metric(c, s, a, b):
    def metric(worker):
        return -999

    a.metrics["executing"] = metric
    a.metrics["cpu"] = metric
    a.metrics["metric"] = metric
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    assert s.workers[a.address].metrics["executing"] != -999
    assert s.workers[a.address].metrics["cpu"] != -999
    assert s.workers[a.address].metrics["metric"] == -999


@gen_cluster(client=True)
async def test_TaskGraph(c, s, a, b):
    gp = TaskGraph(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    await total

    gp.update()
    assert set(map(len, gp.node_source.data.values())) == {6}
    assert set(map(len, gp.edge_source.data.values())) == {5}
    json.dumps(gp.edge_source.data)
    json.dumps(gp.node_source.data)

    da = pytest.importorskip("dask.array")
    x = da.random.random((20, 20), chunks=(10, 10)).persist()
    y = (x + x.T) - x.mean(axis=0)
    y = y.persist()
    await wait(y)

    gp.update()
    gp.update()

    await c.compute((x + y).sum())

    gp.update()

    future = c.submit(inc, 10)
    future2 = c.submit(inc, future)
    await wait(future2)
    key = future.key
    del future, future2
    while key in s.tasks:
        await asyncio.sleep(0.01)

    assert "memory" in gp.node_source.data["state"]

    gp.update()
    gp.update()

    assert not all(x == "False" for x in gp.edge_source.data["visible"])


@gen_cluster(client=True)
async def test_TaskGraph_clear(c, s, a, b):
    gp = TaskGraph(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    await total

    gp.update()

    del total, futures

    while s.tasks:
        await asyncio.sleep(0.01)

    gp.update()
    gp.update()

    start = time()
    while any(gp.node_source.data.values()) or any(gp.edge_source.data.values()):
        await asyncio.sleep(0.1)
        gp.update()
        assert time() < start + 5


@gen_cluster(
    client=True, config={"distributed.dashboard.graph-max-items": 2,},
)
async def test_TaskGraph_limit(c, s, a, b):
    gp = TaskGraph(s)

    def func(x):
        return x

    f1 = c.submit(func, 1)
    await wait(f1)
    gp.update()
    assert len(gp.node_source.data["x"]) == 1
    f2 = c.submit(func, 2)
    await wait(f2)
    gp.update()
    assert len(gp.node_source.data["x"]) == 2
    f3 = c.submit(func, 3)
    await wait(f3)
    gp.update()
    assert len(gp.node_source.data["x"]) == 2


@gen_cluster(client=True, timeout=30)
async def test_TaskGraph_complex(c, s, a, b):
    da = pytest.importorskip("dask.array")
    gp = TaskGraph(s)
    x = da.random.random((2000, 2000), chunks=(1000, 1000))
    y = ((x + x.T) - x.mean(axis=0)).persist()
    await wait(y)
    gp.update()
    assert len(gp.layout.index) == len(gp.node_source.data["x"])
    assert len(gp.layout.index) == len(s.tasks)
    z = (x - y).sum().persist()
    await wait(z)
    gp.update()
    assert len(gp.layout.index) == len(gp.node_source.data["x"])
    assert len(gp.layout.index) == len(s.tasks)
    del z
    await asyncio.sleep(0.2)
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
async def test_TaskGraph_order(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(div, 1, 0)
    await wait(y)

    gp = TaskGraph(s)
    gp.update()

    assert gp.node_source.data["state"][gp.layout.index[y.key]] == "erred"


@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.interval": "10ms",
        "distributed.worker.profile.cycle": "50ms",
    },
)
async def test_profile_server(c, s, a, b):
    ptp = ProfileServer(s)
    start = time()
    await asyncio.sleep(0.100)
    while len(ptp.ts_source.data["time"]) < 2:
        await asyncio.sleep(0.100)
        ptp.trigger_update()
        assert time() < start + 2


@gen_cluster(
    client=True, scheduler_kwargs={"dashboard": True},
)
async def test_root_redirect(c, s, a, b):
    http_client = AsyncHTTPClient()
    response = await http_client.fetch("http://localhost:%d/" % s.http_server.port)
    assert response.code == 200
    assert "/status" in response.effective_url


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    worker_kwargs={"dashboard": True},
    timeout=180,
)
async def test_proxy_to_workers(c, s, a, b):
    try:
        import jupyter_server_proxy  # noqa: F401

        proxy_exists = True
    except ImportError:
        proxy_exists = False

    dashboard_port = s.http_server.port
    http_client = AsyncHTTPClient()
    response = await http_client.fetch("http://localhost:%d/" % dashboard_port)
    assert response.code == 200
    assert "/status" in response.effective_url

    for w in [a, b]:
        host = w.ip
        port = w.http_server.port
        proxy_url = "http://localhost:%d/proxy/%s/%s/status" % (
            dashboard_port,
            port,
            host,
        )
        direct_url = "http://localhost:%s/status" % port
        http_client = AsyncHTTPClient()
        response_proxy = await http_client.fetch(proxy_url)
        response_direct = await http_client.fetch(direct_url)

        assert response_proxy.code == 200
        if proxy_exists:
            assert b"Crossfilter" in response_proxy.body
        else:
            assert b"python -m pip install jupyter-server-proxy" in response_proxy.body
        assert response_direct.code == 200
        assert b"Crossfilter" in response_direct.body


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    config={
        "distributed.scheduler.dashboard.tasks.task-stream-length": 10,
        "distributed.scheduler.dashboard.status.task-stream-length": 10,
    },
)
async def test_lots_of_tasks(c, s, a, b):
    import tlz as toolz

    ts = TaskStream(s)
    ts.update()
    futures = c.map(toolz.identity, range(100))
    await wait(futures)

    tsp = [p for p in s.plugins if "taskstream" in type(p).__name__.lower()][0]
    assert len(tsp.buffer) == 10
    ts.update()
    assert len(ts.source.data["start"]) == 10
    assert "identity" in str(ts.source.data)

    futures = c.map(lambda x: x, range(100), pure=False)
    await wait(futures)
    ts.update()
    assert "lambda" in str(ts.source.data)


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    config={
        "distributed.scheduler.dashboard.tls.key": get_cert("tls-key.pem"),
        "distributed.scheduler.dashboard.tls.cert": get_cert("tls-cert.pem"),
        "distributed.scheduler.dashboard.tls.ca-file": get_cert("tls-ca-cert.pem"),
    },
)
async def test_https_support(c, s, a, b):
    port = s.http_server.port

    assert (
        format_dashboard_link("localhost", port) == "https://localhost:%d/status" % port
    )

    ctx = ssl.create_default_context()
    ctx.load_verify_locations(get_cert("tls-ca-cert.pem"))

    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        "https://localhost:%d/individual-plots.json" % port, ssl_options=ctx
    )
    response = json.loads(response.body.decode())

    for suffix in [
        "system",
        "counters",
        "workers",
        "status",
        "tasks",
        "stealing",
        "graph",
    ] + [url.strip("/") for url in response.values()]:
        req = HTTPRequest(
            url="https://localhost:%d/%s" % (port, suffix), ssl_options=ctx
        )
        response = await http_client.fetch(req)
        assert response.code < 300
        body = response.body.decode()
        assert not re.search("href=./", body)  # no absolute links


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_memory_by_key(c, s, a, b):
    mbk = MemoryByKey(s)

    da = pytest.importorskip("dask.array")
    x = (da.random.random((20, 20), chunks=(10, 10)) + 1).persist(optimize_graph=False)
    await x

    y = await dask.delayed(inc)(1).persist()

    mbk.update()
    assert mbk.source.data["name"] == ["add", "inc"]
    assert mbk.source.data["nbytes"] == [x.nbytes, sys.getsizeof(1)]


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_aggregate_action(c, s, a, b):
    mbk = AggregateAction(s)

    da = pytest.importorskip("dask.array")
    x = (da.ones((20, 20), chunks=(10, 10)) + 1).persist(optimize_graph=False)

    await x
    y = await dask.delayed(inc)(1).persist()
    z = (x + x.T) - x.mean(axis=0)
    await c.compute(z.sum())

    mbk.update()
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        "http://localhost:%d/individual-aggregate-time-per-action" % s.http_server.port
    )
    assert response.code == 200

    assert ("transfer") in mbk.action_source.data["names"]
    assert ("compute") in mbk.action_source.data["names"]


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_compute_per_key(c, s, a, b):
    mbk = ComputePerKey(s)

    da = pytest.importorskip("dask.array")
    x = (da.ones((20, 20), chunks=(10, 10)) + 1).persist(optimize_graph=False)

    await x
    y = await dask.delayed(inc)(1).persist()
    z = (x + x.T) - x.mean(axis=0)
    await c.compute(z.sum())

    mbk.update()
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        "http://localhost:%d/individual-compute-time-per-key" % s.http_server.port
    )
    assert response.code == 200
    assert ("sum-aggregate") in mbk.compute_source.data["names"]
    assert ("add") in mbk.compute_source.data["names"]
    assert "angles" in mbk.compute_source.data.keys()


@gen_cluster(scheduler_kwargs={"http_prefix": "foo-bar", "dashboard": True})
async def test_prefix_bokeh(s, a, b):
    prefix = "foo-bar"
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        f"http://localhost:{s.http_server.port}/{prefix}/status"
    )
    assert response.code == 200
    assert (
        f'<script type="text/javascript" src="/{prefix}/static/'
        in response.body.decode()
    )

    bokeh_app = s.http_application.applications[0]
    assert isinstance(bokeh_app, BokehTornado)
    assert bokeh_app.prefix == f"/{prefix}"

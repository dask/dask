import json
import re

import pytest

pytest.importorskip("bokeh")

from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient, HTTPClientError

from dask.sizeof import sizeof
from distributed.utils import is_valid_xml
from distributed.utils_test import gen_cluster, slowinc, inc


@gen_cluster(client=True)
async def test_connect(c, s, a, b):
    future = c.submit(lambda x: x + 1, 1)
    x = c.submit(slowinc, 1, delay=1, retries=5)
    await future
    http_client = AsyncHTTPClient()
    for suffix in [
        "info/main/workers.html",
        "info/worker/" + url_escape(a.address) + ".html",
        "info/task/" + url_escape(future.key) + ".html",
        "info/main/logs.html",
        "info/logs/" + url_escape(a.address) + ".html",
        "info/call-stack/" + url_escape(x.key) + ".html",
        "info/call-stacks/" + url_escape(a.address) + ".html",
        "json/counts.json",
        "json/identity.json",
        "json/index.html",
        "individual-plots.json",
    ]:
        response = await http_client.fetch(
            "http://localhost:%d/%s" % (s.http_server.port, suffix)
        )
        assert response.code == 200
        body = response.body.decode()
        if suffix.endswith(".json"):
            json.loads(body)
        else:
            assert is_valid_xml(body)
            assert not re.search("href=./", body)  # no absolute links


@gen_cluster(client=True, nthreads=[])
async def test_worker_404(c, s):
    http_client = AsyncHTTPClient()
    with pytest.raises(HTTPClientError) as err:
        await http_client.fetch(
            "http://localhost:%d/info/worker/unknown" % s.http_server.port
        )
    assert err.value.code == 404
    with pytest.raises(HTTPClientError) as err:
        await http_client.fetch(
            "http://localhost:%d/info/task/unknown" % s.http_server.port
        )
    assert err.value.code == 404


@gen_cluster(client=True, scheduler_kwargs={"http_prefix": "/foo", "dashboard": True})
async def test_prefix(c, s, a, b):
    http_client = AsyncHTTPClient()
    for suffix in ["foo/info/main/workers.html", "foo/json/index.html", "foo/system"]:
        response = await http_client.fetch(
            "http://localhost:%d/%s" % (s.http_server.port, suffix)
        )
        assert response.code == 200
        body = response.body.decode()
        if suffix.endswith(".json"):
            json.loads(body)
        else:
            assert is_valid_xml(body)


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_prometheus(c, s, a, b):
    pytest.importorskip("prometheus_client")
    from prometheus_client.parser import text_string_to_metric_families

    http_client = AsyncHTTPClient()

    # request data twice since there once was a case where metrics got registered multiple times resulting in
    # prometheus_client errors
    for _ in range(2):
        response = await http_client.fetch(
            "http://localhost:%d/metrics" % s.http_server.port
        )
        assert response.code == 200
        assert response.headers["Content-Type"] == "text/plain; version=0.0.4"

        txt = response.body.decode("utf8")
        families = {familiy.name for familiy in text_string_to_metric_families(txt)}
        assert "dask_scheduler_workers" in families


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_prometheus_collect_task_states(c, s, a, b):
    pytest.importorskip("prometheus_client")
    from prometheus_client.parser import text_string_to_metric_families

    http_client = AsyncHTTPClient()

    async def fetch_metrics():
        port = s.http_server.port
        response = await http_client.fetch(f"http://localhost:{port}/metrics")
        txt = response.body.decode("utf8")
        families = {
            family.name: family for family in text_string_to_metric_families(txt)
        }

        active_metrics = {
            sample.labels["state"]: sample.value
            for sample in families["dask_scheduler_tasks"].samples
        }
        forgotten_tasks = [
            sample.value
            for sample in families["dask_scheduler_tasks_forgotten"].samples
        ]
        return active_metrics, forgotten_tasks

    expected = {"memory", "released", "processing", "waiting", "no-worker", "erred"}

    # Ensure that we get full zero metrics for all states even though the
    # scheduler did nothing, yet
    assert not s.tasks
    active_metrics, forgotten_tasks = await fetch_metrics()
    assert active_metrics.keys() == expected
    assert sum(active_metrics.values()) == 0.0
    assert sum(forgotten_tasks) == 0.0

    # submit a task which should show up in the prometheus scraping
    future = c.submit(slowinc, 1, delay=0.5)

    active_metrics, forgotten_tasks = await fetch_metrics()
    assert active_metrics.keys() == expected
    assert sum(active_metrics.values()) == 1.0
    assert sum(forgotten_tasks) == 0.0

    res = await c.gather(future)
    assert res == 2

    del future
    active_metrics, forgotten_tasks = await fetch_metrics()
    assert active_metrics.keys() == expected
    assert sum(active_metrics.values()) == 0.0
    assert sum(forgotten_tasks) == 0.0


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_health(c, s, a, b):
    http_client = AsyncHTTPClient()

    response = await http_client.fetch(
        "http://localhost:%d/health" % s.http_server.port
    )
    assert response.code == 200
    assert response.headers["Content-Type"] == "text/plain"

    txt = response.body.decode("utf8")
    assert txt == "ok"


@gen_cluster(client=True)
async def test_task_page(c, s, a, b):
    future = c.submit(lambda x: x + 1, 1, workers=a.address)
    x = c.submit(inc, 1)
    await future
    http_client = AsyncHTTPClient()

    "info/task/" + url_escape(future.key) + ".html",
    response = await http_client.fetch(
        "http://localhost:%d/info/task/" % s.http_server.port
        + url_escape(future.key)
        + ".html"
    )
    assert response.code == 200
    body = response.body.decode()

    assert str(sizeof(1)) in body
    assert "int" in body
    assert a.address in body
    assert "memory" in body


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    config={
        "distributed.scheduler.dashboard.bokeh-application.allow_websocket_origin": [
            "good.invalid"
        ]
    },
)
async def test_allow_websocket_origin(c, s, a, b):
    from tornado.httpclient import HTTPRequest
    from tornado.websocket import websocket_connect

    url = (
        "ws://localhost:%d/status/ws?bokeh-protocol-version=1.0&bokeh-session-id=1"
        % s.http_server.port
    )
    with pytest.raises(HTTPClientError) as err:
        await websocket_connect(
            HTTPRequest(url, headers={"Origin": "http://evil.invalid"})
        )
    assert err.value.code == 403


@gen_cluster(client=True)
async def test_eventstream(c, s, a, b):
    from tornado.websocket import websocket_connect

    ws_client = await websocket_connect(
        "ws://localhost:%d/%s" % (s.http_server.port, "eventstream")
    )
    assert "websocket" in str(s.plugins).lower()
    ws_client.close()

from __future__ import print_function, division, absolute_import

import json
import re
import xml.etree.ElementTree

import pytest

pytest.importorskip("bokeh")

from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient

from dask.sizeof import sizeof
from distributed.utils_test import gen_cluster, slowinc, inc
from distributed.bokeh.scheduler import BokehScheduler


@gen_cluster(client=True, scheduler_kwargs={"services": {("bokeh", 0): BokehScheduler}})
def test_connect(c, s, a, b):
    future = c.submit(lambda x: x + 1, 1)
    x = c.submit(slowinc, 1, delay=1, retries=5)
    yield future
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
        response = yield http_client.fetch(
            "http://localhost:%d/%s" % (s.services["bokeh"].port, suffix)
        )
        assert response.code == 200
        body = response.body.decode()
        if suffix.endswith(".json"):
            json.loads(body)
        else:
            assert xml.etree.ElementTree.fromstring(body) is not None
            assert not re.search("href=./", body)  # no absolute links


@gen_cluster(
    client=True,
    scheduler_kwargs={"services": {("bokeh", 0): (BokehScheduler, {"prefix": "/foo"})}},
)
def test_prefix(c, s, a, b):
    http_client = AsyncHTTPClient()
    for suffix in ["foo/info/main/workers.html", "foo/json/index.html", "foo/system"]:
        response = yield http_client.fetch(
            "http://localhost:%d/%s" % (s.services["bokeh"].port, suffix)
        )
        assert response.code == 200
        body = response.body.decode()
        if suffix.endswith(".json"):
            json.loads(body)
        else:
            assert xml.etree.ElementTree.fromstring(body) is not None


@gen_cluster(
    client=True,
    check_new_threads=False,
    scheduler_kwargs={"services": {("bokeh", 0): BokehScheduler}},
)
def test_prometheus(c, s, a, b):
    pytest.importorskip("prometheus_client")
    from prometheus_client.parser import text_string_to_metric_families

    http_client = AsyncHTTPClient()

    # request data twice since there once was a case where metrics got registered multiple times resulting in
    # prometheus_client errors
    for _ in range(2):
        response = yield http_client.fetch(
            "http://localhost:%d/metrics" % s.services["bokeh"].port
        )
        assert response.code == 200
        assert response.headers["Content-Type"] == "text/plain; version=0.0.4"

        txt = response.body.decode("utf8")
        families = {familiy.name for familiy in text_string_to_metric_families(txt)}
        assert "dask_scheduler_workers" in families


@gen_cluster(
    client=True,
    check_new_threads=False,
    scheduler_kwargs={"services": {("bokeh", 0): BokehScheduler}},
)
def test_health(c, s, a, b):
    http_client = AsyncHTTPClient()

    response = yield http_client.fetch(
        "http://localhost:%d/health" % s.services["bokeh"].port
    )
    assert response.code == 200
    assert response.headers["Content-Type"] == "text/plain"

    txt = response.body.decode("utf8")
    assert txt == "ok"


@gen_cluster(client=True, scheduler_kwargs={"services": {("bokeh", 0): BokehScheduler}})
def test_task_page(c, s, a, b):
    future = c.submit(lambda x: x + 1, 1, workers=a.address)
    x = c.submit(inc, 1)
    yield future
    http_client = AsyncHTTPClient()

    "info/task/" + url_escape(future.key) + ".html",
    response = yield http_client.fetch(
        "http://localhost:%d/info/task/" % s.services["bokeh"].port
        + url_escape(future.key)
        + ".html"
    )
    assert response.code == 200
    body = response.body.decode()

    assert str(sizeof(1)) in body
    assert "int" in body
    assert a.address in body
    assert "memory" in body

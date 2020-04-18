import pytest

from tornado.httpclient import AsyncHTTPClient
from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_prometheus(c, s, a, b):
    pytest.importorskip("prometheus_client")
    from prometheus_client.parser import text_string_to_metric_families

    http_client = AsyncHTTPClient()

    # request data twice since there once was a case where metrics got registered
    # multiple times resulting in prometheus_client errors
    for _ in range(2):
        response = await http_client.fetch(
            "http://localhost:%d/metrics" % a.http_server.port
        )
        assert response.code == 200
        assert response.headers["Content-Type"] == "text/plain; version=0.0.4"

        txt = response.body.decode("utf8")
        families = {familiy.name for familiy in text_string_to_metric_families(txt)}
        assert "dask_worker_latency_seconds" in families


@gen_cluster(client=True)
async def test_health(c, s, a, b):
    http_client = AsyncHTTPClient()

    response = await http_client.fetch(
        "http://localhost:%d/health" % a.http_server.port
    )
    assert response.code == 200
    assert response.headers["Content-Type"] == "text/plain"

    txt = response.body.decode("utf8")
    assert txt == "ok"

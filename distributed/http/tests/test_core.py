from distributed.utils_test import gen_cluster
from tornado.httpclient import AsyncHTTPClient


@gen_cluster(client=True)
async def test_scheduler(c, s, a, b):
    client = AsyncHTTPClient()
    response = await client.fetch(
        "http://localhost:{}/health".format(s.http_server.port)
    )
    assert response.code == 200

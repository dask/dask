from tornado import web
from tornado.httpclient import AsyncHTTPClient, HTTPClientError
import pytest

from distributed.http.routing import RoutingApplication


class OneHandler(web.RequestHandler):
    def get(self):
        self.write("one")


class TwoHandler(web.RequestHandler):
    def get(self):
        self.write("two")


@pytest.mark.asyncio
async def test_basic():
    application = RoutingApplication([(r"/one", OneHandler),])
    two = web.Application([(r"/two", TwoHandler),])
    server = application.listen(1234)

    client = AsyncHTTPClient("http://localhost:1234")
    response = await client.fetch("http://localhost:1234/one")
    assert response.body.decode() == "one"

    with pytest.raises(HTTPClientError):
        response = await client.fetch("http://localhost:1234/two")

    application.applications.append(two)

    response = await client.fetch("http://localhost:1234/two")
    assert response.body.decode() == "two"

    application.add_handlers(".*", [(r"/three", OneHandler, {})])
    response = await client.fetch("http://localhost:1234/three")
    assert response.body.decode() == "one"

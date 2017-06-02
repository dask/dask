from distributed.tornado_client import TornadoClient
from distributed.utils_test import gen_cluster


@gen_cluster()
def test_tornado_client(s, a, b):
    c = yield TornadoClient(s.address)
    future = yield c.scatter(1)
    result = yield c.gather(future)
    assert result == 1
    yield c.shutdown()

from distributed.core import ConnectionPool
from distributed.comm import Comm
from distributed.utils_test import gen_cluster
from distributed.utils_comm import pack_data, gather_from_workers


def test_pack_data():
    data = {"x": 1}
    assert pack_data(("x", "y"), data) == (1, "y")
    assert pack_data({"a": "x", "b": "y"}, data) == {"a": 1, "b": "y"}
    assert pack_data({"a": ["x"], "b": "y"}, data) == {"a": [1], "b": "y"}


@gen_cluster(client=True)
def test_gather_from_workers_permissive(c, s, a, b):
    rpc = ConnectionPool()
    x = yield c.scatter({"x": 1}, workers=a.address)

    data, missing, bad_workers = yield gather_from_workers(
        {"x": [a.address], "y": [b.address]}, rpc=rpc
    )

    assert data == {"x": 1}
    assert list(missing) == ["y"]


class BrokenComm(Comm):
    peer_address = None
    local_address = None

    def close(self):
        pass

    def closed(self):
        pass

    def abort(self):
        pass

    def read(self, deserializers=None):
        raise EnvironmentError

    def write(self, msg, serializers=None, on_error=None):
        raise EnvironmentError


class BrokenConnectionPool(ConnectionPool):
    async def connect(self, *args, **kwargs):
        return BrokenComm()


@gen_cluster(client=True)
def test_gather_from_workers_permissive_flaky(c, s, a, b):
    x = yield c.scatter({"x": 1}, workers=a.address)

    rpc = BrokenConnectionPool()
    data, missing, bad_workers = yield gather_from_workers({"x": [a.address]}, rpc=rpc)

    assert missing == {"x": [a.address]}
    assert bad_workers == [a.address]

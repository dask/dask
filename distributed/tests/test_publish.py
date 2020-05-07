import asyncio
import pytest

from dask import delayed
from distributed import Client
from distributed.client import futures_of
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc
from distributed.utils_test import client, cluster_fixture, loop  # noqa F401
from distributed.protocol import Serialized


@gen_cluster(client=False)
async def test_publish_simple(s, a, b):
    c = Client(s.address, asynchronous=True)
    f = Client(s.address, asynchronous=True)
    await asyncio.gather(c, f)

    data = await c.scatter(range(3))
    await c.publish_dataset(data=data)
    assert "data" in s.extensions["publish"].datasets
    assert isinstance(s.extensions["publish"].datasets["data"]["data"], Serialized)

    with pytest.raises(KeyError) as exc_info:
        await c.publish_dataset(data=data)

    assert "exists" in str(exc_info.value)
    assert "data" in str(exc_info.value)

    result = await c.scheduler.publish_list()
    assert result == ("data",)

    result = await f.scheduler.publish_list()
    assert result == ("data",)

    await asyncio.gather(c.close(), f.close())


@gen_cluster(client=False)
async def test_publish_non_string_key(s, a, b):
    async with Client(s.address, asynchronous=True) as c:
        for name in [("a", "b"), 9.0, 8]:
            data = await c.scatter(range(3))
            await c.publish_dataset(data, name=name)
            assert name in s.extensions["publish"].datasets
            assert isinstance(
                s.extensions["publish"].datasets[name]["data"], Serialized
            )

            datasets = await c.scheduler.publish_list()
            assert name in datasets


@gen_cluster(client=False)
async def test_publish_roundtrip(s, a, b):
    c = await Client(s.address, asynchronous=True)
    f = await Client(s.address, asynchronous=True)

    data = await c.scatter([0, 1, 2])
    await c.publish_dataset(data=data)

    assert "published-data" in s.who_wants[data[0].key]
    result = await f.get_dataset(name="data")

    assert len(result) == len(data)
    out = await f.gather(result)
    assert out == [0, 1, 2]

    with pytest.raises(KeyError) as exc_info:
        await f.get_dataset(name="nonexistent")

    assert "not found" in str(exc_info.value)
    assert "nonexistent" in str(exc_info.value)

    await c.close()
    await f.close()


@gen_cluster(client=True)
async def test_unpublish(c, s, a, b):
    data = await c.scatter([0, 1, 2])
    await c.publish_dataset(data=data)

    key = data[0].key
    del data

    await c.scheduler.publish_delete(name="data")

    assert "data" not in s.extensions["publish"].datasets

    start = time()
    while key in s.who_wants:
        await asyncio.sleep(0.01)
        assert time() < start + 5

    with pytest.raises(KeyError) as exc_info:
        await c.get_dataset(name="data")

    assert "not found" in str(exc_info.value)
    assert "data" in str(exc_info.value)


def test_unpublish_sync(client):
    data = client.scatter([0, 1, 2])
    client.publish_dataset(data=data)
    client.unpublish_dataset(name="data")

    with pytest.raises(KeyError) as exc_info:
        client.get_dataset(name="data")

    assert "not found" in str(exc_info.value)
    assert "data" in str(exc_info.value)


@gen_cluster(client=True)
async def test_publish_multiple_datasets(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(2)

    await c.publish_dataset(x=x, y=y)
    datasets = await c.scheduler.publish_list()
    assert set(datasets) == {"x", "y"}


def test_unpublish_multiple_datasets_sync(client):
    x = delayed(inc)(1)
    y = delayed(inc)(2)
    client.publish_dataset(x=x, y=y)
    client.unpublish_dataset(name="x")

    with pytest.raises(KeyError) as exc_info:
        client.get_dataset(name="x")

    datasets = client.list_datasets()
    assert set(datasets) == {"y"}

    assert "not found" in str(exc_info.value)
    assert "x" in str(exc_info.value)

    client.unpublish_dataset(name="y")

    with pytest.raises(KeyError) as exc_info:
        client.get_dataset(name="y")

    assert "not found" in str(exc_info.value)
    assert "y" in str(exc_info.value)


@gen_cluster(client=False)
async def test_publish_bag(s, a, b):
    db = pytest.importorskip("dask.bag")
    c = await Client(s.address, asynchronous=True)
    f = await Client(s.address, asynchronous=True)

    bag = db.from_sequence([0, 1, 2])
    bagp = c.persist(bag)

    assert len(futures_of(bagp)) == 3
    keys = {f.key for f in futures_of(bagp)}
    assert keys == set(bag.dask)

    await c.publish_dataset(data=bagp)

    # check that serialization didn't affect original bag's dask
    assert len(futures_of(bagp)) == 3

    result = await f.get_dataset("data")
    assert set(result.dask.keys()) == set(bagp.dask.keys())
    assert {f.key for f in result.dask.values()} == {f.key for f in bagp.dask.values()}

    out = await f.compute(result)
    assert out == [0, 1, 2]
    await c.close()
    await f.close()


def test_datasets_setitem(client):
    for key in ["key", ("key", "key"), 1]:
        value = "value"
        client.datasets[key] = value
        assert client.get_dataset(key) == value


def test_datasets_getitem(client):
    for key in ["key", ("key", "key"), 1]:
        value = "value"
        client.publish_dataset(value, name=key)
        assert client.datasets[key] == value


def test_datasets_delitem(client):
    for key in ["key", ("key", "key"), 1]:
        value = "value"
        client.publish_dataset(value, name=key)
        del client.datasets[key]
        assert key not in client.list_datasets()


def test_datasets_keys(client):
    client.publish_dataset(**{str(n): n for n in range(10)})
    keys = list(client.datasets.keys())
    assert keys == [str(n) for n in range(10)]


def test_datasets_contains(client):
    key, value = "key", "value"
    client.publish_dataset(key=value)
    assert key in client.datasets


def test_datasets_iter(client):
    keys = [n for n in range(10)]
    client.publish_dataset(**{str(key): key for key in keys})
    for n, key in enumerate(client.datasets):
        assert key == str(n)
    with pytest.raises(TypeError):
        client.datasets.__aiter__()


@gen_cluster(client=True)
async def test_datasets_async(c, s, a, b):
    await c.publish_dataset(foo=1, bar=2)
    assert await c.datasets["foo"] == 1
    assert {k async for k in c.datasets} == {"foo", "bar"}
    with pytest.raises(TypeError):
        c.datasets["baz"] = 3
    with pytest.raises(TypeError):
        del c.datasets["foo"]
    with pytest.raises(TypeError):
        next(iter(c.datasets))
    with pytest.raises(TypeError):
        len(c.datasets)


@gen_cluster(client=True)
async def test_pickle_safe(c, s, a, b):
    async with Client(s.address, asynchronous=True, serializers=["msgpack"]) as c2:
        await c2.publish_dataset(x=[1, 2, 3])
        result = await c2.get_dataset("x")
        assert result == [1, 2, 3]

        with pytest.raises(TypeError):
            await c2.publish_dataset(y=lambda x: x)

        await c.publish_dataset(z=lambda x: x)  # this can use pickle

        with pytest.raises(TypeError):
            await c2.get_dataset("z")


@gen_cluster(client=True)
async def test_deserialize_client(c, s, a, b):
    """Test that the client attached to Futures returned by Client.get_dataset is always
    the instance of the client that invoked the method.
    Specifically:

    - when the client is defined by hostname, test that it is not accidentally
      reinitialised by IP;
    - when multiple clients are connected to the same scheduler, test that they don't
      interfere with each other.

    See: test_client.test_serialize_future
    See: https://github.com/dask/distributed/issues/3227
    """
    future = await c.scatter("123")
    await c.publish_dataset(foo=future)
    future = await c.get_dataset("foo")
    assert future.client is c

    for addr in (s.address, "localhost:" + s.address.split(":")[-1]):
        async with Client(addr, asynchronous=True) as c2:
            future = await c.get_dataset("foo")
            assert future.client is c
            future = await c2.get_dataset("foo")
            assert future.client is c2

    # Ensure cleanup
    from distributed.client import _current_client

    assert _current_client.get() is None

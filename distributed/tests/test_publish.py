
import pytest

from dask import delayed
from distributed import Client
from distributed.client import futures_of
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, cluster
from distributed.utils_test import loop # flake8: noqa
from tornado import gen


@gen_cluster(client=False)
def test_publish_simple(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)
    f = yield Client((s.ip, s.port), asynchronous=True)

    data = yield c.scatter(range(3))
    out = yield c.publish_dataset(data=data)
    assert 'data' in s.extensions['publish'].datasets

    with pytest.raises(KeyError) as exc_info:
        out = yield c.publish_dataset(data=data)

    assert "exists" in str(exc_info.value)
    assert "data" in str(exc_info.value)

    result = yield c.scheduler.publish_list()
    assert result == ['data']

    result = yield f.scheduler.publish_list()
    assert result == ['data']

    yield c.close()
    yield f.close()


@gen_cluster(client=False)
def test_publish_roundtrip(s, a, b):
    c = yield Client((s.ip, s.port), asynchronous=True)
    f = yield Client((s.ip, s.port), asynchronous=True)

    data = yield c.scatter([0, 1, 2])
    yield c.publish_dataset(data=data)

    assert 'published-data' in s.who_wants[data[0].key]
    result = yield f.get_dataset(name='data')

    assert len(result) == len(data)
    out = yield f.gather(result)
    assert out == [0, 1, 2]

    with pytest.raises(KeyError) as exc_info:
        result = yield f.get_dataset(name='nonexistent')

    assert "not found" in str(exc_info.value)
    assert "nonexistent" in str(exc_info.value)

    yield c.close()
    yield f.close()


@gen_cluster(client=True)
def test_unpublish(c, s, a, b):
    data = yield c.scatter([0, 1, 2])
    yield c.publish_dataset(data=data)

    key = data[0].key
    del data

    yield c.scheduler.publish_delete(name='data')

    assert 'data' not in s.extensions['publish'].datasets

    start = time()
    while key in s.who_wants:
        yield gen.sleep(0.01)
        assert time() < start + 5

    with pytest.raises(KeyError) as exc_info:
        result = yield c.get_dataset(name='data')

    assert "not found" in str(exc_info.value)
    assert "data" in str(exc_info.value)


def test_unpublish_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            data = c.scatter([0, 1, 2])
            c.publish_dataset(data=data)
            c.unpublish_dataset(name='data')

            with pytest.raises(KeyError) as exc_info:
                result = c.get_dataset(name='data')

            assert "not found" in str(exc_info.value)
            assert "data" in str(exc_info.value)


@gen_cluster(client=True)
def test_publish_multiple_datasets(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(2)

    yield c.publish_dataset(x=x, y=y)
    datasets = yield c.scheduler.publish_list()
    assert set(datasets) == {'x', 'y'}


def test_unpublish_multiple_datasets_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            x = delayed(inc)(1)
            y = delayed(inc)(2)
            c.publish_dataset(x=x, y=y)
            c.unpublish_dataset(name='x')

            with pytest.raises(KeyError) as exc_info:
                result = c.get_dataset(name='x')

            datasets = c.list_datasets()
            assert set(datasets) == {'y'}

            assert "not found" in str(exc_info.value)
            assert "x" in str(exc_info.value)

            c.unpublish_dataset(name='y')

            with pytest.raises(KeyError) as exc_info:
                result = c.get_dataset(name='y')

            assert "not found" in str(exc_info.value)
            assert "y" in str(exc_info.value)


@gen_cluster(client=False)
def test_publish_bag(s, a, b):
    db = pytest.importorskip('dask.bag')
    c = yield Client((s.ip, s.port), asynchronous=True)
    f = yield Client((s.ip, s.port), asynchronous=True)

    bag = db.from_sequence([0, 1, 2])
    bagp = c.persist(bag)

    assert len(futures_of(bagp)) == 3
    keys = {f.key for f in futures_of(bagp)}
    assert keys == set(bag.dask)

    yield c.publish_dataset(data=bagp)

    # check that serialization didn't affect original bag's dask
    assert len(futures_of(bagp)) == 3

    result = yield f.get_dataset('data')
    assert set(result.dask.keys()) == set(bagp.dask.keys())
    assert {f.key for f in result.dask.values()} == {f.key for f in bagp.dask.values()}

    out = yield f.compute(result)
    assert out == [0, 1, 2]
    yield c.close()
    yield f.close()


def test_datasets_setitem(loop):
    with cluster() as (s, _):
        with Client(s['address'], loop=loop) as client:
            key, value = 'key', 'value'
            client.datasets[key] = value
            assert client.get_dataset('key') == value


def test_datasets_getitem(loop):
    with cluster() as (s, _):
        with Client(s['address'], loop=loop) as client:
            key, value = 'key', 'value'
            client.publish_dataset(key=value)
            assert client.datasets[key] == value


def test_datasets_delitem(loop):
    with cluster() as (s, _):
        with Client(s['address'], loop=loop) as client:
            key, value = 'key', 'value'
            client.publish_dataset(key=value)
            del client.datasets[key]
            assert key not in client.list_datasets()


def test_datasets_keys(loop):
    with cluster() as (s, _):
        with Client(s['address'], loop=loop) as client:
            client.publish_dataset(**{str(n): n for n in range(10)})
            keys = list(client.datasets.keys())
            assert keys == [str(n) for n in range(10)]


def test_datasets_contains(loop):
    with cluster() as (s, _):
        with Client(s['address'], loop=loop) as client:
            key, value = 'key', 'value'
            client.publish_dataset(key=value)
            assert key in client.datasets


def test_datasets_iter(loop):
    with cluster() as (s, _):
        with Client(s['address'], loop=loop) as client:
            keys = [n for n in range(10)]
            client.publish_dataset(**{str(key): key for key in keys})
            for n, key in enumerate(client.datasets):
                assert key == str(n)

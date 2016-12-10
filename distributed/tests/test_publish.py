
import pytest

from dask import delayed
from distributed import Client
from distributed.client import futures_of
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc

@gen_cluster(client=False)
def test_publish_simple(s, a, b):
    c = Client((s.ip, s.port), start=False)
    yield c._start()
    f = Client((s.ip, s.port), start=False)
    yield f._start()

    data = yield c._scatter(range(3))
    out = yield c._publish_dataset(data=data)
    assert 'data' in s.extensions['publish'].datasets

    with pytest.raises(KeyError) as exc_info:
        out = yield c._publish_dataset(data=data)

    assert "exists" in str(exc_info.value)
    assert "data" in str(exc_info.value)

    result = yield c.scheduler.publish_list()
    assert result == ['data']

    result = yield f.scheduler.publish_list()
    assert result == ['data']

    yield c._shutdown()
    yield f._shutdown()


@gen_cluster(client=False)
def test_publish_roundtrip(s, a, b):
    c = Client((s.ip, s.port), start=False)
    yield c._start()
    f = Client((s.ip, s.port), start=False)
    yield f._start()

    data = yield c._scatter([0, 1, 2])
    yield c._publish_dataset(data=data)

    assert 'published-data' in s.who_wants[data[0].key]
    result = yield f._get_dataset(name='data')

    assert len(result) == len(data)
    out = yield f._gather(result)
    assert out == [0, 1, 2]

    with pytest.raises(KeyError) as exc_info:
        result = yield f._get_dataset(name='nonexistent')

    assert "not found" in str(exc_info.value)
    assert "nonexistent" in str(exc_info.value)

    yield c._shutdown()
    yield f._shutdown()


@gen_cluster(client=True)
def test_unpublish(c, s, a, b):
    data = yield c._scatter([0, 1, 2])
    yield c._publish_dataset(data=data)

    key = data[0].key
    del data

    yield c.scheduler.publish_delete(name='data')

    assert 'data' not in s.extensions['publish'].datasets

    start = time()
    while key in s.who_wants:
        yield gen.sleep(0.01)
        assert time() < start + 5

    with pytest.raises(KeyError) as exc_info:
        result = yield c._get_dataset(name='data')

    assert "not found" in str(exc_info.value)
    assert "data" in str(exc_info.value)


@gen_cluster(client=True)
def test_publish_multiple_datasets(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(2)

    yield c._publish_dataset(x=x, y=y)
    datasets = yield c.scheduler.publish_list()
    assert set(datasets) == {'x', 'y'}


@gen_cluster(client=False)
def test_publish_bag(s, a, b):
    db = pytest.importorskip('dask.bag')
    c = Client((s.ip, s.port), start=False)
    yield c._start()
    f = Client((s.ip, s.port), start=False)
    yield f._start()

    bag = db.from_sequence([0, 1, 2])
    bagp = c.persist(bag)

    assert len(futures_of(bagp)) == 3
    keys = {f.key for f in futures_of(bagp)}
    assert keys == set(bag.dask)

    yield c._publish_dataset(data=bagp)

    # check that serialization didn't affect original bag's dask
    assert len(futures_of(bagp)) == 3

    result = yield f._get_dataset('data')
    assert set(result.dask.keys()) == set(bagp.dask.keys())
    assert {f.key for f in result.dask.values()} == {f.key for f in bagp.dask.values()}

    out = yield f.compute(result)._result()
    assert out == [0, 1, 2]
    yield c._shutdown()
    yield f._shutdown()

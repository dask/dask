import pytest

distributed = pytest.importorskip("distributed")

import asyncio
from functools import partial
from operator import add

from tornado import gen

import dask
from dask import persist, delayed, compute
from dask.delayed import Delayed
from dask.utils import tmpdir, get_named_args
from distributed import futures_of
from distributed.client import wait
from distributed.utils_test import (  # noqa F401
    gen_cluster,
    inc,
    cluster,
    cluster_fixture,
    loop,
    client as c,
)


if "should_check_state" in get_named_args(gen_cluster):
    gen_cluster = partial(gen_cluster, should_check_state=False)
    cluster = partial(cluster, should_check_state=False)


def test_can_import_client():
    from dask.distributed import Client  # noqa: F401


@gen_cluster(client=True)
def test_persist(c, s, a, b):
    x = delayed(inc)(1)
    (x2,) = persist(x)

    yield wait(x2)
    assert x2.key in a.data or x2.key in b.data

    y = delayed(inc)(10)
    y2, one = persist(y, 1)

    yield wait(y2)
    assert y2.key in a.data or y2.key in b.data


def test_persist_nested(c):
    a = delayed(1) + 5
    b = a + 1
    c = a + 2
    result = persist({"a": a, "b": [1, 2, b]}, (c, 2), 4, [5])
    assert isinstance(result[0]["a"], Delayed)
    assert isinstance(result[0]["b"][2], Delayed)
    assert isinstance(result[1][0], Delayed)

    sol = ({"a": 6, "b": [1, 2, 7]}, (8, 2), 4, [5])
    assert compute(*result) == sol

    res = persist([a, b], c, 4, [5], traverse=False)
    assert res[0][0] is a
    assert res[0][1] is b
    assert res[1].compute() == 8
    assert res[2:] == (4, [5])


def test_futures_to_delayed_dataframe(c):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    df = pd.DataFrame({"x": [1, 2, 3]})

    futures = c.scatter([df, df])
    ddf = dd.from_delayed(futures)
    dd.utils.assert_eq(ddf.compute(), pd.concat([df, df], axis=0))

    with pytest.raises(TypeError):
        ddf = dd.from_delayed([1, 2])


def test_futures_to_delayed_bag(c):
    db = pytest.importorskip("dask.bag")
    L = [1, 2, 3]

    futures = c.scatter([L, L])
    b = db.from_delayed(futures)
    assert list(b) == L + L


def test_futures_to_delayed_array(c):
    da = pytest.importorskip("dask.array")
    from dask.array.utils import assert_eq

    np = pytest.importorskip("numpy")
    x = np.arange(5)

    futures = c.scatter([x, x])
    A = da.concatenate(
        [da.from_delayed(f, shape=x.shape, dtype=x.dtype) for f in futures], axis=0
    )
    assert_eq(A.compute(), np.concatenate([x, x], axis=0))


@gen_cluster(client=True)
def test_local_get_with_distributed_active(c, s, a, b):
    with dask.config.set(scheduler="sync"):
        x = delayed(inc)(1).persist()
    yield gen.sleep(0.01)
    assert not s.tasks  # scheduler hasn't done anything

    x = delayed(inc)(2).persist(scheduler="sync")  # noqa F841
    yield gen.sleep(0.01)
    assert not s.tasks  # scheduler hasn't done anything


def test_to_hdf_distributed(c):
    from ..dataframe.io.tests.test_hdf import test_to_hdf

    test_to_hdf()


@pytest.mark.parametrize(
    "npartitions",
    [
        1,
        pytest.param(
            4,
            marks=pytest.mark.xfail(reason="HDF not multi-process safe", strict=False),
        ),
        pytest.param(
            10,
            marks=pytest.mark.xfail(reason="HDF not multi-process safe", strict=False),
        ),
    ],
)
def test_to_hdf_scheduler_distributed(npartitions, c):
    from ..dataframe.io.tests.test_hdf import test_to_hdf_schedulers

    test_to_hdf_schedulers(None, npartitions)


@gen_cluster(client=True)
def test_serializable_groupby_agg(c, s, a, b):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    df = pd.DataFrame({"x": [1, 2, 3, 4], "y": [1, 0, 1, 0]})
    ddf = dd.from_pandas(df, npartitions=2)

    result = ddf.groupby("y").agg("count")

    yield c.compute(result)


def test_futures_in_graph(c):
    x, y = delayed(1), delayed(2)
    xx = delayed(add)(x, x)
    yy = delayed(add)(y, y)
    xxyy = delayed(add)(xx, yy)

    xxyy2 = c.persist(xxyy)
    xxyy3 = delayed(add)(xxyy2, 10)

    assert xxyy3.compute(scheduler="dask.distributed") == ((1 + 1) + (2 + 2)) + 10


def test_zarr_distributed_roundtrip():
    da = pytest.importorskip("dask.array")
    pytest.importorskip("zarr")
    assert_eq = da.utils.assert_eq

    with tmpdir() as d:
        a = da.zeros((3, 3), chunks=(1, 1))
        a.to_zarr(d)
        a2 = da.from_zarr(d)
        assert_eq(a, a2)
        assert a2.chunks == a.chunks


def test_zarr_in_memory_distributed_err(c):
    da = pytest.importorskip("dask.array")
    zarr = pytest.importorskip("zarr")

    c = (1, 1)
    a = da.ones((3, 3), chunks=c)
    z = zarr.zeros_like(a, chunks=c)

    with pytest.raises(RuntimeError):
        a.to_zarr(z)


def test_scheduler_equals_client(c):
    x = delayed(lambda: 1)()
    assert x.compute(scheduler=c) == 1
    assert c.run_on_scheduler(lambda dask_scheduler: dask_scheduler.story(x.key))


@gen_cluster(client=True)
async def test_await(c, s, a, b):
    x = dask.delayed(inc)(1)
    x = await x.persist()
    assert x.key in s.tasks
    assert a.data or b.data
    assert all(f.done() for f in futures_of(x))


def test_local_scheduler():
    async def f():
        x = dask.delayed(inc)(1)
        y = x + 1
        z = await y.persist()
        assert len(z.dask) == 1

    asyncio.get_event_loop().run_until_complete(f())

import asyncio
from time import time

from dask import delayed
import pytest

from distributed import Worker
from distributed.client import wait
from distributed.compatibility import WINDOWS
from distributed.utils import tokey
from distributed.utils_test import inc, gen_cluster, slowinc, slowadd
from distributed.utils_test import client, cluster_fixture, loop, s, a, b  # noqa: F401


@gen_cluster(client=True, nthreads=[])
async def test_resources(c, s):
    assert not s.worker_resources
    assert not s.resources

    a = Worker(s.address, loop=s.loop, resources={"GPU": 2})
    b = Worker(s.address, loop=s.loop, resources={"GPU": 1, "DB": 1})
    await asyncio.gather(a, b)

    assert s.resources == {"GPU": {a.address: 2, b.address: 1}, "DB": {b.address: 1}}
    assert s.worker_resources == {a.address: {"GPU": 2}, b.address: {"GPU": 1, "DB": 1}}

    await b.close()

    assert s.resources == {"GPU": {a.address: 2}, "DB": {}}
    assert s.worker_resources == {a.address: {"GPU": 2}}

    await a.close()


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 5}}),
        ("127.0.0.1", 1, {"resources": {"A": 1, "B": 1}}),
    ],
)
async def test_resource_submit(c, s, a, b):
    x = c.submit(inc, 1, resources={"A": 3})
    y = c.submit(inc, 2, resources={"B": 1})
    z = c.submit(inc, 3, resources={"C": 2})

    await wait(x)
    assert x.key in a.data

    await wait(y)
    assert y.key in b.data

    assert s.get_task_status(keys=[z.key]) == {z.key: "no-worker"}

    d = await Worker(s.address, loop=s.loop, resources={"C": 10})

    await wait(z)
    assert z.key in d.data

    await d.close()


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_submit_many_non_overlapping(c, s, a, b):
    futures = [c.submit(inc, i, resources={"A": 1}) for i in range(5)]
    await wait(futures)

    assert len(a.data) == 5
    assert len(b.data) == 0


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_move(c, s, a, b):
    [x] = await c._scatter([1], workers=b.address)

    future = c.submit(inc, x, resources={"A": 1})

    await wait(future)
    assert a.data[future.key] == 2


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_dont_work_steal(c, s, a, b):
    [x] = await c._scatter([1], workers=a.address)

    futures = [
        c.submit(slowadd, x, i, resources={"A": 1}, delay=0.05) for i in range(10)
    ]

    await wait(futures)
    assert all(f.key in a.data for f in futures)


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_map(c, s, a, b):
    futures = c.map(inc, range(10), resources={"B": 1})
    await wait(futures)
    assert set(b.data) == {f.key for f in futures}
    assert not a.data


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_persist(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    xx, yy = c.persist([x, y], resources={x: {"A": 1}, y: {"B": 1}})

    await wait([xx, yy])

    assert x.key in a.data
    assert y.key in b.data


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 11}}),
    ],
)
async def test_compute(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    yy = c.compute(y, resources={x: {"A": 1}, y: {"B": 1}})
    await wait(yy)

    assert b.data

    xs = [delayed(inc)(i) for i in range(10, 20)]
    xxs = c.compute(xs, resources={"B": 1})
    await wait(xxs)

    assert len(b.data) > 10


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_get(c, s, a, b):
    dsk = {"x": (inc, 1), "y": (inc, "x")}

    result = await c.get(dsk, "y", resources={"y": {"A": 1}}, sync=False)
    assert result == 3


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_persist_tuple(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    xx, yy = c.persist([x, y], resources={(x, y): {"A": 1}})

    await wait([xx, yy])

    assert x.key in a.data
    assert y.key in a.data
    assert not b.data


@gen_cluster(client=True)
async def test_resources_str(c, s, a, b):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    await a.set_resources(MyRes=1)

    x = dd.from_pandas(pd.DataFrame({"A": [1, 2], "B": [3, 4]}), npartitions=1)
    y = x.apply(lambda row: row.sum(), axis=1, meta=(None, "int64"))
    yy = y.persist(resources={"MyRes": 1})
    await wait(yy)

    ts_first = s.tasks[tokey(y.__dask_keys__()[0])]
    assert ts_first.resource_restrictions == {"MyRes": 1}
    ts_last = s.tasks[tokey(y.__dask_keys__()[-1])]
    assert ts_last.resource_restrictions == {"MyRes": 1}


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 4, {"resources": {"A": 2}}),
        ("127.0.0.1", 4, {"resources": {"A": 1}}),
    ],
)
async def test_submit_many_non_overlapping(c, s, a, b):
    futures = c.map(slowinc, range(100), resources={"A": 1}, delay=0.02)

    while len(a.data) + len(b.data) < 100:
        await asyncio.sleep(0.01)
        assert len(a.executing) <= 2
        assert len(b.executing) <= 1

    await wait(futures)
    assert a.total_resources == a.available_resources
    assert b.total_resources == b.available_resources


@gen_cluster(client=True, nthreads=[("127.0.0.1", 4, {"resources": {"A": 2, "B": 1}})])
async def test_minimum_resource(c, s, a):
    futures = c.map(slowinc, range(30), resources={"A": 1, "B": 1}, delay=0.02)

    while len(a.data) < 30:
        await asyncio.sleep(0.01)
        assert len(a.executing) <= 1

    await wait(futures)
    assert a.total_resources == a.available_resources


@gen_cluster(client=True, nthreads=[("127.0.0.1", 2, {"resources": {"A": 1}})])
async def test_prefer_constrained(c, s, a):
    futures = c.map(slowinc, range(1000), delay=0.1)
    constrained = c.map(inc, range(10), resources={"A": 1})

    start = time()
    await wait(constrained)
    end = time()
    assert end - start < 4
    has_what = dict(s.has_what)
    processing = dict(s.processing)
    assert len(has_what) < len(constrained) + 2  # at most two slowinc's finished
    assert s.processing[a.address]


@pytest.mark.skip(reason="")
@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 2, {"resources": {"A": 1}}),
        ("127.0.0.1", 2, {"resources": {"A": 1}}),
    ],
)
async def test_balance_resources(c, s, a, b):
    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address)
    constrained = c.map(inc, range(2), resources={"A": 1})

    await wait(constrained)
    assert any(f.key in a.data for f in constrained)  # share
    assert any(f.key in b.data for f in constrained)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 2)])
async def test_set_resources(c, s, a):
    await a.set_resources(A=2)
    assert a.total_resources["A"] == 2
    assert a.available_resources["A"] == 2
    assert s.worker_resources[a.address] == {"A": 2}

    future = c.submit(slowinc, 1, delay=1, resources={"A": 1})
    while a.available_resources["A"] == 2:
        await asyncio.sleep(0.01)

    await a.set_resources(A=3)
    assert a.total_resources["A"] == 3
    assert a.available_resources["A"] == 2
    assert s.worker_resources[a.address] == {"A": 3}


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_persist_collections(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.arange(10, chunks=(5,))
    y = x.map_blocks(lambda x: x + 1)
    z = y.map_blocks(lambda x: 2 * x)
    w = z.sum()

    ww, yy = c.persist([w, y], resources={tuple(y.__dask_keys__()): {"A": 1}})

    await wait([ww, yy])

    assert all(tokey(key) in a.data for key in y.__dask_keys__())


@pytest.mark.skip(reason="Should protect resource keys from optimization")
@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_dont_optimize_out(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.arange(10, chunks=(5,))
    y = x.map_blocks(lambda x: x + 1)
    z = y.map_blocks(lambda x: 2 * x)
    w = z.sum()

    await c.compute(w, resources={tuple(y.__dask_keys__()): {"A": 1}})

    for key in map(tokey, y.__dask_keys__()):
        assert "executing" in str(a.story(key))


@pytest.mark.xfail(reason="atop fusion seemed to break this")
@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_full_collections(c, s, a, b):
    dd = pytest.importorskip("dask.dataframe")
    df = dd.demo.make_timeseries(
        freq="60s", partition_freq="1d", start="2000-01-01", end="2000-01-31"
    )
    z = df.x + df.y  # some extra nodes in the graph

    await c.compute(z, resources={tuple(z.dask): {"A": 1}})
    assert a.log
    assert not b.log


@pytest.mark.parametrize(
    "optimize_graph",
    [
        pytest.param(
            True,
            marks=pytest.mark.xfail(
                reason="don't track resources through optimization"
            ),
        ),
        pytest.param(
            False, marks=pytest.mark.skipif(WINDOWS, reason="intermittent failure")
        ),
    ],
)
def test_collections_get(client, optimize_graph, s, a, b):
    da = pytest.importorskip("dask.array")

    async def f(dask_worker):
        await dask_worker.set_resources(**{"A": 1})

    client.run(f, workers=[a["address"]])

    x = da.random.random(100, chunks=(10,)) + 1

    x.compute(resources={tuple(x.dask): {"A": 1}}, optimize_graph=optimize_graph)

    def g(dask_worker):
        return len(dask_worker.log)

    logs = client.run(g)
    assert logs[a["address"]]
    assert not logs[b["address"]]

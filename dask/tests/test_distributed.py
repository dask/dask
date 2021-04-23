import pytest

distributed = pytest.importorskip("distributed")

import asyncio
from functools import partial
from operator import add

from tornado import gen

from distributed import futures_of
from distributed.client import wait
from distributed.utils_test import client as c  # noqa F401
from distributed.utils_test import cluster_fixture  # noqa F401
from distributed.utils_test import loop  # noqa F401
from distributed.utils_test import cluster, gen_cluster, inc, varying

import dask
import dask.bag as db
from dask import compute, delayed, persist
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer
from dask.utils import get_named_args, tmpdir

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


@pytest.mark.parametrize("fuse", [True, False])
def test_fused_blockwise_dataframe_merge(c, fuse):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # Generate two DataFrames with more partitions than
    # the `max_branch` default used for shuffling (32).
    # We need a multi-stage shuffle to cover #7178 fix.
    size = 35
    df1 = pd.DataFrame({"x": range(size), "y": range(size)})
    df2 = pd.DataFrame({"x": range(size), "z": range(size)})
    ddf1 = dd.from_pandas(df1, npartitions=size) + 10
    ddf2 = dd.from_pandas(df2, npartitions=5) + 10
    df1 += 10
    df2 += 10

    with dask.config.set({"optimization.fuse.active": fuse}):
        ddfm = ddf1.merge(ddf2, on=["x"], how="left")
        ddfm.head()  # https://github.com/dask/dask/issues/7178
        dfm = ddfm.compute().sort_values("x")
        # We call compute above since `sort_values` is not
        # supported in `dask.dataframe`
    dd.utils.assert_eq(
        dfm, df1.merge(df2, on=["x"], how="left").sort_values("x"), check_index=False
    )


def test_futures_to_delayed_bag(c):
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
    pytest.importorskip("numpy")
    pytest.importorskip("pandas")

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
    pytest.importorskip("numpy")
    pytest.importorskip("pandas")

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


@gen_cluster(client=True)
async def test_annotations_blockwise_unpack(c, s, a, b):
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")
    from dask.array.utils import assert_eq

    # A flaky doubling function -- need extra args because it is called before
    # application to establish dtype/meta.
    scale = varying([ZeroDivisionError("one"), ZeroDivisionError("two"), 2, 2])

    def flaky_double(x):
        return scale() * x

    # A reliable double function.
    def reliable_double(x):
        return 2 * x

    x = da.ones(10, chunks=(5,))

    # The later annotations should not override the earlier annotations
    with dask.annotate(retries=2):
        y = x.map_blocks(flaky_double, meta=np.array((), dtype=np.float_))
    with dask.annotate(retries=0):
        z = y.map_blocks(reliable_double, meta=np.array((), dtype=np.float_))

    with dask.config.set(optimization__fuse__active=False):
        z = await c.compute(z)

    assert_eq(z, np.ones(10) * 4.0)


@pytest.mark.parametrize(
    "io",
    [
        "ones",
        "zeros",
        "full",
    ],
)
@pytest.mark.parametrize("fuse", [True, False])
def test_blockwise_array_creation(c, io, fuse):
    np = pytest.importorskip("numpy")
    da = pytest.importorskip("dask.array")

    chunks = (5, 2)
    shape = (10, 4)

    if io == "ones":
        darr = da.ones(shape, chunks=chunks)
        narr = np.ones(shape)
    elif io == "zeros":
        darr = da.zeros(shape, chunks=chunks)
        narr = np.zeros(shape)
    elif io == "full":
        darr = da.full(shape, 10, chunks=chunks)
        narr = np.full(shape, 10)

    darr += 2
    narr += 2
    with dask.config.set({"optimization.fuse.active": fuse}):
        darr.compute()
        da.assert_eq(darr, narr)


@gen_cluster(client=True)
async def test_blockwise_numpy_args(c, s, a, b):
    """Test pack/unpack of blockwise that includes a NumPy literal argument"""
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")

    def fn(x, dt):
        assert type(dt) is np.uint16
        return x.astype(dt)

    arr = da.blockwise(
        fn, "x", da.ones(1000), "x", np.uint16(42), None, dtype=np.uint16
    )
    res = await c.compute(arr.sum(), optimize_graph=False)
    assert res == 1000


@gen_cluster(client=True)
async def test_blockwise_numpy_kwargs(c, s, a, b):
    """Test pack/unpack of blockwise that includes a NumPy literal keyword argument"""
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")

    def fn(x, dt=None):
        assert type(dt) is np.uint16
        return x.astype(dt)

    arr = da.blockwise(fn, "x", da.ones(1000), "x", dtype=np.uint16, dt=np.uint16(42))
    res = await c.compute(arr.sum(), optimize_graph=False)
    assert res == 1000


@gen_cluster(client=True)
async def test_combo_of_layer_types(c, s, a, b):
    """Check pack/unpack of a HLG that has every type of Layers!"""

    da = pytest.importorskip("dask.array")
    dd = pytest.importorskip("dask.dataframe")
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")

    def add(x, y, z, extra_arg):
        return x + y + z + extra_arg

    y = c.submit(lambda x: x, 2)
    z = c.submit(lambda x: x, 3)
    x = da.blockwise(
        add,
        "x",
        da.zeros((3,), chunks=(1,)),
        "x",
        da.ones((3,), chunks=(1,)),
        "x",
        y,
        None,
        concatenate=False,
        dtype=int,
        extra_arg=z,
    )

    df = dd.from_pandas(pd.DataFrame({"a": np.arange(3)}), npartitions=3)
    df = df.shuffle("a", shuffle="tasks")
    df = df["a"].to_dask_array()

    res = x.sum() + df.sum()
    res = await c.compute(res, optimize_graph=False)
    assert res == 21


@gen_cluster(client=True)
async def test_blockwise_concatenate(c, s, a, b):
    """Test a blockwise operation with concatenated axes"""
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")

    def f(x, y):
        da.assert_eq(y, [[0, 1, 2]])
        return x

    x = da.from_array(np.array([0, 1, 2]))
    y = da.from_array(np.array([[0, 1, 2]]))
    z = da.blockwise(
        f,
        ("i"),
        x,
        ("i"),
        y,
        ("ij"),
        dtype=x.dtype,
        concatenate=True,
    )

    await c.compute(z, optimize_graph=False)
    da.assert_eq(z, x)


@gen_cluster(client=True)
async def test_map_partitions_partition_info(c, s, a, b):
    dd = pytest.importorskip("dask.dataframe")
    pd = pytest.importorskip("pandas")

    ddf = dd.from_pandas(pd.DataFrame({"a": range(10)}), npartitions=2)
    res = await c.compute(
        ddf.map_partitions(lambda x, partition_info=None: partition_info)
    )
    assert res[0] == {"number": 0, "division": 0}
    assert res[1] == {"number": 1, "division": 5}


@gen_cluster(client=True)
async def test_annotation_pack_unpack(c, s, a, b):
    hlg = HighLevelGraph({"l1": MaterializedLayer({"n": 42})}, {"l1": set()})
    packed_hlg = hlg.__dask_distributed_pack__(c, ["n"])

    annotations = {"workers": ("alice",)}
    unpacked_hlg = HighLevelGraph.__dask_distributed_unpack__(packed_hlg, annotations)
    annotations = unpacked_hlg["annotations"]
    assert annotations == {"workers": {"n": ("alice",)}}

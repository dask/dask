import pytest

distributed = pytest.importorskip("distributed")

import asyncio
import os
import sys
from functools import partial
from operator import add

from distributed.utils_test import client as c  # noqa F401
from distributed.utils_test import cluster_fixture  # noqa F401
from distributed.utils_test import loop  # noqa F401
from distributed.utils_test import cluster, gen_cluster, inc, varying

import dask
import dask.bag as db
from dask import compute, delayed, persist
from dask.blockwise import Blockwise
from dask.delayed import Delayed
from dask.distributed import futures_of, wait
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer
from dask.utils import get_named_args, tmpdir, tmpfile

if "should_check_state" in get_named_args(gen_cluster):
    gen_cluster = partial(gen_cluster, should_check_state=False)
    cluster = partial(cluster, should_check_state=False)


# TODO: the fixture teardown for `cluster_fixture` is failing periodically with
# a PermissionError on windows only (in CI). Since this fixture lives in the
# distributed codebase and is nested within other fixtures we use, it's hard to
# patch it from the dask codebase. And since the error is during fixture
# teardown, an xfail won't cut it. As a hack, for now we skip all these tests
# on windows. See https://github.com/dask/dask/issues/8877.
pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "The teardown of distributed.utils_test.cluster_fixture "
        "fails on windows CI currently"
    ),
)


def test_can_import_client():
    from dask.distributed import Client  # noqa: F401


def test_can_import_nested_things():
    from dask.distributed.protocol import dumps  # noqa: F401


@gen_cluster(client=True)
async def test_persist(c, s, a, b):
    x = delayed(inc)(1)
    (x2,) = persist(x)

    await wait(x2)
    assert x2.key in a.data or x2.key in b.data

    y = delayed(inc)(10)
    y2, one = persist(y, 1)

    await wait(y2)
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

    # Make sure from_delayed is Blockwise
    assert isinstance(ddf.dask.layers[ddf._name], Blockwise)

    with pytest.raises(TypeError):
        ddf = dd.from_delayed([1, 2])


def test_from_delayed_dataframe(c):
    # Check that Delayed keys in the form of a tuple
    # are properly serialized in `from_delayed`
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    df = pd.DataFrame({"x": range(20)})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf = dd.from_delayed(ddf.to_delayed())
    dd.utils.assert_eq(ddf, df, scheduler=c)


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


@pytest.mark.filterwarnings(
    "ignore:Running on a single-machine scheduler when a distributed client "
    "is active might lead to unexpected results."
)
@gen_cluster(client=True)
async def test_local_get_with_distributed_active(c, s, a, b):

    with dask.config.set(scheduler="sync"):
        x = delayed(inc)(1).persist()
    await asyncio.sleep(0.01)
    assert not s.tasks  # scheduler hasn't done anything

    x = delayed(inc)(2).persist(scheduler="sync")  # noqa F841
    await asyncio.sleep(0.01)
    assert not s.tasks  # scheduler hasn't done anything


def test_to_hdf_distributed(c):
    pytest.importorskip("numpy")
    pytest.importorskip("pandas")

    from dask.dataframe.io.tests.test_hdf import test_to_hdf

    test_to_hdf()


@pytest.mark.filterwarnings(
    "ignore:Running on a single-machine scheduler when a distributed client "
    "is active might lead to unexpected results."
)
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

    from dask.dataframe.io.tests.test_hdf import test_to_hdf_schedulers

    test_to_hdf_schedulers(None, npartitions)


@gen_cluster(client=True)
async def test_serializable_groupby_agg(c, s, a, b):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    df = pd.DataFrame({"x": [1, 2, 3, 4], "y": [1, 0, 1, 0]})
    ddf = dd.from_pandas(df, npartitions=2)

    result = ddf.groupby("y").agg("count", split_out=2)

    # Check Culling and Compute
    agg0 = await c.compute(result.partitions[0])
    agg1 = await c.compute(result.partitions[1])
    dd.utils.assert_eq(
        pd.concat([agg0, agg1]),
        pd.DataFrame({"x": [2, 2], "y": [0, 1]}).set_index("y"),
    )


def test_futures_in_graph(c):
    x, y = delayed(1), delayed(2)
    xx = delayed(add)(x, x)
    yy = delayed(add)(y, y)
    xxyy = delayed(add)(xx, yy)

    xxyy2 = c.persist(xxyy)
    xxyy3 = delayed(add)(xxyy2, 10)

    assert xxyy3.compute(scheduler="dask.distributed") == ((1 + 1) + (2 + 2)) + 10


def test_zarr_distributed_roundtrip(c):
    da = pytest.importorskip("dask.array")
    pytest.importorskip("zarr")

    with tmpdir() as d:
        a = da.zeros((3, 3), chunks=(1, 1))
        a.to_zarr(d)
        a2 = da.from_zarr(d)
        da.assert_eq(a, a2, scheduler=c)
        assert a2.chunks == a.chunks


def test_zarr_in_memory_distributed_err(c):
    da = pytest.importorskip("dask.array")
    zarr = pytest.importorskip("zarr")

    chunks = (1, 1)
    a = da.ones((3, 3), chunks=chunks)
    z = zarr.zeros_like(a, chunks=chunks)

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

    asyncio.run(f())


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
@pytest.mark.parametrize("fuse", [True, False, None])
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
        dsk = dask.array.optimize(darr.dask, darr.__dask_keys__())
        # dsk should be a dict unless fuse is explicitly False
        assert isinstance(dsk, dict) == (fuse is not False)
        da.assert_eq(darr, narr, scheduler=c)


@pytest.mark.filterwarnings(
    "ignore:Running on a single-machine scheduler when a distributed client "
    "is active might lead to unexpected results."
)
@pytest.mark.parametrize(
    "io",
    ["parquet-pyarrow", "parquet-fastparquet", "csv", "hdf"],
)
@pytest.mark.parametrize("fuse", [True, False, None])
@pytest.mark.parametrize("from_futures", [True, False])
def test_blockwise_dataframe_io(c, tmpdir, io, fuse, from_futures):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # TODO: this configuration is flaky on osx in CI
    # See https://github.com/dask/dask/issues/8816
    if from_futures and sys.platform == "darwin":
        pytest.xfail("This test sometimes fails on osx in CI")

    df = pd.DataFrame({"x": [1, 2, 3] * 5, "y": range(15)})

    if from_futures:
        parts = [df.iloc[:5], df.iloc[5:10], df.iloc[10:15]]
        futs = c.scatter(parts)
        ddf0 = dd.from_delayed(futs, meta=parts[0])
    else:
        ddf0 = dd.from_pandas(df, npartitions=3)

    if io.startswith("parquet"):
        if io == "parquet-pyarrow":
            pytest.importorskip("pyarrow.parquet")
            engine = "pyarrow"
        else:
            pytest.importorskip("fastparquet")
            engine = "fastparquet"
        ddf0.to_parquet(str(tmpdir), engine=engine)
        ddf = dd.read_parquet(str(tmpdir), engine=engine)
    elif io == "csv":
        ddf0.to_csv(str(tmpdir), index=False)
        ddf = dd.read_csv(os.path.join(str(tmpdir), "*"))
    elif io == "hdf":
        pytest.importorskip("tables")
        fn = str(tmpdir.join("h5"))
        ddf0.to_hdf(fn, "/data*")
        ddf = dd.read_hdf(fn, "/data*")

    df = df[["x"]] + 10
    ddf = ddf[["x"]] + 10
    with dask.config.set({"optimization.fuse.active": fuse}):
        ddf.compute()
        dsk = dask.dataframe.optimize(ddf.dask, ddf.__dask_keys__())
        # dsk should not be a dict unless fuse is explicitly True
        assert isinstance(dsk, dict) == bool(fuse)

        dd.assert_eq(ddf, df, check_index=False)


def test_blockwise_fusion_after_compute(c):
    # See: https://github.com/dask/dask/issues/7720

    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # Simple sequence of Dask-Dataframe manipulations
    df = pd.DataFrame({"x": [1, 2, 3] * 5})
    series = dd.from_pandas(df, npartitions=2)["x"]
    result = series < 3

    # Trigger an optimization of the `series` graph
    # (which `result` depends on), then compute `result`.
    # This is essentially a test of `rewrite_blockwise`.
    series_len = len(series)
    assert series_len == 15
    assert df.x[result.compute()].sum() == 15


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


def test_blockwise_different_optimization(c):
    # Regression test for incorrect results due to SubgraphCallable.__eq__
    # not correctly handling subgraphs with the same outputs and arity but
    # different internals (GH-7632). The bug is triggered by distributed
    # because it uses a function cache.
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")

    u = da.from_array(np.arange(3))
    v = da.from_array(np.array([10 + 2j, 7 - 3j, 8 + 1j]))
    cv = v.conj()
    x = u * cv
    (cv,) = dask.optimize(cv)
    y = u * cv
    expected = np.array([0 + 0j, 7 + 3j, 16 - 2j])
    with dask.config.set({"optimization.fuse.active": False}):
        x_value = x.compute()
        y_value = y.compute()
    np.testing.assert_equal(x_value, expected)
    np.testing.assert_equal(y_value, expected)


def test_blockwise_cull_allows_numpy_dtype_keys(c):
    # Regression test for https://github.com/dask/dask/issues/9072
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")

    # Create a multi-block array.
    x = da.ones((100, 100), chunks=(10, 10))

    # Make a layer that pulls a block out of the array, but
    # refers to that block using a numpy.int64 for the key rather
    # than a python int.
    name = next(iter(x.dask.layers))
    block = {("block", 0, 0): (name, np.int64(0), np.int64(1))}
    dsk = HighLevelGraph.from_collections("block", block, [x])
    arr = da.Array(dsk, "block", ((10,), (10,)), dtype=x.dtype)

    # Stick with high-level optimizations to force serialization of
    # the blockwise layer.
    with dask.config.set({"optimization.fuse.active": False}):
        da.assert_eq(np.ones((10, 10)), arr, scheduler=c)


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


def test_blockwise_concatenate(c):
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
    c.compute(z, optimize_graph=False)
    da.assert_eq(z, x, scheduler=c)


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
async def test_futures_in_subgraphs(c, s, a, b):
    """Copied from distributed (tests/test_client.py)"""

    dd = pytest.importorskip("dask.dataframe")
    pd = pytest.importorskip("pandas")

    ddf = dd.from_pandas(
        pd.DataFrame(
            dict(
                uid=range(50),
                enter_time=pd.date_range(
                    start="2020-01-01", end="2020-09-01", periods=50, tz="UTC"
                ),
            )
        ),
        npartitions=1,
    )

    ddf = ddf[ddf.uid.isin(range(29))].persist()
    ddf["day"] = ddf.enter_time.dt.day_name()
    ddf = await c.submit(dd.categorical.categorize, ddf, columns=["day"], index=False)


@pytest.mark.flaky(reruns=5, reruns_delay=5)
@gen_cluster(client=True)
async def test_shuffle_priority(c, s, a, b):
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    dd = pytest.importorskip("dask.dataframe")

    # Test marked as "flaky" since the scheduling behavior
    # is not deterministic. Note that the test is still
    # very likely to fail every time if the "split" tasks
    # are not prioritized correctly

    df = pd.DataFrame({"a": range(1000)})
    ddf = dd.from_pandas(df, npartitions=10)
    ddf2 = ddf.shuffle("a", shuffle="tasks", max_branch=32)
    await c.compute(ddf2)

    # Parse transition log for processing tasks
    log = [
        eval(l[0])[0]
        for l in s.transition_log
        if l[1] == "processing" and "simple-shuffle-" in l[0]
    ]

    # Make sure most "split" tasks are processing before
    # any "combine" tasks begin
    late_split = np.quantile(
        [i for i, st in enumerate(log) if st.startswith("split")], 0.75
    )
    early_combine = np.quantile(
        [i for i, st in enumerate(log) if st.startswith("simple")], 0.25
    )
    assert late_split < early_combine


@gen_cluster(client=True)
async def test_map_partitions_da_input(c, s, a, b):
    """Check that map_partitions can handle a dask array input"""
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    da = pytest.importorskip("dask.array")
    datasets = pytest.importorskip("dask.datasets")

    def f(d, a):
        assert isinstance(d, pd.DataFrame)
        assert isinstance(a, np.ndarray)
        return d

    df = datasets.timeseries(freq="1d").persist()
    arr = da.ones((1,), chunks=1).persist()
    await c.compute(df.map_partitions(f, arr, meta=df._meta))


def test_map_partitions_df_input():
    """
    Check that map_partitions can handle a delayed
    partition of a dataframe input
    """
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    def f(d, a):
        assert isinstance(d, pd.DataFrame)
        assert isinstance(a, pd.DataFrame)
        return d

    def main():
        item_df = dd.from_pandas(pd.DataFrame({"a": range(10)}), npartitions=1)
        ddf = item_df.to_delayed()[0].persist()
        merged_df = dd.from_pandas(pd.DataFrame({"b": range(10)}), npartitions=1)

        # Notice, we include a shuffle in order to trigger a complex culling
        merged_df = merged_df.shuffle(on="b")

        merged_df.map_partitions(
            f, ddf, meta=merged_df, enforce_metadata=False
        ).compute()

    with distributed.LocalCluster(
        scheduler_port=0,
        dashboard_address=":0",
        asynchronous=False,
        n_workers=1,
        nthreads=1,
        processes=False,
    ) as cluster:
        with distributed.Client(cluster, asynchronous=False):
            main()


@gen_cluster(client=True)
async def test_annotation_pack_unpack(c, s, a, b):
    hlg = HighLevelGraph({"l1": MaterializedLayer({"n": 42})}, {"l1": set()})

    annotations = {"workers": ("alice",)}
    packed_hlg = hlg.__dask_distributed_pack__(c, ["n"], annotations)

    unpacked_hlg = HighLevelGraph.__dask_distributed_unpack__(packed_hlg)
    annotations = unpacked_hlg["annotations"]
    assert annotations == {"workers": {"n": ("alice",)}}


@gen_cluster(client=True)
async def test_pack_MaterializedLayer_handles_futures_in_graph_properly(c, s, a, b):
    fut = c.submit(inc, 1)

    hlg = HighLevelGraph(
        {"l1": MaterializedLayer({"x": fut, "y": (inc, "x"), "z": (inc, "y")})},
        {"l1": set()},
    )
    # fill hlg.key_dependencies cache. This excludes known futures, so only
    # includes a subset of all dependencies. Previously if the cache was present
    # the future dependencies would be missing when packed.
    hlg.get_all_dependencies()
    packed = hlg.__dask_distributed_pack__(c, ["z"], {})
    unpacked = HighLevelGraph.__dask_distributed_unpack__(packed)
    assert unpacked["deps"] == {"x": {fut.key}, "y": {fut.key}, "z": {"y"}}


@pytest.mark.filterwarnings(
    "ignore:Running on a single-machine scheduler when a distributed client "
    "is active might lead to unexpected results."
)
@gen_cluster(client=True)
async def test_to_sql_engine_kwargs(c, s, a, b):
    # https://github.com/dask/dask/issues/8738
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    pytest.importorskip("sqlalchemy")

    df = pd.DataFrame({"a": range(10), "b": range(10)})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=1)
    with tmpfile() as f:
        uri = f"sqlite:///{f}"
        result = ddf.to_sql(
            "test", uri, index=True, engine_kwargs={"echo": False}, compute=False
        )
        await c.compute(result)

        dd.utils.assert_eq(
            ddf,
            dd.read_sql_table("test", uri, "index"),
            check_divisions=False,
        )


@gen_cluster(client=True)
async def test_non_recursive_df_reduce(c, s, a, b):
    # See https://github.com/dask/dask/issues/8773

    dd = pytest.importorskip("dask.dataframe")
    pd = pytest.importorskip("pandas")

    class SomeObject:
        def __init__(self, val):
            self.val = val

    N = 170
    series = pd.Series(data=[1] * N, index=range(2, N + 2))
    dask_series = dd.from_pandas(series, npartitions=34)
    result = dask_series.reduction(
        chunk=lambda x: x,
        aggregate=lambda x: SomeObject(x.sum().sum()),
        split_every=False,
        token="commit-dataset",
        meta=object,
    )

    assert (await c.compute(result)).val == 170


def test_set_index_no_resursion_error(c):
    # see: https://github.com/dask/dask/issues/8955
    pytest.importorskip("dask.dataframe")
    try:
        ddf = (
            dask.datasets.timeseries(start="2000-01-01", end="2000-07-01", freq="12h")
            .reset_index()
            .astype({"timestamp": str})
        )
        ddf = ddf.set_index("timestamp", sorted=True)
        ddf.compute()
    except RecursionError:
        pytest.fail("dd.set_index triggered a recursion error")


@pytest.mark.xfail(reason="https://github.com/dask/dask/issues/8991", strict=True)
@gen_cluster(client=True)
async def test_gh_8991(c, s, a, b):
    # Test illustrating something amiss with HighLevelGraph.key_dependencies.
    # The intention is for this to be a cache, so if we clear it, things
    # should still work.

    # This is a bad test, and we should rethink/remove it as soon as the issue is
    # resolved whether, it's fixing the underlying problem or removing
    # HighLevelGraph.key_dependencies alltogether.
    datasets = pytest.importorskip("dask.datasets")
    result = datasets.timeseries().shuffle("x").to_orc("tmp", compute=False)

    # Create a dsk and mock sending it to the scheduler.
    dsk_opt = result.__dask_optimize__(result.dask, result.key)
    unpacked = HighLevelGraph.__dask_distributed_unpack__(
        dsk_opt.__dask_distributed_pack__(c, result.key)
    )
    deps = unpacked["deps"]

    # Create a version of the dsk without the key_dependencies and mock sending it to
    # the scheduler as well.
    dsk_opt_nokeys = dsk_opt.copy()
    dsk_opt_nokeys.key_dependencies.clear()
    unpacked_nokeys = HighLevelGraph.__dask_distributed_unpack__(
        dsk_opt_nokeys.__dask_distributed_pack__(c, result.key)
    )
    deps_nokeys = unpacked_nokeys["deps"]

    # The recalculated dependencies should still be the same!
    assert deps == deps_nokeys

import os

import pytest

distributed = pytest.importorskip("distributed")

from operator import getitem

from distributed import Client
from distributed.utils_test import cluster, loop  # noqa F401

from dask import config
from dask.layers import ArrayChunkShapeDep, ArraySliceDep, fractional_slice


def test_array_chunk_shape_dep():
    dac = pytest.importorskip("dask.array.core")
    d = 2  # number of chunks in x,y
    chunk = (2, 3)  # chunk shape
    shape = tuple(d * n for n in chunk)  # array shape
    chunks = dac.normalize_chunks(chunk, shape)
    array_deps = ArrayChunkShapeDep(chunks)

    def check(i, j):
        chunk_shape = array_deps[(i, j)]
        assert chunk_shape == chunk

    for i in range(d):
        for j in range(d):
            check(i, j)


def test_array_slice_deps():
    dac = pytest.importorskip("dask.array.core")
    d = 2  # number of chunks in x,y
    chunk = (2, 3)  # chunk shape
    shape = tuple(d * n for n in chunk)  # array shape
    chunks = dac.normalize_chunks(chunk, shape)
    array_deps = ArraySliceDep(chunks)

    def check(i, j):
        slices = array_deps[(i, j)]
        assert slices == (
            slice(chunk[0] * i, chunk[0] * (i + 1), None),
            slice(chunk[1] * j, chunk[1] * (j + 1), None),
        )

    for i in range(d):
        for j in range(d):
            check(i, j)


def _dataframe_shuffle(tmpdir):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # Perform a computation using an HLG-based shuffle
    df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
    return dd.from_pandas(df, npartitions=2).shuffle("a", shuffle="tasks")


def _dataframe_tree_reduction(tmpdir):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # Perform a computation using an HLG-based tree reduction
    df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
    return dd.from_pandas(df, npartitions=2).mean()


def _dataframe_broadcast_join(tmpdir):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # Perform a computation using an HLG-based broadcast join
    df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
    ddf1 = dd.from_pandas(df, npartitions=4)
    ddf2 = dd.from_pandas(df, npartitions=2)
    return ddf1.merge(ddf2, how="left", broadcast=True, shuffle="tasks")


def _array_creation(tmpdir):
    da = pytest.importorskip("dask.array")

    # Perform a computation using HLG-based array creation
    return da.ones((100,)) + da.zeros((100,))


def _array_map_overlap(tmpdir):
    da = pytest.importorskip("dask.array")
    array = da.ones((100,))
    return array.map_overlap(lambda x: x, depth=1, boundary="none")


def test_fractional_slice():
    assert fractional_slice(("x", 4.9), {0: 2}) == (getitem, ("x", 5), (slice(0, 2),))

    assert fractional_slice(("x", 3, 5.1), {0: 2, 1: 3}) == (
        getitem,
        ("x", 3, 5),
        (slice(None, None, None), slice(-3, None)),
    )

    assert fractional_slice(("x", 2.9, 5.1), {0: 2, 1: 3}) == (
        getitem,
        ("x", 3, 5),
        (slice(0, 2), slice(-3, None)),
    )

    fs = fractional_slice(("x", 4.9), {0: 2})
    assert isinstance(fs[1][1], int)


def _pq_pyarrow(tmpdir):
    pytest.importorskip("pyarrow.parquet")
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    try:
        import pyarrow.dataset as pa_ds
    except ImportError:
        # PyArrow version too old for Dataset API
        pa_ds = None

    dd.from_pandas(pd.DataFrame({"a": range(10)}), npartitions=2,).to_parquet(
        str(tmpdir),
        engine="pyarrow",
    )
    filters = [(("a", "<=", 2))]

    ddf1 = dd.read_parquet(str(tmpdir), engine="pyarrow", filters=filters)
    if pa_ds:
        # Need to test that layer serialization succeeds
        # with "pyarrow-dataset" filtering
        ddf2 = dd.read_parquet(
            str(tmpdir),
            engine="pyarrow-dataset",
            filters=filters,
        )
        return (ddf1, ddf2)
    else:
        return ddf1


def _pq_fastparquet(tmpdir):
    pytest.importorskip("fastparquet")
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    dd.from_pandas(pd.DataFrame({"a": range(10)}), npartitions=2,).to_parquet(
        str(tmpdir),
        engine="fastparquet",
    )
    return dd.read_parquet(str(tmpdir), engine="fastparquet")


def _read_csv(tmpdir):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    dd.from_pandas(
        pd.DataFrame({"a": range(10)}),
        npartitions=2,
    ).to_csv(str(tmpdir))
    return dd.read_csv(os.path.join(str(tmpdir), "*"))


@pytest.mark.parametrize(
    "op",
    [
        _dataframe_shuffle,
        _dataframe_tree_reduction,
        _dataframe_broadcast_join,
        _pq_pyarrow,
        _pq_fastparquet,
        _read_csv,
        _array_creation,
        _array_map_overlap,
    ],
)
@pytest.mark.parametrize("optimize_graph", [True, False])
@pytest.mark.parametrize("allow_pickle", [True, False])
def test_scheduler_highlevel_graph_unpack_pickle(
    op, optimize_graph, loop, tmpdir, allow_pickle
):
    # Test common HLG-Layer execution with both True and False
    # settings for the "distributed.scheduler.pickle" config
    with cluster() as (scheduler, workers):
        with Client(scheduler["address"], loop=loop) as c:
            with config.set({"distributed.scheduler.pickle": allow_pickle}):
                # Perform a computation using a HighLevelGraph Layer
                c.compute(op(tmpdir), optimize_graph=optimize_graph)

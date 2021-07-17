import os
import pathlib
import tempfile
from time import sleep

import numpy as np
import pandas as pd
import pytest

import dask
import dask.dataframe as dd
from dask.dataframe._compat import tm
from dask.dataframe.optimize import optimize_dataframe_getitem
from dask.dataframe.utils import assert_eq
from dask.layers import DataFrameIOLayer
from dask.utils import dependency_depth


def test_to_hdf(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    a = dd.from_pandas(df, 2)

    fn_1 = os.path.join(tmp_path, "fn_1.h5")
    a.to_hdf(fn_1, "/data")
    out = pd.read_hdf(fn_1, "/data")
    tm.assert_frame_equal(df, out[:])

    fn_2 = os.path.join(tmp_path, "fn_2.h5")
    a.x.to_hdf(fn_2, "/data")
    out = pd.read_hdf(fn_2, "/data")
    tm.assert_series_equal(df.x, out[:])

    a = dd.from_pandas(df, 1)
    fn_3 = os.path.join(tmp_path, "fn_3.h5")
    a.to_hdf(fn_3, "/data")
    out = pd.read_hdf(fn_3, "/data")
    tm.assert_frame_equal(df, out[:])

    # test compute = False
    fn_4 = os.path.join(tmp_path, "fn_4.h5")
    r = a.to_hdf(fn_4, "/data", compute=False)
    r.compute()
    out = pd.read_hdf(fn_4, "/data")
    tm.assert_frame_equal(df, out[:])


def test_to_hdf_multiple_nodes(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    a = dd.from_pandas(df, 2)
    df16 = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
            ],
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        },
        index=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            15.0,
            16.0,
        ],
    )
    b = dd.from_pandas(df16, 16)

    # saving to multiple nodes
    fn_1 = os.path.join(tmp_path, "fn_1.h5")
    a.to_hdf(fn_1, "/data*")
    out = dd.read_hdf(fn_1, "/data*")
    assert_eq(df, out)

    # saving to multiple nodes making sure order is kept
    fn_2 = os.path.join(tmp_path, "fn_2.h5")
    b.to_hdf(fn_2, "/data*")
    out = dd.read_hdf(fn_2, "/data*")
    assert_eq(df16, out)

    # saving to multiple datasets with custom name_function
    fn_3 = os.path.join(tmp_path, "fn_3.h5")
    a.to_hdf(fn_3, "/data_*", name_function=lambda i: "a" * (i + 1))
    out = dd.read_hdf(fn_3, "/data_*")
    assert_eq(df, out)

    out = pd.read_hdf(fn_3, "/data_a")
    tm.assert_frame_equal(out, df.iloc[:2])
    out = pd.read_hdf(fn_3, "/data_aa")
    tm.assert_frame_equal(out, df.iloc[2:])

    # test multiple nodes with hdf object
    fn_4 = os.path.join(tmp_path, "fn_4.h5")
    with pd.HDFStore(fn_4) as hdf:
        b.to_hdf(hdf, "/data*")
    out = dd.read_hdf(fn_4, "/data*")
    assert_eq(df16, out)

    # Test getitem optimization
    fn_5 = os.path.join(tmp_path, "fn_5.h5")
    a.to_hdf(fn_5, "/data*")
    out = dd.read_hdf(fn_5, "/data*")[["x"]]
    dsk = optimize_dataframe_getitem(out.dask, keys=out.__dask_keys__())
    read = [key for key in dsk.layers if key.startswith("read-hdf")][0]
    subgraph = dsk.layers[read]
    assert isinstance(subgraph, DataFrameIOLayer)
    assert subgraph.columns == ["x"]


def test_to_hdf_multiple_files(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    a = dd.from_pandas(df, 2)
    df16 = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
            ],
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        },
        index=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            15.0,
            16.0,
        ],
    )
    b = dd.from_pandas(df16, 16)

    # saving to multiple files
    with tempfile.TemporaryDirectory() as tmpdir:
        fn = os.path.join(tmpdir, "data_*.h5")
        a.to_hdf(fn, "/data")
        out = dd.read_hdf(fn, "/data")
        assert_eq(df, out)

    # saving to multiple files making sure order is kept
    with tempfile.TemporaryDirectory() as tmpdir:
        fn = os.path.join(tmpdir, "data_*.h5")
        b.to_hdf(fn, "/data")
        out = dd.read_hdf(fn, "/data")
        assert_eq(df16, out)

    # saving to multiple files with custom name_function
    with tempfile.TemporaryDirectory() as tmpdir:
        fn = os.path.join(tmpdir, "data_*.h5")
        a.to_hdf(fn, "/data", name_function=lambda i: "a" * (i + 1))
        out = dd.read_hdf(fn, "/data")
        assert_eq(df, out)

        out = pd.read_hdf(os.path.join(tmpdir, "data_a.h5"), "/data")
        tm.assert_frame_equal(out, df.iloc[:2])
        out = pd.read_hdf(os.path.join(tmpdir, "data_aa.h5"), "/data")
        tm.assert_frame_equal(out, df.iloc[2:])

    # test hdf object
    fn = tmp_path / "foo.h5"
    with pd.HDFStore(fn) as hdf:
        a.to_hdf(hdf, "/data*")
    out = dd.read_hdf(fn, "/data*")
    assert_eq(df, out)


def test_to_hdf_modes_multiple_nodes(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )

    # appending a single partition to existing data
    a = dd.from_pandas(df, 1)
    fn_1 = os.path.join(tmp_path, "fn_1.h5")
    a.to_hdf(fn_1, "/data2")
    a.to_hdf(fn_1, "/data*", mode="a")
    out = dd.read_hdf(fn_1, "/data*")
    assert_eq(df.append(df), out)

    # overwriting a file with a single partition
    a = dd.from_pandas(df, 1)
    fn_2 = os.path.join(tmp_path, "fn_2.h5")
    a.to_hdf(fn_2, "/data2")
    a.to_hdf(fn_2, "/data*", mode="w")
    out = dd.read_hdf(fn_2, "/data*")
    assert_eq(df, out)

    # appending two partitions to existing data
    a = dd.from_pandas(df, 2)
    fn_3 = os.path.join(tmp_path, "fn_3.h5")
    a.to_hdf(fn_3, "/data2")
    a.to_hdf(fn_3, "/data*", mode="a")
    out = dd.read_hdf(fn_3, "/data*")
    assert_eq(df.append(df), out)

    # overwriting a file with two partitions
    a = dd.from_pandas(df, 2)
    fn_4 = os.path.join(tmp_path, "fn_4.h5")
    a.to_hdf(fn_4, "/data2")
    a.to_hdf(fn_4, "/data*", mode="w")
    out = dd.read_hdf(fn_4, "/data*")
    assert_eq(df, out)

    # overwriting a single partition, keeping other partitions
    a = dd.from_pandas(df, 2)
    fn_5 = os.path.join(tmp_path, "fn_5.h5")
    a.to_hdf(fn_5, "/data1")
    a.to_hdf(fn_5, "/data2")
    a.to_hdf(fn_5, "/data*", mode="a", append=False)
    out = dd.read_hdf(fn_5, "/data*")
    assert_eq(df.append(df), out)


def test_to_hdf_modes_multiple_files():
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )

    # appending a single partition to existing data
    a = dd.from_pandas(df, 1)
    with tempfile.TemporaryDirectory() as dn:
        fn = os.path.join(dn, "data*")
        a.to_hdf(os.path.join(dn, "data2"), "/data")
        a.to_hdf(fn, "/data", mode="a")
        out = dd.read_hdf(fn, "/data*")
        assert_eq(df.append(df), out)

    # appending two partitions to existing data
    a = dd.from_pandas(df, 2)
    with tempfile.TemporaryDirectory() as dn:
        fn = os.path.join(dn, "data*")
        a.to_hdf(os.path.join(dn, "data2"), "/data")
        a.to_hdf(fn, "/data", mode="a")
        out = dd.read_hdf(fn, "/data")
        assert_eq(df.append(df), out)

    # overwriting a file with two partitions
    a = dd.from_pandas(df, 2)
    with tempfile.TemporaryDirectory() as dn:
        fn = os.path.join(dn, "data*")
        a.to_hdf(os.path.join(dn, "data1"), "/data")
        a.to_hdf(fn, "/data", mode="w")
        out = dd.read_hdf(fn, "/data")
        assert_eq(df, out)

    # overwriting a single partition, keeping other partitions
    a = dd.from_pandas(df, 2)
    with tempfile.TemporaryDirectory() as dn:
        fn = os.path.join(dn, "data*")
        a.to_hdf(os.path.join(dn, "data1"), "/data")
        a.to_hdf(fn, "/data", mode="a", append=False)
        out = dd.read_hdf(fn, "/data")
        assert_eq(df.append(df), out)


def test_to_hdf_link_optimizations(tmp_path):
    """testing dask link levels is correct by calculating the depth of the dask graph"""
    pytest.importorskip("tables")
    df16 = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
            ],
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        },
        index=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            15.0,
            16.0,
        ],
    )
    a = dd.from_pandas(df16, 16)

    # saving to multiple hdf files, no links are needed
    # expected layers: from_pandas, to_hdf, list = depth of 3
    fn = os.path.join(tmp_path, "data*")
    d = a.to_hdf(fn, "/data", compute=False)
    assert dependency_depth(d.dask) == 3

    # saving to a single hdf file with multiple nodes
    # all subsequent nodes depend on the first
    # expected layers: from_pandas, first to_hdf(creates file+node), subsequent to_hdfs, list = 4
    fn = os.path.join(tmp_path, "fn1")
    d = a.to_hdf(fn, "/data*", compute=False)
    assert dependency_depth(d.dask) == 4

    # saving to a single hdf file with a single node
    # every node depends on the previous node
    # expected layers: from_pandas, to_hdf times npartitions(15), list = 2 + npartitions = 17
    fn = os.path.join(tmp_path, "fn2")
    d = a.to_hdf(fn, "/data", compute=False)
    assert dependency_depth(d.dask) == 2 + a.npartitions


# @pytest.mark.slow
def test_to_hdf_lock_delays(tmp_path):
    pytest.importorskip("tables")
    df16 = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
            ],
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        },
        index=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            15.0,
            16.0,
        ],
    )
    a = dd.from_pandas(df16, 16)

    # adding artificial delays to make sure last tasks finish first
    # that's a way to simulate last tasks finishing last
    def delayed_nop(i):
        if i[1] < 10:
            sleep(0.1 * (10 - i[1]))
        return i

    # saving to multiple hdf nodes
    a = a.apply(delayed_nop, axis=1, meta=a)
    fn = tmp_path / "foo.hdf"
    a.to_hdf(fn, "/data*")
    out = dd.read_hdf(fn, "/data*")
    assert_eq(df16, out)

    # saving to multiple hdf files
    # adding artificial delays to make sure last tasks finish first
    a = a.apply(delayed_nop, axis=1, meta=a)
    with tempfile.TemporaryDirectory() as dn:
        fn = os.path.join(dn, "data*")
        a.to_hdf(fn, "/data")
        out = dd.read_hdf(fn, "/data")
        assert_eq(df16, out)


def test_to_hdf_exceptions(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    a = dd.from_pandas(df, 1)

    # triggering too many asterisks error
    with pytest.raises(ValueError):
        fn = os.path.join(tmp_path, "data_*.h5")
        a.to_hdf(fn, "/data_*")

    # triggering too many asterisks error
    with pd.HDFStore(tmp_path / "foo.h5") as hdf:
        with pytest.raises(ValueError):
            a.to_hdf(hdf, "/data_*_*")


@pytest.mark.parametrize("scheduler", ["sync", "threads", "processes"])
@pytest.mark.parametrize("npartitions", [1, 4, 10])
def test_to_hdf_schedulers(scheduler, npartitions, tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
            ],
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        },
        index=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            15.0,
            16.0,
        ],
    )
    a = dd.from_pandas(df, npartitions=npartitions)

    # test single file single node
    fn = tmp_path / "foo.h5"
    a.to_hdf(fn, "/data", scheduler=scheduler)
    out = pd.read_hdf(fn, "/data")
    assert_eq(df, out)

    # test multiple files single node
    with tempfile.TemporaryDirectory() as dn:
        fn = os.path.join(dn, "data_*.h5")
        a.to_hdf(fn, "/data", scheduler=scheduler)
        out = dd.read_hdf(fn, "/data")
        assert_eq(df, out)

    # # test single file multiple nodes
    fn = tmp_path / "bar.h5"
    a.to_hdf(fn, "/data*", scheduler=scheduler)
    out = dd.read_hdf(fn, "/data*")
    assert_eq(df, out)


def test_to_hdf_kwargs(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame({"A": ["a", "aaaa"]})
    ddf = dd.from_pandas(df, npartitions=2)
    fn = os.path.join(tmp_path, "fn1.h5")
    ddf.to_hdf(fn, "foo4", format="table", min_itemsize=4)
    df2 = pd.read_hdf(fn, "foo4")
    tm.assert_frame_equal(df, df2)

    # test shorthand 't' for table
    fn = os.path.join(tmp_path, "fn2.h5")
    ddf.to_hdf(fn, "foo4", format="t", min_itemsize=4)
    df2 = pd.read_hdf(fn, "foo4")
    tm.assert_frame_equal(df, df2)


def test_to_fmt_warns(tmp_path):
    pytest.importorskip("tables")
    df16 = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
            ],
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        },
        index=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            15.0,
            16.0,
        ],
    )
    a = dd.from_pandas(df16, 16)

    # testing warning when breaking order
    fn = os.path.join(tmp_path, "fn.h5")
    with pytest.warns(None):
        a.to_hdf(fn, "/data*", name_function=str)

    # testing warning when breaking order
    with pytest.warns(None):
        fn = os.path.join(tmp_path, "data_*.csv")
        a.to_csv(fn, name_function=str)


@pytest.mark.parametrize(
    "data, compare",
    [
        (
            pd.DataFrame(
                {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]},
                index=[1.0, 2.0, 3.0, 4.0],
            ),
            tm.assert_frame_equal,
        ),
        (pd.Series([1, 2, 3, 4], name="a"), tm.assert_series_equal),
    ],
)
def test_read_hdf(data, compare, tmp_path):
    pytest.importorskip("tables")
    fn = os.path.join(tmp_path, "fn1.h5")
    data.to_hdf(fn, "/data")
    try:
        dd.read_hdf(fn, "data", chunksize=2, mode="r")
        assert False
    except TypeError as e:
        assert "format='table'" in str(e)

    fn = os.path.join(tmp_path, "fn2.h5")
    data.to_hdf(fn, "/data", format="table")
    a = dd.read_hdf(fn, "/data", chunksize=2, mode="r")
    assert a.npartitions == 2

    compare(a.compute(), data)

    compare(
        dd.read_hdf(fn, "/data", chunksize=2, start=1, stop=3, mode="r").compute(),
        pd.read_hdf(fn, "/data", start=1, stop=3),
    )

    assert sorted(dd.read_hdf(fn, "/data", mode="r").dask) == sorted(
        dd.read_hdf(fn, "/data", mode="r").dask
    )

    fn = os.path.join(tmp_path, "fn3.h5")
    sorted_data = data.sort_index()
    sorted_data.to_hdf(fn, "/data", format="table")
    a = dd.read_hdf(fn, "/data", chunksize=2, sorted_index=True, mode="r")
    assert a.npartitions == 2

    compare(a.compute(), sorted_data)


def test_read_hdf_multiply_open(tmp_path):
    """Test that we can read from a file that's already opened elsewhere in
    read-only mode."""
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    fn = os.path.join(tmp_path, "fn.h5")
    df.to_hdf(fn, "/data", format="table")
    with pd.HDFStore(fn, mode="r"):
        dd.read_hdf(fn, "/data", chunksize=2, mode="r")


def test_read_hdf_multiple(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {
            "x": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o",
                "p",
            ],
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        },
        index=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            15.0,
            16.0,
        ],
    )
    a = dd.from_pandas(df, 16)

    fn = os.path.join(tmp_path, "fn.h5")
    a.to_hdf(fn, "/data*")
    r = dd.read_hdf(fn, "/data*", sorted_index=True)
    assert a.npartitions == r.npartitions
    assert a.divisions == r.divisions
    assert_eq(a, r)


def test_read_hdf_start_stop_values(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    fn = os.path.join(tmp_path, "fn.h5")
    df.to_hdf(fn, "/data", format="table")

    with pytest.raises(ValueError, match="number of rows"):
        dd.read_hdf(fn, "/data", stop=10)

    with pytest.raises(ValueError, match="is above or equal to"):
        dd.read_hdf(fn, "/data", start=10)

    with pytest.raises(ValueError, match="positive integer"):
        dd.read_hdf(fn, "/data", chunksize=-1)


def test_hdf_globbing(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )

    df.to_hdf(os.path.join(tmp_path, "one.h5"), "/foo/data", format="table")
    df.to_hdf(os.path.join(tmp_path, "two.h5"), "/bar/data", format="table")
    df.to_hdf(os.path.join(tmp_path, "two.h5"), "/foo/data", format="table")

    with dask.config.set(scheduler="sync"):
        res = dd.read_hdf(os.path.join(tmp_path, "one.h5"), "/*/data", chunksize=2)
        assert res.npartitions == 2
        tm.assert_frame_equal(res.compute(), df)

        res = dd.read_hdf(
            os.path.join(tmp_path, "one.h5"), "/*/data", chunksize=2, start=1, stop=3
        )
        expected = pd.read_hdf(
            os.path.join(tmp_path, "one.h5"), "/foo/data", start=1, stop=3
        )
        tm.assert_frame_equal(res.compute(), expected)

        res = dd.read_hdf(os.path.join(tmp_path, "two.h5"), "/*/data", chunksize=2)
        assert res.npartitions == 2 + 2
        tm.assert_frame_equal(res.compute(), pd.concat([df] * 2))

        res = dd.read_hdf(os.path.join(tmp_path, "*.h5"), "/foo/data", chunksize=2)
        assert res.npartitions == 2 + 2
        tm.assert_frame_equal(res.compute(), pd.concat([df] * 2))

        res = dd.read_hdf(os.path.join(tmp_path, "*.h5"), "/*/data", chunksize=2)
        assert res.npartitions == 2 + 2 + 2
        tm.assert_frame_equal(res.compute(), pd.concat([df] * 3))


def test_hdf_file_list(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )

    df.iloc[:2].to_hdf(os.path.join(tmp_path, "one.h5"), "dataframe", format="table")
    df.iloc[2:].to_hdf(os.path.join(tmp_path, "two.h5"), "dataframe", format="table")

    with dask.config.set(scheduler="sync"):
        input_files = [
            os.path.join(tmp_path, "one.h5"),
            os.path.join(tmp_path, "two.h5"),
        ]
        res = dd.read_hdf(input_files, "dataframe")
        tm.assert_frame_equal(res.compute(), df)


def test_read_hdf_pattern_pathlike(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )

    fn = os.path.join(tmp_path, "fn.h5")
    path = pathlib.Path(fn)
    df.to_hdf(path, "dataframe", format="table")
    res = dd.read_hdf(path, "dataframe")
    assert_eq(res, df)


def test_to_hdf_path_pathlike(tmp_path):
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    ddf = dd.from_pandas(df, npartitions=3)

    fn = os.path.join(tmp_path, "fn.h5")
    path = pathlib.Path(fn)
    ddf.to_hdf(path, "/data")
    res = pd.read_hdf(path, "/data")
    assert_eq(res, ddf)


def test_read_hdf_doesnt_segfault(tmp_path):
    pytest.importorskip("tables")
    fn = os.path.join(tmp_path, "fn.h5")
    N = 40
    df = pd.DataFrame(np.random.randn(N, 3))
    with pd.HDFStore(fn, mode="w") as store:
        store.append("/x", df)

    ddf = dd.read_hdf(fn, "/x", chunksize=2)
    assert len(ddf) == N


def test_hdf_filenames():
    pytest.importorskip("tables")
    df = pd.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]}, index=[1.0, 2.0, 3.0, 4.0]
    )
    ddf = dd.from_pandas(df, npartitions=2)
    assert ddf.to_hdf("foo*.hdf5", "key") == ["foo0.hdf5", "foo1.hdf5"]
    os.remove("foo0.hdf5")
    os.remove("foo1.hdf5")


def test_hdf_path_exceptions():

    # single file doesn't exist
    with pytest.raises(IOError):
        dd.read_hdf("nonexistant_store_X34HJK", "/tmp")

    # a file from a list of files doesn't exist
    with pytest.raises(IOError):
        dd.read_hdf(["nonexistant_store_X34HJK", "nonexistant_store_UY56YH"], "/tmp")

    # list of files is empty
    with pytest.raises(ValueError):
        dd.read_hdf([], "/tmp")


def test_hdf_nonpandas_keys(tmp_path):
    # https://github.com/dask/dask/issues/5934
    # TODO: maybe remove this if/when pandas copes with all keys

    tables = pytest.importorskip("tables")
    import tables

    class Table1(tables.IsDescription):
        value1 = tables.Float32Col()

    class Table2(tables.IsDescription):
        value2 = tables.Float32Col()

    class Table3(tables.IsDescription):
        value3 = tables.Float32Col()

    path = os.path.join(tmp_path, "fn.h5")
    with tables.open_file(path, mode="a") as h5file:
        group = h5file.create_group("/", "group")
        t = h5file.create_table(group, "table1", Table1, "Table 1")
        row = t.row
        row["value1"] = 1
        row.append()
        t = h5file.create_table(group, "table2", Table2, "Table 2")
        row = t.row
        row["value2"] = 1
        row.append()
        t = h5file.create_table(group, "table3", Table3, "Table 3")
        row = t.row
        row["value3"] = 1
        row.append()

    # pandas keys should still work
    bar = pd.DataFrame(np.random.randn(10, 4))
    bar.to_hdf(path, "/bar", format="table", mode="a")

    dd.read_hdf(path, "/group/table1")
    dd.read_hdf(path, "/group/table2")
    dd.read_hdf(path, "/group/table3")
    dd.read_hdf(path, "/bar")

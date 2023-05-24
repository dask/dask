from __future__ import annotations

import pandas as pd
import pytest

import dask
import dask.dataframe as dd
from dask.blockwise import Blockwise, optimize_blockwise
from dask.dataframe._compat import tm
from dask.dataframe.optimize import optimize_dataframe_getitem
from dask.dataframe.utils import assert_eq, get_string_dtype


def test_make_timeseries():
    df = dd.demo.make_timeseries(
        "2000", "2015", {"A": float, "B": int, "C": str}, freq="2D", partition_freq="6M"
    )

    assert df.divisions[0] == pd.Timestamp("2000-01-31")
    assert df.divisions[-1] == pd.Timestamp("2014-07-31")
    tm.assert_index_equal(df.columns, pd.Index(["A", "B", "C"]))
    assert df["A"].head().dtype == float
    assert df["B"].head().dtype == int
    assert df["C"].head().dtype == get_string_dtype()
    assert df.index.name == "timestamp"
    assert df.head().index.name == df.index.name
    assert df.divisions == tuple(pd.date_range(start="2000", end="2015", freq="6M"))

    tm.assert_frame_equal(df.head(), df.head())

    a = dd.demo.make_timeseries(
        "2000",
        "2015",
        {"A": float, "B": int, "C": str},
        freq="2D",
        partition_freq="6M",
        seed=123,
    )
    b = dd.demo.make_timeseries(
        "2000",
        "2015",
        {"A": float, "B": int, "C": str},
        freq="2D",
        partition_freq="6M",
        seed=123,
    )
    c = dd.demo.make_timeseries(
        "2000",
        "2015",
        {"A": float, "B": int, "C": str},
        freq="2D",
        partition_freq="6M",
        seed=456,
    )
    d = dd.demo.make_timeseries(
        "2000",
        "2015",
        {"A": float, "B": int, "C": str},
        freq="2D",
        partition_freq="3M",
        seed=123,
    )
    e = dd.demo.make_timeseries(
        "2000",
        "2015",
        {"A": float, "B": int, "C": str},
        freq="1D",
        partition_freq="6M",
        seed=123,
    )
    tm.assert_frame_equal(a.head(), b.head())
    assert not (a.head(10) == c.head(10)).all().all()
    assert a._name == b._name
    assert a._name != c._name
    assert a._name != d._name
    assert a._name != e._name


def test_make_timeseries_no_args():
    df = dd.demo.make_timeseries()
    assert 1 < df.npartitions < 1000
    assert len(df.columns) > 1
    assert len(set(df.dtypes)) > 1


@pytest.mark.skip_with_pyarrow_strings  # checks graph layers
def test_make_timeseries_blockwise():
    df = dd.demo.make_timeseries()
    df = df[["x", "y"]]
    keys = [(df._name, i) for i in range(df.npartitions)]

    # Check that `optimize_dataframe_getitem` changes the
    # `columns` attribute of the "make-timeseries" layer
    graph = optimize_dataframe_getitem(df.__dask_graph__(), keys)
    key = [k for k in graph.layers.keys() if k.startswith("make-timeseries-")][0]
    assert set(graph.layers[key].columns) == {"x", "y"}

    # Check that `optimize_blockwise` fuses both
    # `Blockwise` layers together into a singe `Blockwise` layer
    graph = optimize_blockwise(df.__dask_graph__(), keys)
    layers = graph.layers
    name = list(layers.keys())[0]
    assert len(layers) == 1
    assert isinstance(layers[name], Blockwise)


def test_no_overlaps():
    df = dd.demo.make_timeseries(
        "2000", "2001", {"A": float}, freq="3H", partition_freq="3M"
    )

    assert all(
        df.get_partition(i).index.max().compute()
        < df.get_partition(i + 1).index.min().compute()
        for i in range(df.npartitions - 2)
    )


def test_make_timeseries_keywords():
    df = dd.demo.make_timeseries(
        "2000",
        "2001",
        {"A": int, "B": int, "C": str},
        freq="1D",
        partition_freq="6M",
        A_lam=1000000,
        B_lam=2,
    )
    a_cardinality = df.A.nunique()
    b_cardinality = df.B.nunique()

    aa, bb = dask.compute(a_cardinality, b_cardinality, scheduler="single-threaded")

    assert 100 < aa <= 10000000
    assert 1 < bb <= 100


def test_make_timeseries_fancy_keywords():
    df = dd.demo.make_timeseries(
        "2000",
        "2001",
        {"A_B": int, "B_": int, "C": str},
        freq="1D",
        partition_freq="6M",
        A_B_lam=1000000,
        B__lam=2,
    )
    a_cardinality = df.A_B.nunique()
    b_cardinality = df.B_.nunique()

    aa, bb = dask.compute(a_cardinality, b_cardinality, scheduler="single-threaded")

    assert 100 < aa <= 10000000
    assert 1 < bb <= 100


def test_make_timeseries_getitem_compute():
    # See https://github.com/dask/dask/issues/7692

    df = dd.demo.make_timeseries()
    df2 = df[df.y > 0]
    df3 = df2.compute()
    assert df3["y"].min() > 0
    assert list(df.columns) == list(df3.columns)


def test_make_timeseries_column_projection():
    ddf = dd.demo.make_timeseries(
        "2001", "2002", freq="1D", partition_freq="3M", seed=42
    )

    assert_eq(ddf[["x"]].compute(), ddf.compute()[["x"]])
    assert_eq(
        ddf.groupby("name").aggregate({"x": "sum", "y": "max"}).compute(),
        ddf.compute().groupby("name").aggregate({"x": "sum", "y": "max"}),
    )

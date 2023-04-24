import warnings

import numpy as np
import pandas as pd
import pytest
from pandas.api.types import is_object_dtype

import dask.dataframe as dd
from dask.base import compute_as_if_collection
from dask.dataframe._compat import PANDAS_GT_140, PANDAS_GT_150, PANDAS_GT_200, tm
from dask.dataframe.core import _Frame
from dask.dataframe.methods import concat
from dask.dataframe.multi import (
    _maybe_align_partitions,
    align_partitions,
    concat_indexed_dataframes,
    hash_join,
    merge_indexed_dataframes,
)
from dask.dataframe.utils import (
    assert_divisions,
    assert_eq,
    check_meta,
    clear_known_categories,
    get_string_dtype,
    has_known_categories,
    make_meta,
)
from dask.utils_test import hlg_layer, hlg_layer_topological

try:
    import pyarrow as pa
except ImportError:
    pa = None


def test_align_partitions():
    A = pd.DataFrame(
        {"x": [1, 2, 3, 4, 5, 6], "y": list("abdabd")}, index=[10, 20, 30, 40, 50, 60]
    )
    a = dd.repartition(A, [10, 40, 60])

    B = pd.DataFrame({"x": [1, 2, 3, 4], "y": list("abda")}, index=[30, 70, 80, 100])
    b = dd.repartition(B, [30, 80, 100])

    s = dd.core.Scalar({("s", 0): 10}, "s", "i8")

    (aa, bb), divisions, L = align_partitions(a, b)

    def _check(a, b, aa, bb):
        assert isinstance(a, dd.DataFrame)
        assert isinstance(b, dd.DataFrame)
        assert isinstance(aa, dd.DataFrame)
        assert isinstance(bb, dd.DataFrame)
        assert_eq(a, aa)
        assert_eq(b, bb)
        assert divisions == (10, 30, 40, 60, 80, 100)
        assert isinstance(L, list)
        assert len(divisions) == 1 + len(L)

    _check(a, b, aa, bb)
    assert L == [
        [(aa._name, 0), (bb._name, 0)],
        [(aa._name, 1), (bb._name, 1)],
        [(aa._name, 2), (bb._name, 2)],
        [(aa._name, 3), (bb._name, 3)],
        [(aa._name, 4), (bb._name, 4)],
    ]

    (aa, ss, bb), divisions, L = align_partitions(a, s, b)
    _check(a, b, aa, bb)
    assert L == [
        [(aa._name, 0), None, (bb._name, 0)],
        [(aa._name, 1), None, (bb._name, 1)],
        [(aa._name, 2), None, (bb._name, 2)],
        [(aa._name, 3), None, (bb._name, 3)],
        [(aa._name, 4), None, (bb._name, 4)],
    ]
    assert_eq(ss, 10)

    ldf = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]})
    rdf = pd.DataFrame({"c": [1, 2, 3, 4, 5, 6, 7], "d": [7, 6, 5, 4, 3, 2, 1]})

    for lhs, rhs in [
        (dd.from_pandas(ldf, 1), dd.from_pandas(rdf, 1)),
        (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 2)),
        (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 3)),
        (dd.from_pandas(ldf, 3), dd.from_pandas(rdf, 2)),
    ]:
        (lresult, rresult), div, parts = align_partitions(lhs, rhs)
        assert_eq(lresult, ldf)
        assert_eq(rresult, rdf)

    # different index
    ldf = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )
    rdf = pd.DataFrame(
        {"c": [1, 2, 3, 4, 5, 6, 7], "d": [7, 6, 5, 4, 3, 2, 1]}, index=list("fghijkl")
    )

    for lhs, rhs in [
        (dd.from_pandas(ldf, 1), dd.from_pandas(rdf, 1)),
        (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 2)),
        (dd.from_pandas(ldf, 2), dd.from_pandas(rdf, 3)),
        (dd.from_pandas(ldf, 3), dd.from_pandas(rdf, 2)),
    ]:
        (lresult, rresult), div, parts = align_partitions(lhs, rhs)
        assert_eq(lresult, ldf)
        assert_eq(rresult, rdf)


def test_align_partitions_unknown_divisions():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]})
    # One known, one unknown
    ddf = dd.from_pandas(df, npartitions=2)
    ddf2 = dd.from_pandas(df, npartitions=2, sort=False)
    assert not ddf2.known_divisions

    with pytest.raises(ValueError):
        align_partitions(ddf, ddf2)

    # Both unknown
    ddf = dd.from_pandas(df + 1, npartitions=2, sort=False)
    ddf2 = dd.from_pandas(df, npartitions=2, sort=False)
    assert not ddf.known_divisions
    assert not ddf2.known_divisions

    with pytest.raises(ValueError):
        align_partitions(ddf, ddf2)


def test__maybe_align_partitions():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]})
    # Both known, same divisions
    ddf = dd.from_pandas(df + 1, npartitions=2)
    ddf2 = dd.from_pandas(df, npartitions=2)

    a, b = _maybe_align_partitions([ddf, ddf2])
    assert a is ddf
    assert b is ddf2

    # Both unknown, same divisions
    ddf = dd.from_pandas(df + 1, npartitions=2, sort=False)
    ddf2 = dd.from_pandas(df, npartitions=2, sort=False)
    assert not ddf.known_divisions
    assert not ddf2.known_divisions

    a, b = _maybe_align_partitions([ddf, ddf2])
    assert a is ddf
    assert b is ddf2

    # Both known, different divisions
    ddf = dd.from_pandas(df + 1, npartitions=2)
    ddf2 = dd.from_pandas(df, npartitions=3)

    a, b = _maybe_align_partitions([ddf, ddf2])
    assert a.divisions == b.divisions

    # Both unknown, different divisions
    ddf = dd.from_pandas(df + 1, npartitions=2, sort=False)
    ddf2 = dd.from_pandas(df, npartitions=3, sort=False)
    assert not ddf.known_divisions
    assert not ddf2.known_divisions

    with pytest.raises(ValueError):
        _maybe_align_partitions([ddf, ddf2])

    # One known, one unknown
    ddf = dd.from_pandas(df, npartitions=2)
    ddf2 = dd.from_pandas(df, npartitions=2, sort=False)
    assert not ddf2.known_divisions

    with pytest.raises(ValueError):
        _maybe_align_partitions([ddf, ddf2])


def test_merge_indexed_dataframe_to_indexed_dataframe():
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6]}, index=[1, 2, 3, 4, 6, 7])
    a = dd.repartition(A, [1, 4, 7])

    B = pd.DataFrame({"y": list("abcdef")}, index=[1, 2, 4, 5, 6, 8])
    b = dd.repartition(B, [1, 2, 5, 8])

    c = merge_indexed_dataframes(a, b, how="left")
    assert c.divisions[0] == a.divisions[0]
    assert c.divisions[-1] == max(a.divisions + b.divisions)
    assert_eq(c, A.join(B))

    c = merge_indexed_dataframes(a, b, how="right")
    assert c.divisions[0] == b.divisions[0]
    assert c.divisions[-1] == b.divisions[-1]
    assert_eq(c, A.join(B, how="right"))

    c = merge_indexed_dataframes(a, b, how="inner")
    assert c.divisions[0] == 1
    assert c.divisions[-1] == max(a.divisions + b.divisions)
    assert_eq(c.compute(), A.join(B, how="inner"))

    c = merge_indexed_dataframes(a, b, how="outer")
    assert c.divisions[0] == 1
    assert c.divisions[-1] == 8
    assert_eq(c.compute(), A.join(B, how="outer"))

    assert sorted(merge_indexed_dataframes(a, b, how="inner").dask) == sorted(
        merge_indexed_dataframes(a, b, how="inner").dask
    )
    assert sorted(merge_indexed_dataframes(a, b, how="inner").dask) != sorted(
        merge_indexed_dataframes(a, b, how="outer").dask
    )


def list_eq(aa, bb):
    if isinstance(aa, dd.DataFrame):
        a = aa.compute(scheduler="sync")
    else:
        a = aa
    if isinstance(bb, dd.DataFrame):
        b = bb.compute(scheduler="sync")
    else:
        b = bb
    tm.assert_index_equal(a.columns, b.columns)

    if isinstance(a, pd.DataFrame):
        av = a.sort_values(list(a.columns)).values
        bv = b.sort_values(list(b.columns)).values
    else:
        av = a.sort_values().values
        bv = b.sort_values().values

    dd._compat.assert_numpy_array_equal(av, bv)


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
def test_hash_join(how, shuffle_method):
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    c = hash_join(a, "y", b, "y", how)

    assert not hlg_layer_topological(c.dask, -1).is_materialized()
    result = c.compute()
    expected = pd.merge(A, B, how, "y")
    list_eq(result, expected)

    # Different columns and npartitions
    c = hash_join(a, "x", b, "z", "outer", npartitions=3, shuffle=shuffle_method)
    assert not hlg_layer_topological(c.dask, -1).is_materialized()
    assert c.npartitions == 3

    result = c.compute(scheduler="single-threaded")
    expected = pd.merge(A, B, "outer", None, "x", "z")

    list_eq(result, expected)

    assert (
        hash_join(a, "y", b, "y", "inner", shuffle=shuffle_method)._name
        == hash_join(a, "y", b, "y", "inner", shuffle=shuffle_method)._name
    )
    assert (
        hash_join(a, "y", b, "y", "inner", shuffle=shuffle_method)._name
        != hash_join(a, "y", b, "y", "outer", shuffle=shuffle_method)._name
    )


def test_sequential_joins():
    # Pandas version of multiple inner joins
    df1 = pd.DataFrame(
        {"key": list(range(6)), "A": ["A0", "A1", "A2", "A3", "A4", "A5"]}
    )
    df2 = pd.DataFrame({"key": list(range(4)), "B": ["B0", "B1", "B2", "B3"]})
    df3 = pd.DataFrame({"key": list(range(1, 5)), "C": ["C0", "C1", "C2", "C3"]})

    join_pd = df1.join(df2, how="inner", lsuffix="_l", rsuffix="_r")
    multi_join_pd = join_pd.join(df3, how="inner", lsuffix="_l", rsuffix="_r")

    # Dask version of multiple inner joins
    ddf1 = dd.from_pandas(df1, npartitions=3)
    ddf2 = dd.from_pandas(df2, npartitions=2)
    ddf3 = dd.from_pandas(df3, npartitions=2)

    join_dd = ddf1.join(ddf2, how="inner", lsuffix="_l", rsuffix="_r")
    multi_join_dd = join_dd.join(ddf3, how="inner", lsuffix="_l", rsuffix="_r")
    assert_eq(multi_join_pd, multi_join_dd)


def test_merge_asof_indexed():
    A = pd.DataFrame(
        {"left_val": list("abcd" * 3)},
        index=[1, 3, 7, 9, 10, 13, 14, 17, 20, 24, 25, 28],
    )
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame(
        {"right_val": list("xyz" * 4)},
        index=[1, 2, 3, 6, 7, 10, 12, 14, 16, 19, 23, 26],
    )
    b = dd.from_pandas(B, npartitions=3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = dd.merge_asof(a, b, left_index=True, right_index=True)

    assert_eq(c, C)


def test_merge_asof_on_basic():
    A = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    a = dd.from_pandas(A, npartitions=2)
    B = pd.DataFrame({"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})
    b = dd.from_pandas(B, npartitions=2)

    C = pd.merge_asof(A, B, on="a")
    c = dd.merge_asof(a, b, on="a")
    # merge_asof does not preserve index
    assert_eq(c, C, check_index=False)


def test_merge_asof_on_lefton_righton_error():
    A = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    a = dd.from_pandas(A, npartitions=2)
    B = pd.DataFrame({"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})
    b = dd.from_pandas(B, npartitions=2)

    with pytest.raises(ValueError, match="combination of both"):
        dd.merge_asof(a, b, on="a", left_on="left_val")
    with pytest.raises(ValueError, match="combination of both"):
        dd.merge_asof(a, b, on="a", right_on="right_val")
    with pytest.raises(ValueError, match="combination of both"):
        dd.merge_asof(a, b, on="a", left_on="left_val", right_on="right_val")


def test_merge_asof_by_leftby_rightby_error():
    A = pd.DataFrame({"a": [1, 5, 10], "b": [3, 6, 9], "left_val": ["a", "b", "c"]})
    a = dd.from_pandas(A, npartitions=2)
    B = pd.DataFrame(
        {"a": [1, 2, 3, 6, 7], "b": [3, 6, 9, 12, 15], "right_val": [1, 2, 3, 6, 7]}
    )
    b = dd.from_pandas(B, npartitions=2)

    with pytest.raises(ValueError, match="combination of both"):
        dd.merge_asof(a, b, on="a", by="b", left_by="left_val")
    with pytest.raises(ValueError, match="combination of both"):
        dd.merge_asof(a, b, on="a", by="b", right_by="right_val")
    with pytest.raises(ValueError, match="combination of both"):
        dd.merge_asof(a, b, on="a", by="b", left_by="left_val", right_by="right_val")


@pytest.mark.parametrize("allow_exact_matches", [True, False])
@pytest.mark.parametrize("direction", ["backward", "forward", "nearest"])
def test_merge_asof_on(allow_exact_matches, direction):
    A = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    a = dd.from_pandas(A, npartitions=2)
    B = pd.DataFrame({"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})
    b = dd.from_pandas(B, npartitions=2)

    C = pd.merge_asof(
        A, B, on="a", allow_exact_matches=allow_exact_matches, direction=direction
    )
    c = dd.merge_asof(
        a, b, on="a", allow_exact_matches=allow_exact_matches, direction=direction
    )
    # merge_asof does not preserve index
    assert_eq(c, C, check_index=False)


@pytest.mark.parametrize("allow_exact_matches", [True, False])
@pytest.mark.parametrize("direction", ["backward", "forward", "nearest"])
@pytest.mark.parametrize("unknown_divisions", [False, True])
def test_merge_asof_left_on_right_index(
    allow_exact_matches, direction, unknown_divisions
):
    A = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]}, index=[10, 20, 30])
    a = dd.from_pandas(A, npartitions=2)
    B = pd.DataFrame({"right_val": [2, 3, 6, 7]}, index=[2, 3, 6, 7])
    b = dd.from_pandas(B, npartitions=2)

    if unknown_divisions:
        a.divisions = (None,) * len(a.divisions)

    C = pd.merge_asof(
        A,
        B,
        left_on="a",
        right_index=True,
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    c = dd.merge_asof(
        a,
        b,
        left_on="a",
        right_index=True,
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    assert_eq(c, C)

    for nparts in [1, 2, 3]:
        for a1, idx2 in (
            ([5, 10, 15, 20], [1, 2, 3, 4]),
            ([1, 2, 3, 4], [5, 10, 15, 20]),
            ([5, 5, 10, 10, 15, 15], [4, 5, 6, 9, 10, 11, 14, 15, 16]),
            ([5, 10, 15], [4, 4, 5, 5, 6, 6, 9, 9, 10, 10, 11, 11]),
        ):
            A = pd.DataFrame({"a": a1}, index=[x * 10 for x in a1])
            a = dd.from_pandas(A, npartitions=nparts)
            B = pd.DataFrame({"b": idx2}, index=idx2)
            b = dd.from_pandas(B, npartitions=nparts)

            if unknown_divisions:
                a.divisions = (None,) * len(a.divisions)

            C = pd.merge_asof(
                A,
                B,
                left_on="a",
                right_index=True,
                allow_exact_matches=allow_exact_matches,
                direction=direction,
            )
            c = dd.merge_asof(
                a,
                b,
                left_on="a",
                right_index=True,
                allow_exact_matches=allow_exact_matches,
                direction=direction,
            )
            assert_eq(c, C)


def test_merge_asof_indexed_two_partitions():
    A = pd.DataFrame({"left_val": ["a", "b", "c"]}, index=[1, 5, 10])
    a = dd.from_pandas(A, npartitions=2)
    B = pd.DataFrame({"right_val": [1, 2, 3, 6, 7]}, index=[1, 2, 3, 6, 7])
    b = dd.from_pandas(B, npartitions=2)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = dd.merge_asof(a, b, left_index=True, right_index=True)
    assert_eq(c, C)


def test_merge_asof_on_by():
    times_A = [
        pd.to_datetime(d)
        for d in [
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.030",
            "2016-05-25 13:30:00.041",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.049",
            "2016-05-25 13:30:00.072",
            "2016-05-25 13:30:00.075",
        ]
    ]
    tickers_A = ["GOOG", "MSFT", "MSFT", "MSFT", "GOOG", "AAPL", "GOOG", "MSFT"]
    bids_A = [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01]
    asks_A = [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03]
    times_B = [
        pd.to_datetime(d)
        for d in [
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.038",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.048",
        ]
    ]
    tickers_B = ["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"]
    prices_B = [51.95, 51.95, 720.77, 720.92, 98.00]
    quantities_B = [75, 155, 100, 100, 100]

    A = pd.DataFrame(
        {"time": times_A, "ticker": tickers_A, "bid": bids_A, "ask": asks_A},
        columns=["time", "ticker", "bid", "ask"],
    )
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame(
        {
            "time": times_B,
            "ticker": tickers_B,
            "price": prices_B,
            "quantity": quantities_B,
        },
        columns=["time", "ticker", "price", "quantity"],
    )
    # TODO: Use from_pandas(B, npartitions=3)
    # (see https://github.com/dask/dask/issues/9225)
    b = dd.from_map(
        lambda x: x,
        [B.iloc[0:2], B.iloc[2:5]],
        divisions=[0, 2, 4],
    )

    C = pd.merge_asof(B, A, on="time", by="ticker")
    c = dd.merge_asof(b, a, on="time", by="ticker")
    assert_eq(c, C, check_index=False)


def test_merge_asof_on_by_tolerance():
    times_A = [
        pd.to_datetime(d)
        for d in [
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.030",
            "2016-05-25 13:30:00.041",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.049",
            "2016-05-25 13:30:00.072",
            "2016-05-25 13:30:00.075",
        ]
    ]
    tickers_A = ["GOOG", "MSFT", "MSFT", "MSFT", "GOOG", "AAPL", "GOOG", "MSFT"]
    bids_A = [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01]
    asks_A = [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03]
    times_B = [
        pd.to_datetime(d)
        for d in [
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.038",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.048",
        ]
    ]
    tickers_B = ["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"]
    prices_B = [51.95, 51.95, 720.77, 720.92, 98.00]
    quantities_B = [75, 155, 100, 100, 100]

    A = pd.DataFrame(
        {"time": times_A, "ticker": tickers_A, "bid": bids_A, "ask": asks_A},
        columns=["time", "ticker", "bid", "ask"],
    )
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame(
        {
            "time": times_B,
            "ticker": tickers_B,
            "price": prices_B,
            "quantity": quantities_B,
        },
        columns=["time", "ticker", "price", "quantity"],
    )
    # TODO: Use from_pandas(B, npartitions=3)
    # (see https://github.com/dask/dask/issues/9225)
    b = dd.from_map(
        lambda x: x,
        [B.iloc[0:2], B.iloc[2:5]],
        divisions=[0, 2, 4],
    )

    C = pd.merge_asof(B, A, on="time", by="ticker", tolerance=pd.Timedelta("2ms"))
    c = dd.merge_asof(b, a, on="time", by="ticker", tolerance=pd.Timedelta("2ms"))
    assert_eq(c, C, check_index=False)


def test_merge_asof_on_by_tolerance_no_exact_matches():
    times_A = [
        pd.to_datetime(d)
        for d in [
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.030",
            "2016-05-25 13:30:00.041",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.049",
            "2016-05-25 13:30:00.072",
            "2016-05-25 13:30:00.075",
        ]
    ]
    tickers_A = ["GOOG", "MSFT", "MSFT", "MSFT", "GOOG", "AAPL", "GOOG", "MSFT"]
    bids_A = [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01]
    asks_A = [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03]
    times_B = [
        pd.to_datetime(d)
        for d in [
            "2016-05-25 13:30:00.023",
            "2016-05-25 13:30:00.038",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.048",
            "2016-05-25 13:30:00.048",
        ]
    ]
    tickers_B = ["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"]
    prices_B = [51.95, 51.95, 720.77, 720.92, 98.00]
    quantities_B = [75, 155, 100, 100, 100]

    A = pd.DataFrame(
        {"time": times_A, "ticker": tickers_A, "bid": bids_A, "ask": asks_A},
        columns=["time", "ticker", "bid", "ask"],
    )
    a = dd.from_pandas(A, npartitions=4)
    B = pd.DataFrame(
        {
            "time": times_B,
            "ticker": tickers_B,
            "price": prices_B,
            "quantity": quantities_B,
        },
        columns=["time", "ticker", "price", "quantity"],
    )
    # TODO: Use from_pandas(B, npartitions=3)
    # (see https://github.com/dask/dask/issues/9225)
    b = dd.from_map(
        lambda x: x,
        [B.iloc[0:2], B.iloc[2:5]],
        divisions=[0, 2, 4],
    )

    C = pd.merge_asof(
        B,
        A,
        on="time",
        by="ticker",
        tolerance=pd.Timedelta("10ms"),
        allow_exact_matches=False,
    )
    c = dd.merge_asof(
        b,
        a,
        on="time",
        by="ticker",
        tolerance=pd.Timedelta("10ms"),
        allow_exact_matches=False,
    )
    assert_eq(c, C, check_index=False)


def test_merge_asof_unsorted_raises():
    A = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    a = dd.from_pandas(A, npartitions=2)
    B = pd.DataFrame({"a": [2, 1, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})
    b = dd.from_pandas(B, npartitions=2)

    result = dd.merge_asof(a, b, on="a")
    # raise at runtime
    with pytest.raises(ValueError, match="right keys"):
        result.compute()


def test_merge_asof_with_empty():
    good_df = pd.DataFrame({"index_col": list(range(10)), "good_val": list(range(10))})
    good_dd = dd.from_pandas(good_df, npartitions=2)
    empty_df = (
        good_df[good_df.index_col < 0].copy().rename(columns={"good_val": "empty_val"})
    )
    empty_dd = dd.from_pandas(empty_df, npartitions=2)

    # left good, right empty
    result_dd = dd.merge_asof(
        good_dd.set_index("index_col"),
        empty_dd.set_index("index_col"),
        left_index=True,
        right_index=True,
    )
    result_df = pd.merge_asof(
        good_df.set_index("index_col"),
        empty_df.set_index("index_col"),
        left_index=True,
        right_index=True,
    )
    assert_eq(result_dd, result_df, check_index=False)
    # left empty, right good
    result_dd = dd.merge_asof(
        empty_dd.set_index("index_col"),
        good_dd.set_index("index_col"),
        left_index=True,
        right_index=True,
    )
    result_df = pd.merge_asof(
        empty_df.set_index("index_col"),
        good_df.set_index("index_col"),
        left_index=True,
        right_index=True,
    )
    assert_eq(result_dd, result_df, check_index=False)
    # left/right both empty
    result_dd = dd.merge_asof(
        empty_dd.set_index("index_col"),
        empty_dd.set_index("index_col"),
        left_index=True,
        right_index=True,
    )
    result_df = pd.merge_asof(
        empty_df.set_index("index_col"),
        empty_df.set_index("index_col"),
        left_index=True,
        right_index=True,
    )
    assert_eq(result_dd, result_df, check_index=False)


@pytest.mark.parametrize(
    "left_col, right_col", [("endofweek", "timestamp"), ("endofweek", "endofweek")]
)
def test_merge_asof_on_left_right(left_col, right_col):
    df1 = pd.DataFrame(
        {
            left_col: [1, 1, 2, 2, 3, 4],
            "GroupCol": [1234, 8679, 1234, 8679, 1234, 8679],
        }
    )
    df2 = pd.DataFrame({right_col: [0, 0, 1, 3], "GroupVal": [1234, 1234, 8679, 1234]})

    # pandas
    result_df = pd.merge_asof(df1, df2, left_on=left_col, right_on=right_col)

    # dask
    result_dd = dd.merge_asof(
        dd.from_pandas(df1, npartitions=2),
        dd.from_pandas(df2, npartitions=2),
        left_on=left_col,
        right_on=right_col,
    )

    assert_eq(result_df, result_dd, check_index=False)


def test_merge_asof_with_various_npartitions():
    # https://github.com/dask/dask/issues/8999
    df = pd.DataFrame(dict(ts=[pd.to_datetime("1-1-2020")] * 3, foo=[1, 2, 3]))
    expected = pd.merge_asof(left=df, right=df, on="ts")

    for npartitions in range(1, 5):
        ddf = dd.from_pandas(df, npartitions=npartitions)

        result = dd.merge_asof(left=ddf, right=ddf, on="ts")
        assert_eq(expected, result)


@pytest.mark.parametrize("join", ["inner", "outer"])
def test_indexed_concat(join):
    A = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "y": list("abcdef")}, index=[1, 2, 3, 4, 6, 7]
    )
    a = dd.repartition(A, [1, 4, 7])

    B = pd.DataFrame({"x": [10, 20, 40, 50, 60, 80]}, index=[1, 2, 4, 5, 6, 8])
    b = dd.repartition(B, [1, 2, 5, 8])

    expected = pd.concat([A, B], axis=0, join=join, sort=False)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        result = concat_indexed_dataframes([a, b], join=join)
        assert_eq(result, expected)
        assert sorted(concat_indexed_dataframes([a, b], join=join).dask) == sorted(
            concat_indexed_dataframes([a, b], join=join).dask
        )
        assert sorted(concat_indexed_dataframes([a, b], join="inner").dask) != sorted(
            concat_indexed_dataframes([a, b], join="outer").dask
        )


@pytest.mark.parametrize("join", ["inner", "outer"])
def test_concat(join):
    pdf1 = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "y": list("abcdef")}, index=[1, 2, 3, 4, 6, 7]
    )
    ddf1 = dd.from_pandas(pdf1, 2)
    pdf2 = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "y": list("abcdef")}, index=[8, 9, 10, 11, 12, 13]
    )
    ddf2 = dd.from_pandas(pdf2, 2)

    # different columns
    pdf3 = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "z": list("abcdef")}, index=[8, 9, 10, 11, 12, 13]
    )
    ddf3 = dd.from_pandas(pdf3, 2)

    kwargs = {"sort": False}

    for dd1, dd2, pd1, pd2 in [
        (ddf1, ddf2, pdf1, pdf2),
        (ddf1, ddf3, pdf1, pdf3),
    ]:
        expected = pd.concat([pd1, pd2], join=join, **kwargs)
        result = dd.concat([dd1, dd2], join=join, **kwargs)
        assert_eq(result, expected)


@pytest.mark.parametrize("join", ["inner", "outer"])
def test_concat_series(join):
    pdf1 = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "y": list("abcdef")}, index=[1, 2, 3, 4, 6, 7]
    )
    ddf1 = dd.from_pandas(pdf1, 2)
    pdf2 = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "y": list("abcdef")}, index=[8, 9, 10, 11, 12, 13]
    )
    ddf2 = dd.from_pandas(pdf2, 2)

    # different columns
    pdf3 = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "z": list("abcdef")}, index=[8, 9, 10, 11, 12, 13]
    )
    ddf3 = dd.from_pandas(pdf3, 2)

    kwargs = {"sort": False}

    for dd1, dd2, pd1, pd2 in [
        (ddf1.x, ddf2.x, pdf1.x, pdf2.x),
        (ddf1.x, ddf3.z, pdf1.x, pdf3.z),
    ]:
        expected = pd.concat([pd1, pd2], join=join, **kwargs)
        result = dd.concat([dd1, dd2], join=join, **kwargs)
        assert_eq(result, expected)


def test_concat_with_operation_remains_hlg():
    pdf1 = pd.DataFrame(
        {"x": [1, 2, 3, 4, 6, 7], "y": list("abcdef")}, index=[1, 2, 3, 4, 6, 7]
    )
    ddf1 = dd.from_pandas(pdf1, 2)

    pdf2 = pd.DataFrame({"y": list("abcdef")}, index=[8, 9, 10, 11, 12, 13])
    ddf2 = dd.from_pandas(pdf2, 2)

    # Do some operation which will remain blockwise in the dask case.
    pdf2["x"] = range(len(pdf2["y"]))
    # See https://stackoverflow.com/a/60852409
    ddf2["x"] = ddf2.assign(partition_count=1).partition_count.cumsum() - 1
    kwargs = {"sort": False}

    expected = pd.concat([pdf1, pdf2], **kwargs)
    result = dd.concat([ddf1, ddf2], **kwargs)
    # The third layer is the assignment to column `x`, which should remain
    # blockwise
    assert not hlg_layer_topological(result.dask, 2).is_materialized()
    assert_eq(result, expected)


def test_concat_dataframe_empty():
    df = pd.DataFrame({"a": [100, 200, 300]}, dtype="int64")
    empty_df = pd.DataFrame([], dtype="int64")
    df_concat = pd.concat([df, empty_df])

    ddf = dd.from_pandas(df, npartitions=1)
    empty_ddf = dd.from_pandas(empty_df, npartitions=1)
    ddf_concat = dd.concat([ddf, empty_ddf])
    assert_eq(df_concat, ddf_concat)

    empty_df_with_col = pd.DataFrame([], columns=["x"], dtype="int64")
    df_concat_with_col = pd.concat([df, empty_df_with_col])
    empty_ddf_with_col = dd.from_pandas(empty_df_with_col, npartitions=1)
    ddf_concat_with_col = dd.concat([ddf, empty_ddf_with_col])
    # NOTE: empty_ddf_with_col.index.dtype == dtype('O') while ddf.index.dtype == dtype('int64')
    # when we concatenate the resulting ddf_concat_with_col.index.dtype == dtype('O'). However,
    # the pandas version is smart enough to identify the empty rows and assign dtype('int64'),
    # which causes assert_eq to fail when checking dtype. Hence the follwoing line.
    ddf_concat_with_col._meta.index = ddf_concat_with_col._meta.index.astype("int64")

    assert_eq(df_concat_with_col, ddf_concat_with_col, check_dtype=False)


@pytest.mark.parametrize(
    "value_1, value_2",
    [
        (1.0, 1),
        (1.0, "one"),
        (1.0, pd.to_datetime("1970-01-01")),
        (1, "one"),
        (1, pd.to_datetime("1970-01-01")),
        ("one", pd.to_datetime("1970-01-01")),
    ],
)
def test_concat_different_dtypes(value_1, value_2):
    # check that the resulting dataframe has coherent dtypes
    # refer to https://github.com/dask/dask/issues/4685 and
    # https://github.com/dask/dask/issues/5968
    df_1 = pd.DataFrame({"x": [value_1]})
    df_2 = pd.DataFrame({"x": [value_2]})
    df = pd.concat([df_1, df_2], axis=0)

    # Dask would convert to pyarrow strings, Pandas wouldn't
    expected_dtype = (
        get_string_dtype() if is_object_dtype(df["x"].dtype) else df["x"].dtype
    )

    ddf_1 = dd.from_pandas(df_1, npartitions=1)
    ddf_2 = dd.from_pandas(df_2, npartitions=1)
    ddf = dd.concat([ddf_1, ddf_2], axis=0)

    dask_dtypes = list(ddf.map_partitions(lambda x: x.dtypes).compute())

    assert dask_dtypes == [expected_dtype, expected_dtype]


@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
@pytest.mark.parametrize("on_index", [True, False])
def test_merge_columns_dtypes(how, on_index):
    # tests results of merges with merge columns having different dtypes;
    # asserts that either the merge was successful or the corresponding warning is raised
    # addresses issue #4574

    df1 = pd.DataFrame(
        {"A": list(np.arange(5).astype(float)) * 2, "B": list(np.arange(5)) * 2}
    )
    df2 = pd.DataFrame({"A": np.arange(5), "B": np.arange(5)})

    a = dd.from_pandas(df1, 2)  # merge column "A" is float
    b = dd.from_pandas(df2, 2)  # merge column "A" is int

    on = ["A"]
    left_index = right_index = on_index

    if on_index:
        a = a.set_index("A")
        b = b.set_index("A")
        on = None

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        result = dd.merge(
            a, b, on=on, how=how, left_index=left_index, right_index=right_index
        )
        warned = any("merge column data type mismatches" in str(r) for r in record)

    # result type depends on merge operation -> convert to pandas
    result = result if isinstance(result, pd.DataFrame) else result.compute()

    has_nans = result.isna().values.any()
    assert (has_nans and warned) or not has_nans


@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
def test_merge(how, shuffle_method):
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    assert_eq(
        dd.merge(
            a, b, left_index=True, right_index=True, how=how, shuffle=shuffle_method
        ),
        pd.merge(A, B, left_index=True, right_index=True, how=how),
    )

    result = dd.merge(a, b, on="y", how=how)
    list_eq(result, pd.merge(A, B, on="y", how=how))
    assert all(d is None for d in result.divisions)

    list_eq(
        dd.merge(a, b, left_on="x", right_on="z", how=how, shuffle=shuffle_method),
        pd.merge(A, B, left_on="x", right_on="z", how=how),
    )
    list_eq(
        dd.merge(
            a,
            b,
            left_on="x",
            right_on="z",
            how=how,
            suffixes=("1", "2"),
            shuffle=shuffle_method,
        ),
        pd.merge(A, B, left_on="x", right_on="z", how=how, suffixes=("1", "2")),
    )

    list_eq(dd.merge(a, b, how=how, shuffle=shuffle_method), pd.merge(A, B, how=how))
    list_eq(dd.merge(a, B, how=how, shuffle=shuffle_method), pd.merge(A, B, how=how))
    list_eq(dd.merge(A, b, how=how, shuffle=shuffle_method), pd.merge(A, B, how=how))
    list_eq(dd.merge(A, B, how=how, shuffle=shuffle_method), pd.merge(A, B, how=how))

    list_eq(
        dd.merge(
            a, b, left_index=True, right_index=True, how=how, shuffle=shuffle_method
        ),
        pd.merge(A, B, left_index=True, right_index=True, how=how),
    )
    list_eq(
        dd.merge(
            a,
            b,
            left_index=True,
            right_index=True,
            how=how,
            suffixes=("1", "2"),
            shuffle=shuffle_method,
        ),
        pd.merge(A, B, left_index=True, right_index=True, how=how, suffixes=("1", "2")),
    )

    list_eq(
        dd.merge(a, b, left_on="x", right_index=True, how=how, shuffle=shuffle_method),
        pd.merge(A, B, left_on="x", right_index=True, how=how),
    )
    list_eq(
        dd.merge(
            a,
            b,
            left_on="x",
            right_index=True,
            how=how,
            suffixes=("1", "2"),
            shuffle=shuffle_method,
        ),
        pd.merge(A, B, left_on="x", right_index=True, how=how, suffixes=("1", "2")),
    )

    # pandas result looks buggy
    # list_eq(dd.merge(a, B, left_index=True, right_on='y'),
    #         pd.merge(A, B, left_index=True, right_on='y'))


@pytest.mark.parametrize("how", ["right", "outer"])
def test_merge_empty_left_df(shuffle_method, how):
    # We're merging on "a", but in a situation where the left
    # frame is missing some values, so some of the blocked merges
    # will involve an empty left frame.
    # https://github.com/pandas-dev/pandas/issues/9937
    left = pd.DataFrame({"a": [1, 1, 2, 2], "val": [5, 6, 7, 8]})
    right = pd.DataFrame({"a": [0, 0, 3, 3], "val": [11, 12, 13, 14]})

    dd_left = dd.from_pandas(left, npartitions=4)
    dd_right = dd.from_pandas(right, npartitions=4)

    merged = dd_left.merge(dd_right, on="a", how=how)
    expected = left.merge(right, on="a", how=how)
    assert_eq(merged, expected, check_index=False)

    # Check that the individual partitions have the expected shape
    merged.map_partitions(lambda x: x, meta=merged._meta).compute()


def test_merge_how_raises():
    left = pd.DataFrame(
        {
            "A": ["A0", "A1", "A2", "A3"],
            "B": ["B0", "B1", "B2", "B3"],
        }
    )
    right = pd.DataFrame(
        {
            "A": ["C0", "C1", "C2", "C3"],
            "B": ["D0", "D1", "D2", "D3"],
        }
    )

    with pytest.raises(
        ValueError, match="dask.dataframe.merge does not support how='cross'"
    ):
        dd.merge(left, right, how="cross")


@pytest.mark.parametrize("parts", [(3, 3), (3, 1), (1, 3)])
@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "pandas",
            marks=pytest.mark.xfail(
                reason="Pandas does not support leftsemi or leftanti"
            ),
        ),
        pytest.param("cudf", marks=pytest.mark.gpu),
    ],
)
def test_merge_tasks_semi_anti_cudf(engine, how, parts):
    if engine == "cudf":
        # NOTE: engine == "cudf" requires cudf/dask_cudf,
        # will be skipped by non-GPU CI.

        cudf = pytest.importorskip("cudf")
        dask_cudf = pytest.importorskip("dask_cudf")

    emp = pd.DataFrame(
        {
            "emp_id": np.arange(101, stop=106),
            "name": ["John", "Tom", "Harry", "Rahul", "Sakil"],
            "city": ["Cal", "Mum", "Del", "Ban", "Del"],
            "salary": [50000, 40000, 80000, 60000, 90000],
        }
    )
    skills = pd.DataFrame(
        {
            "skill_id": [404, 405, 406, 407, 408],
            "emp_id": [103, 101, 105, 102, 101],
            "skill_name": ["Dask", "Spark", "C", "Python", "R"],
        }
    )

    if engine == "cudf":
        emp = cudf.from_pandas(emp)
        skills = cudf.from_pandas(skills)
        dd_emp = dask_cudf.from_cudf(emp, npartitions=parts[0])
        dd_skills = dask_cudf.from_cudf(skills, npartitions=parts[1])
    else:
        dd_emp = dd.from_pandas(emp, npartitions=parts[0])
        dd_skills = dd.from_pandas(skills, npartitions=parts[1])

    expect = emp.merge(skills, on="emp_id", how=how).sort_values(["emp_id"])
    result = dd_emp.merge(dd_skills, on="emp_id", how=how).sort_values(["emp_id"])
    assert_eq(result, expect, check_index=False)


def test_merge_tasks_passes_through():
    a = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]})
    b = pd.DataFrame({"c": [1, 2, 3, 4, 5, 6, 7], "d": [7, 6, 5, 4, 3, 2, 1]})

    aa = dd.from_pandas(a, npartitions=3)
    bb = dd.from_pandas(b, npartitions=2)

    cc = aa.merge(bb, left_on="a", right_on="d", shuffle="tasks")

    assert not any("partd" in k[0] for k in cc.dask)


@pytest.mark.slow
@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
def test_merge_by_index_patterns(how, shuffle_method):
    pdf1l = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]})
    pdf1r = pd.DataFrame({"c": [1, 2, 3, 4, 5, 6, 7], "d": [7, 6, 5, 4, 3, 2, 1]})

    pdf2l = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5, 6, 7], "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )
    pdf2r = pd.DataFrame(
        {"c": [7, 6, 5, 4, 3, 2, 1], "d": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )

    pdf3l = pdf2l
    pdf3r = pd.DataFrame({"c": [6, 7, 8, 9], "d": [5, 4, 3, 2]}, index=list("abdg"))

    pdf4l = pdf2l
    pdf4r = pd.DataFrame({"c": [9, 10, 11, 12], "d": [5, 4, 3, 2]}, index=list("abdg"))

    # completely different index
    pdf5l = pd.DataFrame(
        {"a": [1, 1, 2, 2, 3, 3, 4], "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("lmnopqr")
    )
    pdf5r = pd.DataFrame({"c": [1, 1, 1, 1], "d": [5, 4, 3, 2]}, index=list("abcd"))

    pdf6l = pd.DataFrame(
        {"a": [1, 1, 2, 2, 3, 3, 4], "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("cdefghi")
    )
    pdf6r = pd.DataFrame({"c": [1, 2, 1, 2], "d": [5, 4, 3, 2]}, index=list("abcd"))

    pdf7l = pd.DataFrame(
        {"a": [1, 1, 2, 2, 3, 3, 4], "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )
    pdf7r = pd.DataFrame({"c": [5, 6, 7, 8], "d": [5, 4, 3, 2]}, index=list("fghi"))

    def fix_index(out, dtype):
        # Workaround pandas behavior where output dtype of empty index will be
        # object even if input was something else for pandas < 2.0.
        if len(out) == 0 and not PANDAS_GT_200:
            return out.set_index(out.index.astype(dtype))
        return out

    for pdl, pdr in [
        (pdf1l, pdf1r),
        (pdf2l, pdf2r),
        (pdf3l, pdf3r),
        (pdf4l, pdf4r),
        (pdf5l, pdf5r),
        (pdf6l, pdf6r),
        (pdf7l, pdf7r),
    ]:
        for lpart, rpart in [
            (2, 2),  # same partition
            (3, 2),  # left npartition > right npartition
            (2, 3),
        ]:  # left npartition < right npartition
            ddl = dd.from_pandas(pdl, lpart)
            ddr = dd.from_pandas(pdr, rpart)

            assert_eq(
                dd.merge(
                    ddl,
                    ddr,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                ).compute(),
                fix_index(
                    pd.merge(pdl, pdr, how=how, left_index=True, right_index=True),
                    pdl.index.dtype,
                ),
            )
            assert_eq(
                dd.merge(
                    ddr,
                    ddl,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                ).compute(),
                fix_index(
                    pd.merge(pdr, pdl, how=how, left_index=True, right_index=True),
                    pdr.index.dtype,
                ),
            )

            assert_eq(
                dd.merge(
                    ddl,
                    ddr,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                    indicator=True,
                ).compute(),
                fix_index(
                    pd.merge(
                        pdl,
                        pdr,
                        how=how,
                        left_index=True,
                        right_index=True,
                        indicator=True,
                    ),
                    pdl.index.dtype,
                ),
            )
            assert_eq(
                dd.merge(
                    ddr,
                    ddl,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                    indicator=True,
                ).compute(),
                fix_index(
                    pd.merge(
                        pdr,
                        pdl,
                        how=how,
                        left_index=True,
                        right_index=True,
                        indicator=True,
                    ),
                    pdr.index.dtype,
                ),
            )

            assert_eq(
                ddr.merge(
                    ddl,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                ).compute(),
                fix_index(
                    pdr.merge(pdl, how=how, left_index=True, right_index=True),
                    pdr.index.dtype,
                ),
            )
            assert_eq(
                ddl.merge(
                    ddr,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                ).compute(),
                fix_index(
                    pdl.merge(pdr, how=how, left_index=True, right_index=True),
                    pdl.index.dtype,
                ),
            )

            # hash join
            list_eq(
                dd.merge(
                    ddl, ddr, how=how, left_on="a", right_on="c", shuffle=shuffle_method
                ),
                pd.merge(pdl, pdr, how=how, left_on="a", right_on="c"),
            )
            list_eq(
                dd.merge(
                    ddl, ddr, how=how, left_on="b", right_on="d", shuffle=shuffle_method
                ),
                pd.merge(pdl, pdr, how=how, left_on="b", right_on="d"),
            )

            list_eq(
                dd.merge(
                    ddr,
                    ddl,
                    how=how,
                    left_on="c",
                    right_on="a",
                    shuffle=shuffle_method,
                    indicator=True,
                ),
                pd.merge(pdr, pdl, how=how, left_on="c", right_on="a", indicator=True),
            )
            list_eq(
                dd.merge(
                    ddr,
                    ddl,
                    how=how,
                    left_on="d",
                    right_on="b",
                    shuffle=shuffle_method,
                    indicator=True,
                ),
                pd.merge(pdr, pdl, how=how, left_on="d", right_on="b", indicator=True),
            )

            list_eq(
                dd.merge(
                    ddr, ddl, how=how, left_on="c", right_on="a", shuffle=shuffle_method
                ),
                pd.merge(pdr, pdl, how=how, left_on="c", right_on="a"),
            )
            list_eq(
                dd.merge(
                    ddr, ddl, how=how, left_on="d", right_on="b", shuffle=shuffle_method
                ),
                pd.merge(pdr, pdl, how=how, left_on="d", right_on="b"),
            )

            list_eq(
                ddl.merge(
                    ddr, how=how, left_on="a", right_on="c", shuffle=shuffle_method
                ),
                pdl.merge(pdr, how=how, left_on="a", right_on="c"),
            )
            list_eq(
                ddl.merge(
                    ddr, how=how, left_on="b", right_on="d", shuffle=shuffle_method
                ),
                pdl.merge(pdr, how=how, left_on="b", right_on="d"),
            )

            list_eq(
                ddr.merge(
                    ddl, how=how, left_on="c", right_on="a", shuffle=shuffle_method
                ),
                pdr.merge(pdl, how=how, left_on="c", right_on="a"),
            )
            list_eq(
                ddr.merge(
                    ddl, how=how, left_on="d", right_on="b", shuffle=shuffle_method
                ),
                pdr.merge(pdl, how=how, left_on="d", right_on="b"),
            )


@pytest.mark.slow
@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
def test_join_by_index_patterns(how, shuffle_method):
    def fix_index(out, dtype):
        # Workaround pandas bug where output dtype of empty index will be int64
        # even if input was object.
        if len(out) == 0 and not PANDAS_GT_200:
            return out.set_index(out.index.astype(dtype))
        return out

    # Similar test cases as test_merge_by_index_patterns,
    # but columns / index for join have same dtype

    pdf1l = pd.DataFrame(
        {"a": list("abcdefg"), "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )
    pdf1r = pd.DataFrame(
        {"c": list("abcdefg"), "d": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )

    pdf2l = pdf1l
    pdf2r = pd.DataFrame(
        {"c": list("gfedcba"), "d": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )

    pdf3l = pdf1l
    pdf3r = pd.DataFrame({"c": list("abdg"), "d": [5, 4, 3, 2]}, index=list("abdg"))

    pdf4l = pd.DataFrame(
        {"a": list("abcabce"), "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )
    pdf4r = pd.DataFrame({"c": list("abda"), "d": [5, 4, 3, 2]}, index=list("abdg"))

    # completely different index
    pdf5l = pd.DataFrame(
        {"a": list("lmnopqr"), "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("lmnopqr")
    )
    pdf5r = pd.DataFrame({"c": list("abcd"), "d": [5, 4, 3, 2]}, index=list("abcd"))

    pdf6l = pd.DataFrame(
        {"a": list("cdefghi"), "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("cdefghi")
    )
    pdf6r = pd.DataFrame({"c": list("abab"), "d": [5, 4, 3, 2]}, index=list("abcd"))

    pdf7l = pd.DataFrame(
        {"a": list("aabbccd"), "b": [7, 6, 5, 4, 3, 2, 1]}, index=list("abcdefg")
    )
    pdf7r = pd.DataFrame({"c": list("aabb"), "d": [5, 4, 3, 2]}, index=list("fghi"))

    for pdl, pdr in [
        (pdf1l, pdf1r),
        (pdf2l, pdf2r),
        (pdf3l, pdf3r),
        (pdf4l, pdf4r),
        (pdf5l, pdf5r),
        (pdf6l, pdf6r),
        (pdf7l, pdf7r),
    ]:
        for lpart, rpart in [(2, 2), (3, 2), (2, 3)]:
            ddl = dd.from_pandas(pdl, lpart)
            ddr = dd.from_pandas(pdr, rpart)

            assert_eq(
                ddl.join(ddr, how=how, shuffle=shuffle_method).compute(),
                fix_index(pdl.join(pdr, how=how), pdl.index.dtype),
            )

            assert_eq(
                ddr.join(ddl, how=how, shuffle=shuffle_method).compute(),
                fix_index(pdr.join(pdl, how=how), pdr.index.dtype),
            )

            assert_eq(
                ddl.join(
                    ddr, how=how, lsuffix="l", rsuffix="r", shuffle=shuffle_method
                ).compute(),
                fix_index(
                    pdl.join(pdr, how=how, lsuffix="l", rsuffix="r"), pdl.index.dtype
                ),
            )
            assert_eq(
                ddr.join(
                    ddl, how=how, lsuffix="l", rsuffix="r", shuffle=shuffle_method
                ).compute(),
                fix_index(
                    pdr.join(pdl, how=how, lsuffix="l", rsuffix="r"), pdl.index.dtype
                ),
            )

            # temporary disabled bacause pandas may incorrectly raise
            # IndexError for empty DataFrame
            # https://github.com/pydata/pandas/pull/10826

            list_eq(
                ddl.join(ddr, how=how, on="a", lsuffix="l", rsuffix="r"),
                pdl.join(pdr, how=how, on="a", lsuffix="l", rsuffix="r"),
            )

            list_eq(
                ddr.join(ddl, how=how, on="c", lsuffix="l", rsuffix="r"),
                pdr.join(pdl, how=how, on="c", lsuffix="l", rsuffix="r"),
            )

            # merge with index and columns
            list_eq(
                ddl.merge(ddr, how=how, left_on="a", right_index=True),
                pdl.merge(pdr, how=how, left_on="a", right_index=True),
            )
            list_eq(
                ddr.merge(ddl, how=how, left_on="c", right_index=True),
                pdr.merge(pdl, how=how, left_on="c", right_index=True),
            )
            list_eq(
                ddl.merge(ddr, how=how, left_index=True, right_on="c"),
                pdl.merge(pdr, how=how, left_index=True, right_on="c"),
            )
            list_eq(
                ddr.merge(ddl, how=how, left_index=True, right_on="a"),
                pdr.merge(pdl, how=how, left_index=True, right_on="a"),
            )


def test_join_gives_proper_divisions():
    # https://github.com/dask/dask/issues/8113
    df = pd.DataFrame({"a": ["a", "b", "c"]}, index=[0, 1, 2])
    ddf = dd.from_pandas(df, npartitions=1)

    right_df = pd.DataFrame({"b": [1.0, 2.0, 3.0]}, index=["a", "b", "c"])

    expected = df.join(right_df, how="inner", on="a")
    actual = ddf.join(right_df, how="inner", on="a")
    assert actual.divisions == ddf.divisions

    assert_eq(expected, actual)


@pytest.mark.slow
@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
def test_merge_by_multiple_columns(how, shuffle_method):
    def fix_index(out, dtype):
        # In Pandas 2.0, output dtype of empty index will be int64, even if input was object
        if len(out) == 0:
            return out.set_index(out.index.astype(dtype))
        return out

    # warnings here from pandas
    pdf1l = pd.DataFrame(
        {
            "a": list("abcdefghij"),
            "b": list("abcdefghij"),
            "c": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        index=list("abcdefghij"),
    )
    pdf1r = pd.DataFrame(
        {
            "d": list("abcdefghij"),
            "e": list("abcdefghij"),
            "f": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        index=list("abcdefghij"),
    )

    pdf2l = pd.DataFrame(
        {
            "a": list("abcdeabcde"),
            "b": list("abcabcabca"),
            "c": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        index=list("abcdefghij"),
    )
    pdf2r = pd.DataFrame(
        {
            "d": list("edcbaedcba"),
            "e": list("aaabbbcccd"),
            "f": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        index=list("fghijklmno"),
    )

    pdf3l = pd.DataFrame(
        {
            "a": list("aaaaaaaaaa"),
            "b": list("aaaaaaaaaa"),
            "c": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        index=list("abcdefghij"),
    )
    pdf3r = pd.DataFrame(
        {
            "d": list("aaabbbccaa"),
            "e": list("abbbbbbbbb"),
            "f": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        index=list("ABCDEFGHIJ"),
    )

    for pdl, pdr in [(pdf1l, pdf1r), (pdf2l, pdf2r), (pdf3l, pdf3r)]:
        for lpart, rpart in [(2, 2), (3, 2), (2, 3)]:
            ddl = dd.from_pandas(pdl, lpart)
            ddr = dd.from_pandas(pdr, rpart)

            assert_eq(
                ddl.join(ddr, how=how, shuffle=shuffle_method).compute(),
                fix_index(pdl.join(pdr, how=how), pdl.index.dtype),
            )
            assert_eq(
                ddr.join(ddl, how=how, shuffle=shuffle_method),
                fix_index(pdr.join(pdl, how=how), pdr.index.dtype),
            )

            assert_eq(
                dd.merge(
                    ddl,
                    ddr,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                ).compute(),
                fix_index(
                    pd.merge(pdl, pdr, how=how, left_index=True, right_index=True),
                    pdl.index.dtype,
                ),
            )
            assert_eq(
                dd.merge(
                    ddr,
                    ddl,
                    how=how,
                    left_index=True,
                    right_index=True,
                    shuffle=shuffle_method,
                ).compute(),
                fix_index(
                    pd.merge(pdr, pdl, how=how, left_index=True, right_index=True),
                    pdr.index.dtype,
                ),
            )

            # hash join
            list_eq(
                dd.merge(
                    ddl, ddr, how=how, left_on="a", right_on="d", shuffle=shuffle_method
                ),
                pd.merge(pdl, pdr, how=how, left_on="a", right_on="d"),
            )
            list_eq(
                dd.merge(
                    ddl, ddr, how=how, left_on="b", right_on="e", shuffle=shuffle_method
                ),
                pd.merge(pdl, pdr, how=how, left_on="b", right_on="e"),
            )

            list_eq(
                dd.merge(
                    ddr, ddl, how=how, left_on="d", right_on="a", shuffle=shuffle_method
                ),
                pd.merge(pdr, pdl, how=how, left_on="d", right_on="a"),
            )
            list_eq(
                dd.merge(
                    ddr, ddl, how=how, left_on="e", right_on="b", shuffle=shuffle_method
                ),
                pd.merge(pdr, pdl, how=how, left_on="e", right_on="b"),
            )

            list_eq(
                dd.merge(
                    ddl,
                    ddr,
                    how=how,
                    left_on=["a", "b"],
                    right_on=["d", "e"],
                    shuffle=shuffle_method,
                ),
                pd.merge(pdl, pdr, how=how, left_on=["a", "b"], right_on=["d", "e"]),
            )


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        dict(id_vars="int"),
        dict(value_vars="int"),
        dict(value_vars=["obj", "int"], var_name="myvar"),
        dict(id_vars="s1", value_vars=["obj", "int"], value_name="myval"),
        dict(value_vars=["obj", "s1"]),
        dict(value_vars=["s1", "s2"]),
    ],
)
def test_melt(kwargs):
    pdf = pd.DataFrame(
        {
            "obj": list("abcd") * 5,
            "s1": list("XY") * 10,
            "s2": list("abcde") * 4,
            "int": np.random.randn(20),
        }
    )
    if pa:
        # If pyarrow is installed, test that `string[pyarrow]` dtypes
        # give the same result with `pandas` and `dask`
        pdf = pdf.astype({"s1": "string[pyarrow]", "s2": "string[pyarrow]"})

    ddf = dd.from_pandas(pdf, 4)

    list_eq(dd.melt(ddf, **kwargs), pd.melt(pdf, **kwargs))
    # test again as DataFrame method
    list_eq(ddf.melt(**kwargs), pdf.melt(**kwargs))


def test_cheap_inner_merge_with_pandas_object():
    a = pd.DataFrame(
        {"x": [1, 2, 3, 4, 5, 6], "y": list("abdabd")}, index=[10, 20, 30, 40, 50, 60]
    )
    da = dd.from_pandas(a, npartitions=3)

    b = pd.DataFrame({"x": [1, 2, 3, 4], "z": list("abda")})

    dc = da.merge(b, on="x", how="inner")
    assert not hlg_layer_topological(dc.dask, -1).is_materialized()
    assert all("shuffle" not in k[0] for k in dc.dask)

    list_eq(da.merge(b, on="x", how="inner"), a.merge(b, on="x", how="inner"))


@pytest.mark.parametrize("flip", [False, True])
def test_cheap_single_partition_merge(flip):
    a = pd.DataFrame(
        {"x": [1, 2, 3, 4, 5, 6], "y": list("abdabd")}, index=[10, 20, 30, 40, 50, 60]
    )
    aa = dd.from_pandas(a, npartitions=3)

    b = pd.DataFrame({"x": [1, 2, 3, 4], "z": list("abda")})
    bb = dd.from_pandas(b, npartitions=1, sort=False)

    pd_inputs = (b, a) if flip else (a, b)
    inputs = (bb, aa) if flip else (aa, bb)

    cc = dd.merge(*inputs, on="x", how="inner")
    assert not hlg_layer_topological(cc.dask, -1).is_materialized()
    assert all("shuffle" not in k[0] for k in cc.dask)

    # Merge is input layers + a single merge operation
    input_layers = aa.dask.layers.keys() | bb.dask.layers.keys()
    output_layers = cc.dask.layers.keys()
    assert len(output_layers - input_layers) == 1

    list_eq(cc, pd.merge(*pd_inputs, on="x", how="inner"))


def test_cheap_single_partition_merge_divisions():
    a = pd.DataFrame(
        {"x": [1, 2, 3, 4, 5, 6], "y": list("abdabd")}, index=[10, 20, 30, 40, 50, 60]
    )
    aa = dd.from_pandas(a, npartitions=3)

    b = pd.DataFrame({"x": [1, 2, 3, 4], "z": list("abda")})
    bb = dd.from_pandas(b, npartitions=1, sort=False)

    actual = aa.merge(bb, on="x", how="inner")
    assert not hlg_layer_topological(actual.dask, -1).is_materialized()

    assert not actual.known_divisions
    assert_divisions(actual)

    actual = bb.merge(aa, on="x", how="inner")
    assert not actual.known_divisions
    assert_divisions(actual)


@pytest.mark.parametrize("how", ["left", "right"])
@pytest.mark.parametrize("flip", [False, True])
def test_cheap_single_parition_merge_left_right(how, flip):
    a = pd.DataFrame({"x": range(8), "z": list("ababbdda")}, index=range(8))
    aa = dd.from_pandas(a, npartitions=1)

    b = pd.DataFrame({"x": [1, 2, 3, 4], "z": list("abda")}, index=range(4))
    bb = dd.from_pandas(b, npartitions=1)

    pd_inputs = (b, a) if flip else (a, b)
    inputs = (bb, aa) if flip else (aa, bb)

    actual = dd.merge(*inputs, left_index=True, right_on="x", how=how)
    expected = pd.merge(*pd_inputs, left_index=True, right_on="x", how=how)

    assert not hlg_layer_topological(actual.dask, -1).is_materialized()
    assert_eq(actual, expected)

    actual = dd.merge(*inputs, left_on="x", right_index=True, how=how)
    expected = pd.merge(*pd_inputs, left_on="x", right_index=True, how=how)

    assert not hlg_layer_topological(actual.dask, -1).is_materialized()
    assert_eq(actual, expected)


def test_cheap_single_partition_merge_on_index():
    a = pd.DataFrame(
        {"x": [1, 2, 3, 4, 5, 6], "y": list("abdabd")}, index=[10, 20, 30, 40, 50, 60]
    )
    aa = dd.from_pandas(a, npartitions=3)

    b = pd.DataFrame({"x": [1, 2, 3, 4], "z": list("abda")})
    bb = dd.from_pandas(b, npartitions=1, sort=False)

    actual = aa.merge(bb, left_index=True, right_on="x", how="inner")
    expected = a.merge(b, left_index=True, right_on="x", how="inner")

    # Workaround https://github.com/pandas-dev/pandas/issues/26925
    # actual has the correct dtype for the index (Int64). Pandas as object-dtype
    # for empty joins.
    expected.index = expected.index.astype("int64")

    assert not hlg_layer_topological(actual.dask, -1).is_materialized()
    assert not actual.known_divisions
    assert_eq(actual, expected)

    actual = bb.merge(aa, right_index=True, left_on="x", how="inner")
    expected = b.merge(a, right_index=True, left_on="x", how="inner")
    expected.index = expected.index.astype("int64")

    assert not hlg_layer_topological(actual.dask, -1).is_materialized()
    assert not actual.known_divisions
    assert_eq(actual, expected)


def test_merge_maintains_columns():
    lhs = pd.DataFrame(
        {"A": [1, 2, 3], "B": list("abc"), "C": "foo", "D": 1.0}, columns=list("DCBA")
    )
    rhs = pd.DataFrame(
        {"G": [4, 5], "H": 6.0, "I": "bar", "B": list("ab")}, columns=list("GHIB")
    )
    ddf = dd.from_pandas(lhs, npartitions=1)
    merged = dd.merge(ddf, rhs, on="B").compute()
    assert tuple(merged.columns) == ("D", "C", "B", "A", "G", "H", "I")


def test_merge_index_without_divisions(shuffle_method):
    a = pd.DataFrame({"x": [1, 2, 3, 4, 5]}, index=[1, 2, 3, 4, 5])
    b = pd.DataFrame({"y": [1, 2, 3, 4, 5]}, index=[5, 4, 3, 2, 1])

    aa = dd.from_pandas(a, npartitions=3, sort=False)
    bb = dd.from_pandas(b, npartitions=2)

    result = aa.join(bb, how="inner", shuffle=shuffle_method)
    expected = a.join(b, how="inner")
    assert_eq(result, expected)


def test_half_indexed_dataframe_avoids_shuffle():
    a = pd.DataFrame({"x": np.random.randint(100, size=1000)})
    b = pd.DataFrame(
        {"y": np.random.randint(100, size=100)}, index=np.random.randint(100, size=100)
    )

    aa = dd.from_pandas(a, npartitions=100)
    bb = dd.from_pandas(b, npartitions=2)

    c = pd.merge(a, b, left_index=True, right_on="y")
    cc = dd.merge(aa, bb, left_index=True, right_on="y", shuffle="tasks")

    list_eq(c, cc)

    assert len(cc.dask) < 500


def test_errors_for_merge_on_frame_columns():
    a = pd.DataFrame({"x": [1, 2, 3, 4, 5]}, index=[1, 2, 3, 4, 5])
    b = pd.DataFrame({"y": [1, 2, 3, 4, 5]}, index=[5, 4, 3, 2, 1])

    aa = dd.from_pandas(a, npartitions=3, sort=False)
    bb = dd.from_pandas(b, npartitions=2)

    with pytest.raises(NotImplementedError):
        dd.merge(aa, bb, left_on="x", right_on=bb.y)

    with pytest.raises(NotImplementedError):
        dd.merge(aa, bb, left_on=aa.x, right_on=bb.y)


def test_concat_one_series():
    a = pd.Series([1, 2, 3, 4])
    aa = dd.from_pandas(a, npartitions=2, sort=False)

    c = dd.concat([aa], axis=0)
    assert isinstance(c, dd.Series)

    c = dd.concat([aa], axis=1)
    assert isinstance(c, dd.DataFrame)


def test_concat_unknown_divisions():
    a = pd.Series([1, 2, 3, 4])
    b = pd.Series([4, 3, 2, 1])
    aa = dd.from_pandas(a, npartitions=2, sort=False)
    bb = dd.from_pandas(b, npartitions=2, sort=False)

    assert not aa.known_divisions

    with pytest.warns(UserWarning):
        assert_eq(pd.concat([a, b], axis=1), dd.concat([aa, bb], axis=1))

    cc = dd.from_pandas(b, npartitions=1, sort=False)
    with pytest.raises(ValueError):
        dd.concat([aa, cc], axis=1)

    with warnings.catch_warnings(record=True) as record:
        dd.concat([aa, bb], axis=1, ignore_unknown_divisions=True)
    assert not record


def test_concat_unknown_divisions_errors():
    a = pd.Series([1, 2, 3, 4, 5, 6])
    b = pd.Series([4, 3, 2, 1])
    aa = dd.from_pandas(a, npartitions=2, sort=False)
    bb = dd.from_pandas(b, npartitions=2, sort=False)

    with pytest.raises(ValueError):
        with pytest.warns(UserWarning):  # Concat with unknown divisions
            dd.concat([aa, bb], axis=1).compute()


def test_concat2():
    dsk = {
        ("x", 0): pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
        ("x", 1): pd.DataFrame({"a": [4, 5, 6], "b": [3, 2, 1]}),
        ("x", 2): pd.DataFrame({"a": [7, 8, 9], "b": [0, 0, 0]}),
    }
    meta = make_meta({"a": "i8", "b": "i8"}, parent_meta=pd.DataFrame())
    a = dd.DataFrame(dsk, "x", meta, [None, None])
    dsk = {
        ("y", 0): pd.DataFrame({"a": [10, 20, 30], "b": [40, 50, 60]}),
        ("y", 1): pd.DataFrame({"a": [40, 50, 60], "b": [30, 20, 10]}),
        ("y", 2): pd.DataFrame({"a": [70, 80, 90], "b": [0, 0, 0]}),
    }
    b = dd.DataFrame(dsk, "y", meta, [None, None])

    dsk = {
        ("y", 0): pd.DataFrame({"b": [10, 20, 30], "c": [40, 50, 60]}),
        ("y", 1): pd.DataFrame({"b": [40, 50, 60], "c": [30, 20, 10]}),
    }
    meta = make_meta({"b": "i8", "c": "i8"}, parent_meta=pd.DataFrame())
    c = dd.DataFrame(dsk, "y", meta, [None, None])

    dsk = {
        ("y", 0): pd.DataFrame(
            {"b": [10, 20, 30], "c": [40, 50, 60], "d": [70, 80, 90]}
        ),
        ("y", 1): pd.DataFrame(
            {"b": [40, 50, 60], "c": [30, 20, 10], "d": [90, 80, 70]}, index=[3, 4, 5]
        ),
    }
    meta = make_meta(
        {"b": "i8", "c": "i8", "d": "i8"},
        index=pd.Index([], "i8"),
        parent_meta=pd.DataFrame(),
    )
    d = dd.DataFrame(dsk, "y", meta, [0, 3, 5])

    cases = [[a, b], [a, c], [a, d]]
    assert dd.concat([a]) is a
    for case in cases:
        pdcase = [_c.compute() for _c in case]
        expected = pd.concat(pdcase, sort=False)
        result = dd.concat(case)
        assert result.npartitions == case[0].npartitions + case[1].npartitions
        assert result.divisions == (None,) * (result.npartitions + 1)
        assert_eq(expected, result)
        assert set(result.dask) == set(dd.concat(case).dask)

        expected = pd.concat(pdcase, join="inner", sort=False)
        result = dd.concat(case, join="inner")
        assert result.npartitions == case[0].npartitions + case[1].npartitions
        assert result.divisions == (None,) * (result.npartitions + 1)
        assert_eq(result, result)
        assert set(result.dask) == set(dd.concat(case, join="inner").dask)


def test_concat3():
    pdf1 = pd.DataFrame(
        np.random.randn(6, 5), columns=list("ABCDE"), index=list("abcdef")
    )
    pdf2 = pd.DataFrame(
        np.random.randn(6, 5), columns=list("ABCFG"), index=list("ghijkl")
    )
    pdf3 = pd.DataFrame(
        np.random.randn(6, 5), columns=list("ABCHI"), index=list("mnopqr")
    )
    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)

    expected = pd.concat([pdf1, pdf2], sort=False)
    result = dd.concat([ddf1, ddf2])
    assert result.divisions == ddf1.divisions[:-1] + ddf2.divisions
    assert result.npartitions == ddf1.npartitions + ddf2.npartitions
    assert_eq(result, expected)

    assert_eq(
        dd.concat([ddf1, ddf2], interleave_partitions=True), pd.concat([pdf1, pdf2])
    )

    expected = pd.concat([pdf1, pdf2, pdf3], sort=False)
    result = dd.concat([ddf1, ddf2, ddf3])
    assert result.divisions == (
        ddf1.divisions[:-1] + ddf2.divisions[:-1] + ddf3.divisions
    )
    assert result.npartitions == (
        ddf1.npartitions + ddf2.npartitions + ddf3.npartitions
    )
    assert_eq(result, expected)

    assert_eq(
        dd.concat([ddf1, ddf2, ddf3], interleave_partitions=True),
        pd.concat([pdf1, pdf2, pdf3]),
    )


def test_concat4_interleave_partitions():
    pdf1 = pd.DataFrame(
        np.random.randn(10, 5), columns=list("ABCDE"), index=list("abcdefghij")
    )
    pdf2 = pd.DataFrame(
        np.random.randn(13, 5), columns=list("ABCDE"), index=list("fghijklmnopqr")
    )
    pdf3 = pd.DataFrame(
        np.random.randn(13, 6), columns=list("CDEXYZ"), index=list("fghijklmnopqr")
    )

    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)

    msg = (
        "All inputs have known divisions which cannot be "
        "concatenated in order. Specify "
        "interleave_partitions=True to ignore order"
    )

    cases = [
        [ddf1, ddf1],
        [ddf1, ddf2],
        [ddf1, ddf3],
        [ddf2, ddf1],
        [ddf2, ddf3],
        [ddf3, ddf1],
        [ddf3, ddf2],
    ]
    for case in cases:
        pdcase = [c.compute() for c in case]

        assert_eq(
            dd.concat(case, interleave_partitions=True), pd.concat(pdcase, sort=False)
        )
        assert_eq(
            dd.concat(case, join="inner", interleave_partitions=True),
            pd.concat(pdcase, join="inner", sort=False),
        )

    msg = "'join' must be 'inner' or 'outer'"
    with pytest.raises(ValueError) as err:
        dd.concat([ddf1, ddf1], join="invalid", interleave_partitions=True)
    assert msg in str(err.value)


def test_concat5():
    pdf1 = pd.DataFrame(
        np.random.randn(7, 5), columns=list("ABCDE"), index=list("abcdefg")
    )
    pdf2 = pd.DataFrame(
        np.random.randn(7, 6), columns=list("FGHIJK"), index=list("abcdefg")
    )
    pdf3 = pd.DataFrame(
        np.random.randn(7, 6), columns=list("FGHIJK"), index=list("cdefghi")
    )
    pdf4 = pd.DataFrame(
        np.random.randn(7, 5), columns=list("FGHAB"), index=list("cdefghi")
    )
    pdf5 = pd.DataFrame(
        np.random.randn(7, 5), columns=list("FGHAB"), index=list("fklmnop")
    )

    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)
    ddf4 = dd.from_pandas(pdf4, 2)
    ddf5 = dd.from_pandas(pdf5, 3)

    cases = [
        [ddf1, ddf2],
        [ddf1, ddf3],
        [ddf1, ddf4],
        [ddf1, ddf5],
        [ddf3, ddf4],
        [ddf3, ddf5],
        [ddf5, ddf1, ddf4],
        [ddf5, ddf3],
        [ddf1.A, ddf4.A],
        [ddf2.F, ddf3.F],
        [ddf4.A, ddf5.A],
        [ddf1.A, ddf4.F],
        [ddf2.F, ddf3.H],
        [ddf4.A, ddf5.B],
        [ddf1, ddf4.A],
        [ddf3.F, ddf2],
        [ddf5, ddf1.A, ddf2],
    ]

    for case in cases:
        pdcase = [c.compute() for c in case]

        assert_eq(
            dd.concat(case, interleave_partitions=True),
            pd.concat(pdcase, sort=False),
        )

        assert_eq(
            dd.concat(case, join="inner", interleave_partitions=True),
            pd.concat(pdcase, join="inner"),
        )

        assert_eq(dd.concat(case, axis=1), pd.concat(pdcase, axis=1))

        assert_eq(
            dd.concat(case, axis=1, join="inner"),
            pd.concat(pdcase, axis=1, join="inner"),
        )

    # Dask + pandas
    cases = [
        [ddf1, pdf2],
        [ddf1, pdf3],
        [pdf1, ddf4],
        [pdf1.A, ddf4.A],
        [ddf2.F, pdf3.F],
        [ddf1, pdf4.A],
        [ddf3.F, pdf2],
        [ddf2, pdf1, ddf3.F],
    ]

    for case in cases:
        pdcase = [c.compute() if isinstance(c, _Frame) else c for c in case]

        assert_eq(dd.concat(case, interleave_partitions=True), pd.concat(pdcase))

        assert_eq(
            dd.concat(case, join="inner", interleave_partitions=True),
            pd.concat(pdcase, join="inner"),
        )

        assert_eq(dd.concat(case, axis=1), pd.concat(pdcase, axis=1))

        assert_eq(
            dd.concat(case, axis=1, join="inner"),
            pd.concat(pdcase, axis=1, join="inner"),
        )


@pytest.mark.parametrize(
    "known, cat_index, divisions",
    [
        (True, True, False),
        (True, False, True),
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (False, False, False),
    ],
)
def test_concat_categorical(known, cat_index, divisions):
    frames = [
        pd.DataFrame(
            {
                "w": list("xxxxx"),
                "x": np.arange(5),
                "y": list("abcbc"),
                "z": np.arange(5, dtype="f8"),
            }
        ),
        pd.DataFrame(
            {
                "w": list("yyyyy"),
                "x": np.arange(5, 10),
                "y": list("abbba"),
                "z": np.arange(5, 10, dtype="f8"),
            }
        ),
        pd.DataFrame(
            {
                "w": list("zzzzz"),
                "x": np.arange(10, 15),
                "y": list("bcbcc"),
                "z": np.arange(10, 15, dtype="f8"),
            }
        ),
    ]
    for df in frames:
        df.w = df.w.astype("category")
        df.y = df.y.astype("category")

    if cat_index:
        frames = [df.set_index(df.y) for df in frames]

    dframes = [dd.from_pandas(p, npartitions=2, sort=divisions) for p in frames]

    if not known:
        dframes[0]._meta = clear_known_categories(dframes[0]._meta, ["y"], index=True)

    def check_and_return(ddfs, dfs, join):
        sol = concat(dfs, join=join)
        res = dd.concat(ddfs, join=join, interleave_partitions=divisions)
        assert_eq(res, sol)
        if known:
            parts = compute_as_if_collection(
                dd.DataFrame, res.dask, res.__dask_keys__()
            )
            for p in [i.iloc[:0] for i in parts]:
                check_meta(res._meta, p)  # will error if schemas don't align
        assert not cat_index or has_known_categories(res.index) == known
        return res

    for join in ["inner", "outer"]:
        # Frame
        res = check_and_return(dframes, frames, join)
        assert has_known_categories(res.w)
        assert has_known_categories(res.y) == known

        # Series
        res = check_and_return([i.y for i in dframes], [i.y for i in frames], join)
        assert has_known_categories(res) == known

        # Non-cat series with cat index
        if cat_index:
            res = check_and_return([i.x for i in dframes], [i.x for i in frames], join)

        # Partition missing columns
        res = check_and_return(
            [dframes[0][["x", "y"]]] + dframes[1:],
            [frames[0][["x", "y"]]] + frames[1:],
            join,
        )
        assert not hasattr(res, "w") or has_known_categories(res.w)
        assert has_known_categories(res.y) == known


def test_concat_categorical_mixed_simple():
    a = pd.Series(["a", "b", "c"], dtype="category")
    b = pd.Series(["a", "b"], dtype="category")
    da = dd.from_pandas(a, 2).cat.as_unknown().to_frame("A")
    db = dd.from_pandas(b, 2).to_frame("A")

    expected = concat([a.to_frame("A"), b.to_frame("A")])
    result = dd.concat([da, db])
    assert_eq(result, expected)


def test_concat_datetimeindex():
    # https://github.com/dask/dask/issues/2932
    b2 = pd.DataFrame(
        {"x": ["a"]},
        index=pd.DatetimeIndex(["2015-03-24 00:00:16"], dtype="datetime64[ns]"),
    )
    b3 = pd.DataFrame(
        {"x": ["c"]},
        index=pd.DatetimeIndex(["2015-03-29 00:00:44"], dtype="datetime64[ns]"),
    )

    b2["x"] = b2.x.astype("category").cat.set_categories(["a", "c"])
    b3["x"] = b3.x.astype("category").cat.set_categories(["a", "c"])

    db2 = dd.from_pandas(b2, 1)
    db3 = dd.from_pandas(b3, 1)

    result = concat([b2.iloc[:0], b3.iloc[:0]])
    assert result.index.dtype == "M8[ns]"

    result = dd.concat([db2, db3])
    expected = pd.concat([b2, b3])
    assert_eq(result, expected)


def check_append_with_warning(dask_obj, dask_append, pandas_obj, pandas_append):
    if PANDAS_GT_140:
        with pytest.warns(FutureWarning, match="append method is deprecated"):
            expected = pandas_obj.append(pandas_append)
            result = dask_obj.append(dask_append)
            assert_eq(result, expected)
    else:
        expected = pandas_obj.append(pandas_append)
        result = dask_obj.append(dask_append)
        assert_eq(result, expected)

    return result


@pytest.mark.skipif(PANDAS_GT_200, reason="pandas removed append")
def test_append():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6], "b": [1, 2, 3, 4, 5, 6]})
    df2 = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5, 6], "b": [1, 2, 3, 4, 5, 6]}, index=[6, 7, 8, 9, 10, 11]
    )
    df3 = pd.DataFrame(
        {"b": [1, 2, 3, 4, 5, 6], "c": [1, 2, 3, 4, 5, 6]}, index=[6, 7, 8, 9, 10, 11]
    )

    ddf = dd.from_pandas(df, 2)
    ddf2 = dd.from_pandas(df2, 2)
    ddf3 = dd.from_pandas(df3, 2)

    s = pd.Series([7, 8], name=6, index=["a", "b"])

    check_append_with_warning(ddf, s, df, s)
    check_append_with_warning(ddf, ddf2, df, df2)
    check_append_with_warning(ddf.a, ddf2.a, df.a, df2.a)

    # different columns
    check_append_with_warning(ddf, ddf3, df, df3)
    check_append_with_warning(ddf.a, ddf3.b, df.a, df3.b)

    # dask + pandas
    check_append_with_warning(ddf, df2, df, df2)
    check_append_with_warning(ddf.a, df2.a, df.a, df2.a)

    check_append_with_warning(ddf, df3, df, df3)
    check_append_with_warning(ddf.a, df3.b, df.a, df3.b)


@pytest.mark.skipif(PANDAS_GT_200, reason="pandas removed append")
def test_append2():
    dsk = {
        ("x", 0): pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
        ("x", 1): pd.DataFrame({"a": [4, 5, 6], "b": [3, 2, 1]}),
        ("x", 2): pd.DataFrame({"a": [7, 8, 9], "b": [0, 0, 0]}),
    }
    meta = make_meta({"a": "i8", "b": "i8"}, parent_meta=pd.DataFrame())
    ddf1 = dd.DataFrame(dsk, "x", meta, [None, None])
    df1 = ddf1.compute()

    dsk = {
        ("y", 0): pd.DataFrame({"a": [10, 20, 30], "b": [40, 50, 60]}),
        ("y", 1): pd.DataFrame({"a": [40, 50, 60], "b": [30, 20, 10]}),
        ("y", 2): pd.DataFrame({"a": [70, 80, 90], "b": [0, 0, 0]}),
    }
    ddf2 = dd.DataFrame(dsk, "y", meta, [None, None])
    df2 = ddf2.compute()

    dsk = {
        ("y", 0): pd.DataFrame({"b": [10, 20, 30], "c": [40, 50, 60]}),
        ("y", 1): pd.DataFrame({"b": [40, 50, 60], "c": [30, 20, 10]}),
    }
    meta = make_meta({"b": "i8", "c": "i8"}, parent_meta=pd.DataFrame())
    ddf3 = dd.DataFrame(dsk, "y", meta, [None, None])
    df3 = ddf3.compute()

    check_append_with_warning(ddf1, ddf2, df1, df2)
    check_append_with_warning(ddf2, ddf1, df2, df1)

    # different columns
    check_append_with_warning(ddf1, ddf3, df1, df3)
    check_append_with_warning(ddf3, ddf1, df3, df1)

    # Dask + pandas
    check_append_with_warning(ddf1, df2, df1, df2)
    check_append_with_warning(ddf2, df1, df2, df1)

    # different columns
    check_append_with_warning(ddf1, df3, df1, df3)
    check_append_with_warning(ddf3, df1, df3, df1)


@pytest.mark.skipif(PANDAS_GT_200, reason="pandas removed append")
def test_append_categorical():
    frames = [
        pd.DataFrame(
            {
                "x": np.arange(5, 10),
                "y": list("abbba"),
                "z": np.arange(5, 10, dtype="f8"),
            }
        ),
        pd.DataFrame(
            {
                "x": np.arange(10, 15),
                "y": list("bcbcc"),
                "z": np.arange(10, 15, dtype="f8"),
            }
        ),
    ]
    frames2 = []
    for df in frames:
        df.y = df.y.astype("category")
        df2 = df.copy()
        df2.y = df2.y.cat.set_categories(list("abc"))
        df.index = df.y
        frames2.append(df2.set_index(df2.y))

    df1, df2 = frames2

    for known in [True, False]:
        dframes = [dd.from_pandas(p, npartitions=2, sort=False) for p in frames]
        if not known:
            dframes[0]._meta = clear_known_categories(
                dframes[0]._meta, ["y"], index=True
            )
        ddf1, ddf2 = dframes

        res = check_append_with_warning(ddf1, ddf2, df1, df2)

        assert has_known_categories(res.index) == known
        assert has_known_categories(res.y) == known

        res = check_append_with_warning(ddf1.y, ddf2.y, df1.y, df2.y)
        assert has_known_categories(res.index) == known
        assert has_known_categories(res) == known

        res = check_append_with_warning(ddf1.index, ddf2.index, df1.index, df2.index)
        assert has_known_categories(res) == known


@pytest.mark.skipif(PANDAS_GT_200, reason="pandas removed append")
def test_append_lose_divisions():
    df = pd.DataFrame({"x": [1, 2, 3, 4]}, index=[1, 2, 3, 4])
    ddf = dd.from_pandas(df, npartitions=2)

    res = check_append_with_warning(ddf, ddf, df, df)
    assert res.known_divisions is False


def test_singleton_divisions():
    df = pd.DataFrame({"x": [1, 1, 1]}, index=[1, 2, 3])
    ddf = dd.from_pandas(df, npartitions=2)
    ddf2 = ddf.set_index("x")

    joined = ddf2.join(ddf2, rsuffix="r")
    assert joined.divisions == (1, 1)
    joined.compute()


def test_repartition_repeated_divisions():
    df = pd.DataFrame({"x": [0, 0, 0, 0]})
    ddf = dd.from_pandas(df, npartitions=2).set_index("x")

    ddf2 = ddf.repartition(divisions=(0, 0), force=True)
    assert_eq(ddf2, df.set_index("x"))


def test_multi_duplicate_divisions():
    df1 = pd.DataFrame({"x": [0, 0, 0, 0]})
    df2 = pd.DataFrame({"x": [0]})

    ddf1 = dd.from_pandas(df1, npartitions=2).set_index("x")
    ddf2 = dd.from_pandas(df2, npartitions=1).set_index("x")
    assert ddf1.npartitions <= 2
    assert len(ddf1) == len(df1)

    r1 = ddf1.merge(ddf2, how="left", left_index=True, right_index=True)

    sf1 = df1.set_index("x")
    sf2 = df2.set_index("x")
    r2 = sf1.merge(sf2, how="left", left_index=True, right_index=True)

    assert_eq(r1, r2)


def test_merge_outer_empty():
    # Issue #5470 bug reproducer
    k_clusters = 3
    df = pd.DataFrame(
        {"user": ["A", "B", "C", "D", "E", "F"], "cluster": [1, 1, 2, 2, 3, 3]}
    )
    df = dd.from_pandas(df, npartitions=10)
    empty_df = dd.from_pandas(pd.DataFrame(), npartitions=10)

    for x in range(0, k_clusters + 1):
        assert_eq(
            dd.merge(empty_df, df[df.cluster == x], how="outer"),
            df[df.cluster == x],
            check_index=False,
            check_divisions=False,
        )


def test_dtype_equality_warning():
    # https://github.com/dask/dask/issues/5437
    df1 = pd.DataFrame({"a": np.array([1, 2], dtype=np.dtype(np.int64))})
    df2 = pd.DataFrame({"a": np.array([1, 2], dtype=np.dtype(np.longlong))})

    with warnings.catch_warnings(record=True) as record:
        dd.multi.warn_dtype_mismatch(df1, df2, "a", "a")
    assert not record


@pytest.mark.parametrize(
    "engine", ["pandas", pytest.param("cudf", marks=pytest.mark.gpu)]
)
def test_groupby_concat_cudf(engine):
    # NOTE: Issue #5643 Reproducer

    size = 6
    npartitions = 3
    d1 = pd.DataFrame(
        {
            "a": np.random.permutation(np.arange(size)),
            "b": np.random.randint(100, size=size),
        }
    )
    d2 = pd.DataFrame(
        {
            "c": np.random.permutation(np.arange(size)),
            "d": np.random.randint(100, size=size),
        }
    )

    if engine == "cudf":
        # NOTE: engine == "cudf" requires cudf/dask_cudf,
        # will be skipped by non-GPU CI.

        cudf = pytest.importorskip("cudf")
        dask_cudf = pytest.importorskip("dask_cudf")

        d1 = cudf.from_pandas(d1)
        d2 = cudf.from_pandas(d2)
        dd1 = dask_cudf.from_cudf(d1, npartitions)
        dd2 = dask_cudf.from_cudf(d2, npartitions)
    else:
        dd1 = dd.from_pandas(d1, npartitions)
        dd2 = dd.from_pandas(d2, npartitions)

    grouped_d1 = d1.groupby(["a"]).sum()
    grouped_d2 = d2.groupby(["c"]).sum()
    res = concat([grouped_d1, grouped_d2], axis=1)

    grouped_dd1 = dd1.groupby(["a"]).sum()
    grouped_dd2 = dd2.groupby(["c"]).sum()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        res_dd = dd.concat([grouped_dd1, grouped_dd2], axis=1)

    assert_eq(res_dd.compute().sort_index(), res.sort_index())


@pytest.mark.parametrize("ordered", [True, False])
def test_concat_ignore_order(ordered):
    pdf1 = pd.DataFrame(
        {
            "x": pd.Categorical(
                ["a", "b", "c", "a"], categories=["a", "b", "c"], ordered=ordered
            )
        }
    )
    ddf1 = dd.from_pandas(pdf1, 2)
    pdf2 = pd.DataFrame(
        {
            "x": pd.Categorical(
                ["c", "b", "a"], categories=["c", "b", "a"], ordered=ordered
            )
        }
    )
    ddf2 = dd.from_pandas(pdf2, 2)
    expected = pd.concat([pdf1, pdf2])
    expected["x"] = expected["x"].astype("category")
    result = dd.concat([ddf1, ddf2], ignore_order=True)
    assert_eq(result, expected)


@pytest.mark.parametrize(
    "dtype",
    [
        "Int64",
        "Float64",
        pytest.param(
            "int64[pyarrow]",
            marks=pytest.mark.skipif(
                pa is None or not PANDAS_GT_150,
                reason="Support for ArrowDtypes requires pyarrow and pandas>=1.5.0",
            ),
        ),
        pytest.param(
            "float64[pyarrow]",
            marks=pytest.mark.skipif(
                pa is None or not PANDAS_GT_150,
                reason="Support for ArrowDtypes requires pyarrow and pandas>=1.5.0",
            ),
        ),
    ],
)
def test_nullable_types_merge(dtype):
    df1 = pd.DataFrame({"a": [1, 2, 3], "b": [1, 1, 3], "c": list("aab")})
    df2 = pd.DataFrame({"a": [1, 2, 3], "e": [1, 1, 3], "f": list("aab")})
    df1["a"] = df1["a"].astype(dtype)
    df2["a"] = df2["a"].astype(dtype)

    ddf1 = dd.from_pandas(df1, npartitions=2)
    ddf2 = dd.from_pandas(df2, npartitions=2)

    expect = df1.merge(df2, on="a")
    actual = ddf1.merge(ddf2, on="a")
    assert_eq(expect, actual, check_index=False)

    expect = pd.merge(df1, df2, on="a")
    actual = dd.merge(ddf1, ddf2, on="a")
    assert_eq(expect, actual, check_index=False)


def test_categorical_join():
    # https://github.com/dask/dask/issues/6134
    df = pd.DataFrame(
        {
            "join_col": ["a", "a", "b", "b"],
            "a": [0, 0, 10, 10],
        }
    )
    df2 = pd.DataFrame({"b": [1, 2, 1, 2]}, index=["a", "a", "b", "b"])

    ddf = dd.from_pandas(df, npartitions=2)
    ddf2 = dd.from_pandas(df2, npartitions=1)
    ddf["join_col"] = ddf["join_col"].astype("category")
    ddf2.index = ddf2.index.astype("category")

    expected = ddf.compute().join(ddf2.compute(), on="join_col", how="left")

    actual_dask = ddf.join(ddf2, on="join_col", how="left")
    assert actual_dask.join_col.dtype == "category"

    actual = actual_dask.compute()
    assert actual.join_col.dtype == "category"
    assert assert_eq(expected, actual)


def test_categorical_merge_with_columns_missing_from_left():
    df1 = pd.DataFrame({"A": [0, 1], "B": pd.Categorical(["a", "b"])})
    df2 = pd.DataFrame({"C": pd.Categorical(["a", "b"])})

    expected = pd.merge(df2, df1, left_index=True, right_on="A")

    ddf1 = dd.from_pandas(df1, npartitions=2)
    ddf2 = dd.from_pandas(df2, npartitions=2)

    actual = dd.merge(ddf2, ddf1, left_index=True, right_on="A").compute()
    assert actual.C.dtype == "category"
    assert actual.B.dtype == "category"
    assert actual.A.dtype == "int64"
    assert actual.index.dtype == "int64"
    assert assert_eq(expected, actual)


def test_categorical_merge_with_merge_column_cat_in_one_and_not_other_upcasts():
    df1 = pd.DataFrame({"A": pd.Categorical([0, 1]), "B": pd.Categorical(["a", "b"])})
    df2 = pd.DataFrame({"C": pd.Categorical(["a", "b"])})

    expected = pd.merge(df2, df1, left_index=True, right_on="A")

    ddf1 = dd.from_pandas(df1, npartitions=2)
    ddf2 = dd.from_pandas(df2, npartitions=2)

    actual = dd.merge(ddf2, ddf1, left_index=True, right_on="A").compute()
    assert actual.C.dtype == "category"
    assert actual.B.dtype == "category"
    assert actual.A.dtype == "int64"
    assert actual.index.dtype == "int64"
    assert assert_eq(expected, actual)


def test_categorical_merge_retains_category_dtype():
    # https://github.com/dask/dask/issues/6142
    a = pd.DataFrame({"A": [0, 1, 2, 3], "B": [4, 5, 6, 7]})
    b = pd.DataFrame({"A": [0, 1, 2, 4], "C": [4, 5, 7, 7]})

    df1 = dd.from_pandas(a, 2)
    df1["A"] = df1.A.astype("category")

    df2 = dd.from_pandas(b, 2)
    df2["A"] = df2.A.astype("category")

    actual_dask = df1.merge(df2, on="A")
    assert actual_dask.A.dtype == "category"

    actual = actual_dask.compute()
    assert actual.A.dtype == "category"


def test_categorical_merge_does_not_raise_setting_with_copy_warning():
    # https://github.com/dask/dask/issues/7087
    df1 = pd.DataFrame(data={"A": ["a", "b", "c"]}, index=["s", "v", "w"])
    df2 = pd.DataFrame(data={"B": ["t", "d", "i"]}, index=["v", "w", "r"])

    ddf1 = dd.from_pandas(df1, npartitions=1)

    df2 = df2.astype({"B": "category"})
    assert_eq(df1.join(df2), ddf1.join(df2))


@pytest.mark.parametrize("how", ["inner", "left", "right"])
@pytest.mark.parametrize("npartitions", [28, 32])
@pytest.mark.parametrize("base", ["lg", "sm"])
def test_merge_tasks_large_to_small(how, npartitions, base):
    size_lg = 3000
    size_sm = 300
    npartitions_lg = 30
    npartitions_sm = 3
    broadcast_bias = 1.0  # Prioritize broadcast

    lg = pd.DataFrame(
        {
            "x": np.random.choice(np.arange(100), size_lg),
            "y": np.arange(size_lg),
        }
    )
    ddf_lg = dd.from_pandas(lg, npartitions=npartitions_lg)

    sm = pd.DataFrame(
        {
            "x": np.random.choice(np.arange(100), size_sm),
            "y": np.arange(size_sm),
        }
    )
    ddf_sm = dd.from_pandas(sm, npartitions=npartitions_sm)

    if base == "lg":
        left = lg
        ddf_left = ddf_lg
        right = sm
        ddf_right = ddf_sm
    else:
        left = sm
        ddf_left = ddf_sm
        right = lg
        ddf_right = ddf_lg

    dd_result = dd.merge(
        ddf_left,
        ddf_right,
        on="y",
        how=how,
        npartitions=npartitions,
        broadcast=broadcast_bias,
        shuffle="tasks",
    )
    pd_result = pd.merge(left, right, on="y", how=how)

    # Make sure `on` dtypes match
    dd_result["y"] = dd_result["y"].astype(np.int32)
    pd_result["y"] = pd_result["y"].astype(np.int32)

    if npartitions:
        assert dd_result.npartitions == npartitions

    assert_eq(
        dd_result.compute().sort_values("y"),
        pd_result.sort_values("y"),
        check_index=False,
    )


@pytest.mark.parametrize("shuffle", [None, "tasks"])
def test_broadcast_true(shuffle):
    # Check that broadcast=True is satisfied
    # See: https://github.com/dask/dask/issues/9851
    left = dd.from_dict({"a": [1, 2] * 80, "b_left": range(160)}, npartitions=16)
    right = dd.from_dict({"a": [2, 1] * 10, "b_right": range(20)}, npartitions=2)

    result = dd.merge(left, right, broadcast=True, shuffle=shuffle)
    assert hlg_layer(result.dask, "bcast-join")


@pytest.mark.parametrize("how", ["right", "inner"])
def test_pairwise_rejects_unsupported_join_types(how):
    base_df = dd.from_pandas(
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[0, 1, 3]), 3
    )
    dfs = [
        dd.from_pandas(
            pd.DataFrame({"a": [4, 5, 6], "b": [3, 2, 1]}, index=[5, 6, 8]), 3
        ),
        dd.from_pandas(
            pd.DataFrame({"a": [7, 8, 9], "b": [0, 0, 0]}, index=[9, 9, 9]), 3
        ),
    ]

    with pytest.raises(ValueError) as e:
        base_df.join(dfs, how=how)
    e.match("merge_multi only supports left or outer joins")


@pytest.mark.parametrize("how", ["left", "outer"])
@pytest.mark.parametrize("npartitions_base", [1, 2, 3])
@pytest.mark.parametrize("npartitions_other", [1, 2, 3])
def test_pairwise_merge_results_in_identical_output_df(
    how, npartitions_base, npartitions_other
):
    dfs_to_merge = []
    for i in range(10):
        df = pd.DataFrame(
            {
                f"{i}A": [5, 6, 7, 8],
                f"{i}B": [4, 3, 2, 1],
            },
            index=[0, 1, 2, 3],
        )
        ddf = dd.from_pandas(df, npartitions_other)
        dfs_to_merge.append(ddf)

    ddf_loop = dd.from_pandas(pd.DataFrame(index=[0, 1, 3]), npartitions_base)
    for ddf in dfs_to_merge:
        ddf_loop = ddf_loop.join(ddf, how=how)

    ddf_pairwise = dd.from_pandas(pd.DataFrame(index=[0, 1, 3]), npartitions_base)

    ddf_pairwise = ddf_pairwise.join(dfs_to_merge, how=how)

    assert_eq(ddf_pairwise, ddf_loop)


def test_merge_empty_result_index_dtype():
    df1 = pd.DataFrame({"a": [1, 2]}, index=["a", "b"])
    df2 = pd.DataFrame({"b": [1, 2]}, index=["x", "y"])

    df1_d = dd.from_pandas(df1, npartitions=1)
    df2_d = dd.from_pandas(df2, npartitions=1)
    pandas_result = df1.join(df2, how="inner")
    dask_result = df1_d.join(df2_d, how="inner").compute()

    assert_eq(dask_result, pandas_result)

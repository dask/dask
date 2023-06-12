import os

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

from dask_expr import from_dask_dataframe, from_pandas, optimize, read_csv, read_parquet
from dask_expr.expr import Expr, Lengths, Literal
from dask_expr.io import ReadParquet
from dask_expr.reductions import Len


def _make_file(dir, format="parquet", df=None):
    fn = os.path.join(str(dir), f"myfile.{format}")
    if df is None:
        df = pd.DataFrame({c: range(10) for c in "abcde"})
    if format == "csv":
        df.to_csv(fn)
    elif format == "parquet":
        df.to_parquet(fn)
    else:
        ValueError(f"{format} not a supported format")
    return fn


def df(fn):
    return read_parquet(fn, columns=["a", "b", "c"])


def df_bc(fn):
    return read_parquet(fn, columns=["b", "c"])


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            # Add -> Mul
            lambda fn: df(fn) + df(fn),
            lambda fn: 2 * df(fn),
        ),
        (
            # Column projection
            lambda fn: df(fn)[["b", "c"]],
            lambda fn: read_parquet(fn, columns=["b", "c"]),
        ),
        (
            # Compound
            lambda fn: 3 * (df(fn) + df(fn))[["b", "c"]],
            lambda fn: 6 * df_bc(fn),
        ),
        (
            # Traverse Sum
            lambda fn: df(fn).sum()[["b", "c"]],
            lambda fn: df_bc(fn).sum(),
        ),
        (
            # Respect Sum keywords
            lambda fn: df(fn).sum(numeric_only=True)[["b", "c"]],
            lambda fn: df_bc(fn).sum(numeric_only=True),
        ),
    ],
)
def test_optimize(tmpdir, input, expected):
    fn = _make_file(tmpdir, format="parquet")
    result = optimize(input(fn), fuse=False)
    assert str(result.expr) == str(expected(fn).expr)


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_io_fusion(tmpdir, fmt):
    fn = _make_file(tmpdir, format=fmt)
    if fmt == "parquet":
        df = read_parquet(fn)
    else:
        df = read_csv(fn)
    df2 = optimize(df[["a", "b"]] + 1, fuse=True)

    # All tasks should be fused for each partition
    assert len(df2.dask) == df2.npartitions
    assert_eq(df2, df[["a", "b"]] + 1)


def test_predicate_pushdown(tmpdir):
    original = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, format="parquet", df=original)
    df = read_parquet(fn)
    assert_eq(df, original)
    x = df[df.a == 5][df.c > 20]["b"]
    y = optimize(x, fuse=False)
    assert isinstance(y.expr, ReadParquet)
    assert ("a", "==", 5) in y.expr.operand("filters")[0]
    assert ("c", ">", 20) in y.expr.operand("filters")[0]
    assert list(y.columns) == ["b"]

    # Check computed result
    y_result = y.compute()
    assert y_result.name == "b"
    assert len(y_result) == 6
    assert all(y_result == 4)


def test_predicate_pushdown_compound(tmpdir):
    pdf = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, format="parquet", df=pdf)
    df = read_parquet(fn)

    # Test AND
    x = df[(df.a == 5) & (df.c > 20)]["b"]
    y = optimize(x, fuse=False)
    assert isinstance(y.expr, ReadParquet)
    assert {("c", ">", 20), ("a", "==", 5)} == set(y.filters[0])
    assert_eq(
        y,
        pdf[(pdf.a == 5) & (pdf.c > 20)]["b"],
        check_index=False,
    )

    # Test OR
    x = df[(df.a == 5) | (df.c > 20)][df.b != 0]["b"]
    y = optimize(x, fuse=False)
    assert isinstance(y.expr, ReadParquet)
    filters = [set(y.filters[0]), set(y.filters[1])]
    assert {("c", ">", 20), ("b", "!=", 0)} in filters
    assert {("a", "==", 5), ("b", "!=", 0)} in filters
    assert_eq(
        y,
        pdf[(pdf.a == 5) | (pdf.c > 20)][pdf.b != 0]["b"],
        check_index=False,
    )

    # Test OR and AND
    x = df[((df.a == 5) | (df.c > 20)) & (df.b != 0)]["b"]
    z = optimize(x, fuse=False)
    assert isinstance(z.expr, ReadParquet)
    filters = [set(z.filters[0]), set(z.filters[1])]
    assert {("c", ">", 20), ("b", "!=", 0)} in filters
    assert {("a", "==", 5), ("b", "!=", 0)} in filters
    assert_eq(y, z)


@pytest.mark.parametrize("fmt", ["parquet", "csv", "pandas"])
def test_io_culling(tmpdir, fmt):
    pdf = pd.DataFrame({c: range(10) for c in "abcde"})
    if fmt == "parquet":
        dd.from_pandas(pdf, 2).to_parquet(tmpdir)
        df = read_parquet(tmpdir)
    elif fmt == "csv":
        dd.from_pandas(pdf, 2).to_csv(tmpdir)
        df = read_csv(tmpdir + "/*")
    else:
        df = from_pandas(pdf, 2)
    df = (df[["a", "b"]] + 1).partitions[1]
    df2 = optimize(df)

    # All tasks should be fused for the single output partition
    assert df2.npartitions == 1
    assert len(df2.dask) == df2.npartitions
    expected = pdf.iloc[5:][["a", "b"]] + 1
    assert_eq(df2, expected, check_index=False)

    def _check_culling(expr, partitions):
        """CHeck that _partitions is set to the expected value"""
        for dep in expr.dependencies():
            _check_culling(dep, partitions)
        if "_partitions" in expr._parameters:
            assert expr._partitions == partitions

    # Check that we still get culling without fusion
    df3 = optimize(df, fuse=False)
    _check_culling(df3.expr, [1])
    assert_eq(df3, expected, check_index=False)


@pytest.mark.parametrize("sort", [True, False])
def test_from_pandas(sort):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2, sort=sort)

    assert df.divisions == (0, 3, 5) if sort else (None,) * 3
    assert_eq(df, pdf)


def test_parquet_complex_filters(tmpdir):
    df = read_parquet(_make_file(tmpdir))
    pdf = df.compute()
    got = df["a"][df["b"] > df["b"].mean()]
    expect = pdf["a"][pdf["b"] > pdf["b"].mean()]

    assert_eq(got, expect)
    assert_eq(got.optimize(), expect)


def test_parquet_len(tmpdir):
    df = read_parquet(_make_file(tmpdir))
    pdf = df.compute()

    assert len(df[df.a > 5]) == len(pdf[pdf.a > 5])

    s = (df["b"] + 1).astype("Int32")
    assert len(s) == len(pdf)

    assert isinstance(Len(s.expr).optimize(), Literal)
    assert isinstance(Lengths(s.expr).optimize(), Literal)


@pytest.mark.parametrize("optimize", [True, False])
def test_from_dask_dataframe(optimize):
    ddf = dd.from_dict({"a": range(100)}, npartitions=10)
    df = from_dask_dataframe(ddf, optimize=optimize)
    assert isinstance(df.expr, Expr)
    assert_eq(df, ddf)


@pytest.mark.parametrize("optimize", [True, False])
def test_to_dask_dataframe(optimize):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2)
    ddf = df.to_dask_dataframe(optimize=optimize)
    assert isinstance(ddf, dd.DataFrame)
    assert_eq(df, ddf)


@pytest.mark.parametrize("write_metadata_file", [True, False])
def test_to_parquet(tmpdir, write_metadata_file):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2)

    # Check basic parquet round trip
    df.to_parquet(tmpdir, write_metadata_file=write_metadata_file)
    df2 = read_parquet(tmpdir, calculate_divisions=True)
    assert_eq(df, df2)

    # Check overwrite behavior
    df["new"] = df["x"] + 1
    df.to_parquet(tmpdir, overwrite=True, write_metadata_file=write_metadata_file)
    df2 = read_parquet(tmpdir, calculate_divisions=True)
    assert_eq(df, df2)

    # Check that we cannot overwrite a path we are
    # reading from in the same graph
    with pytest.raises(ValueError, match="Cannot overwrite"):
        df2.to_parquet(tmpdir, overwrite=True)

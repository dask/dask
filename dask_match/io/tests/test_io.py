import os

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

from dask_match import from_pandas, optimize, read_csv, read_parquet


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
    from dask_match.io import ReadParquet

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
    assert ("a", "==", 5) in y.expr.operand("filters") or (
        "a",
        "==",
        5,
    ) in y.expr.operand("filters")
    assert ("c", ">", 20) in y.expr.operand("filters")
    assert list(y.columns) == ["b"]

    # Check computed result
    y_result = y.compute()
    assert list(y_result.columns) == ["b"]
    assert len(y_result["b"]) == 6
    assert all(y_result["b"] == 4)


@pytest.mark.parametrize("fmt", ["parquet", "csv", "pandas"])
def test_io_culling(tmpdir, fmt):
    pdf = pd.DataFrame({c: range(10) for c in "abcde"})
    if fmt == "parquet":
        dd.from_pandas(pdf, 2).to_parquet(tmpdir)
        df = read_parquet(tmpdir)
    elif fmt == "parquet":
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

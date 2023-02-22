import os

import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from dask.utils import M

from dask_match import ReadCSV, from_pandas, optimize, read_parquet


def test_basic():
    x = read_parquet("myfile.parquet", columns=("a", "b", "c"))
    y = ReadCSV("myfile.csv", usecols=("a", "d", "e"))

    z = x + y
    result = z[("a", "b", "d")].sum(skipna="foo")
    assert result.skipna == "foo"
    assert result.operands[0].columns == ("a", "b", "d")

    x + 1
    1 + x


df = read_parquet("myfile.parquet", columns=["a", "b", "c"])
df_bc = read_parquet("myfile.parquet", columns=["b", "c"])


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            # Add -> Mul
            df + df,
            2 * df,
        ),
        (
            # Column projection
            df[("b", "c")],
            read_parquet("myfile.parquet", columns=("b", "c")),
        ),
        (
            # Compound
            3 * (df + df)[["b", "c"]],
            6 * df_bc,
        ),
        (
            # Traverse Sum
            df.sum()[["b", "c"]],
            df_bc.sum(),
        ),
        (
            # Respect Sum keywords
            df.sum(numeric_only=True)[["b", "c"]],
            df_bc.sum(numeric_only=True),
        ),
        # (
        #     # Traverse Max
        #     df.max()[["b", "c"]],
        #     df_bc.max(),
        # ),
    ],
)
def test_optimize(input, expected):
    result = optimize(input)
    assert str(result) == str(expected)


def test_meta_divisions_name():
    a = pd.DataFrame({"x": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]})
    df = 2 * from_pandas(a, npartitions=2)
    assert list(df.columns) == list(a.columns)
    assert df.npartitions == 2

    assert df.x.sum()._meta == 0
    assert df.x.sum().npartitions == 1

    assert "mul" in df._name
    assert "sum" in df.sum()._name


def test_meta_blockwise():
    a = pd.DataFrame({"x": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]})
    b = pd.DataFrame({"z": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]})

    aa = from_pandas(a, npartitions=2)
    bb = from_pandas(b, npartitions=2)

    cc = 2 * aa - 3 * bb
    assert set(cc.columns) == {"x", "y", "z"}


def test_dask():
    df = pd.DataFrame({"x": range(100), "y": range(100)})
    df["y"] = df.y * 10.0

    ddf = from_pandas(df, npartitions=10)
    assert (ddf.x + ddf.y).npartitions == 10
    z = (ddf.x + ddf.y).sum()

    assert z.compute() == (df.x + df.y).sum()


@pytest.mark.parametrize(
    "func",
    [
        M.max,
        M.min,
        M.sum,
        M.count,
        pytest.param(
            M.mean,
            marks=pytest.mark.skip(reason="scalars don't work yet"),
        ),
        lambda df: df.size,
    ],
)
def test_reductions(func):
    df = pd.DataFrame({"x": range(100), "y": range(100)})
    df["y"] = df.y * 10.0
    ddf = from_pandas(df, npartitions=10)

    assert_eq(func(ddf), func(df))
    assert_eq(func(ddf.x), func(df.x))


def test_mode():
    df = pd.DataFrame({"x": [1, 2, 3, 1, 2]})
    ddf = from_pandas(df, npartitions=3)

    assert_eq(ddf.x.mode(), df.x.mode())


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.x > 10,
        lambda df: df.x + 20 > df.y,
        lambda df: 10 < df.x,
        lambda df: 10 <= df.x,
        lambda df: 10 == df.x,
        lambda df: df.x < df.y,
        lambda df: df.x > df.y,
        lambda df: df.x == df.y,
        lambda df: df.x != df.y,
    ],
)
def test_conditionals(func):
    df = pd.DataFrame({"x": range(100), "y": range(100)})
    df["y"] = df.y * 2.0
    ddf = from_pandas(df, npartitions=10)

    assert_eq(func(df), func(ddf))


def test_predicate_pushdown(tmpdir):
    from dask_match.io.parquet import ReadParquet

    fn = os.path.join(str(tmpdir), "myfile.parquet")
    original = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0] * 50,
            "c": range(50),
        }
    )
    original.to_parquet(fn)

    df = read_parquet(fn)
    assert_eq(df, original)
    x = df[df.a == 5][df.c > 20]["b"]
    y = optimize(x)
    assert isinstance(y, ReadParquet)
    assert ("==", "a", 5) in y.filters or ("==", 5, "a") in y.filters
    assert (">", "c", 20) in y.filters
    assert y.columns == ["b"]


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.astype(int),
        lambda df: df.apply(lambda row, x, y=10: row * x + y, x=2),
        lambda df: df[df.x > 5],
    ],
)
def test_blockwise(func):
    df = pd.DataFrame({"x": range(20), "y": range(20)})
    df["y"] = df.y * 2.0
    ddf = from_pandas(df, npartitions=3)

    assert_eq(func(df), func(ddf))


def test_repr():
    df = pd.DataFrame({"x": range(20), "y": range(20)})
    df = from_pandas(df, npartitions=1)

    assert "+ 1" in str(df + 1)
    assert "+ 1" in repr(df + 1)

    s = (df["x"] + 1).sum(skipna=False)
    assert '["x"]' in s or "['x']" in s
    assert "+ 1" in s
    assert "sum(skipna=False)" in s

    assert "ReadParquet" in read_parquet("filename")


def test_columns_traverse_filters():
    df = pd.DataFrame({"x": range(20), "y": range(20), "z": range(20)})
    df = from_pandas(df, npartitions=2)

    expr = df[df.x > 5].y
    result = optimize(expr)
    expected = df.y[df.x > 5]

    assert str(result) == str(expected)


def test_persist():
    df = pd.DataFrame({"x": range(20), "y": range(20), "z": range(20)})
    ddf = from_pandas(df, npartitions=2)

    a = ddf + 2
    b = a.persist()

    assert_eq(a, b)
    assert len(a.__dask_graph__()) > len(b.__dask_graph__())

    assert len(b.__dask_graph__()) == b.npartitions

    assert_eq(b.y.sum(), (df + 2).y.sum())

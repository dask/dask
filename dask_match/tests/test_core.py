import pandas as pd
import pytest

from dask_match import ReadCSV, ReadParquet, from_pandas, optimize


def test_basic():
    x = ReadParquet("myfile.parquet", columns=("a", "b", "c"))
    y = ReadCSV("myfile.csv", usecols=("a", "d", "e"))

    z = x + y
    result = z[("a", "b", "d")].sum(skipna=False)
    assert result.skipna is False
    assert result.operands[0].columns == ("a", "b", "d")


df = ReadParquet("myfile.parquet", columns=("a", "b", "c"))
df_bc = ReadParquet("myfile.parquet", columns=("b", "c"))


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
            ReadParquet("myfile.parquet", columns=("b", "c")),
        ),
        (
            # Compound
            3 * (df + df)[("b", "c")],
            6 * df_bc,
        ),
        (
            # Traverse Sum
            df.sum()[("b", "c")],
            df_bc.sum(),
        ),
        (
            # Respect Sum keywords
            df.sum(numeric_only=True)[("b", "c")],
            df_bc.sum(numeric_only=True),
        ),
    ],
)
def test_optimize(input, expected):
    result = optimize(input)
    assert result == expected


def test_meta_divisions_name():
    a = pd.DataFrame({"x": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]})
    df = 2 * from_pandas(a, npartitions=2)
    assert list(df.columns) == list(a.columns)
    assert df.npartitions == 2

    assert df["x"].sum()._meta == 0
    assert df["x"].sum().npartitions == 1

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

    ddf = from_pandas(df, npartitions=1)
    assert (ddf["x"] + ddf["y"]).npartitions == 1
    z = (ddf["x"] + ddf["y"]).sum()

    assert z.compute() == (df.x + df.y).sum()

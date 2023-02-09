import pytest

from dask_match import ReadCSV, ReadParquet, optimize


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
            # Compound
            df.sum()[("b", "c")],
            df_bc.sum(),
        ),
        (
            # Compound
            df.sum(numeric_only=True)[("b", "c")],
            df_bc.sum(numeric_only=True),
        ),
    ],
)
def test_optimize(input, expected):
    result = optimize(input)
    assert result == expected

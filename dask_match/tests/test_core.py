from dask_match import ReadParquet, ReadCSV, optimize, Mul
import pytest

def test_basic():
    x = ReadParquet("myfile.parquet", columns=("a", "b", "c"))
    y = ReadCSV("myfile.csv", usecols=("a", "d", "e"))

    z = x + y
    result = z[("a", "b", "d")].sum(skipna=False)
    assert result.skipna is False
    assert result.operands[0].columns == ("a", "b", "d")


df = ReadParquet("myfile.parquet", columns=("a", "b", "c"))


@pytest.mark.parametrize("input,expected", [
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
])
def test_optimize(input, expected):
    result = optimize(input)
    assert result == expected

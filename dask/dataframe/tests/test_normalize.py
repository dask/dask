import pytest
import dask.dataframe as dd
import pandas as pd
from dask.dataframe.transformations.normalize import normalize_range

def test_normalize_range_basic():
    # Create a small Pandas DataFrame
    pdf = pd.DataFrame({
        "a": [10, 20, 30, 40, 50],
        "b": [5, 15, 25, 35, 45]
    })

    # Convert to Dask DataFrame
    ddf = dd.from_pandas(pdf, npartitions=1)

    # Apply normalization
    result = normalize_range(ddf, ["a", "b"], 0, 1).compute()

    # Each column should now range between 0 and 1
    for col in ["a", "b"]:
        assert abs(result[col].min() - 0.0) < 1e-9
        assert abs(result[col].max() - 1.0) < 1e-9

def test_normalize_range_partial_columns():
    pdf = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [100, 200, 300]
    })
    ddf = dd.from_pandas(pdf, npartitions=1)

    # Normalize only column 'b'
    result = normalize_range(ddf, ["b"], 0, 10).compute()

    assert abs(result["b"].min() - 0.0) < 1e-9
    assert abs(result["b"].max() - 10.0) < 1e-9
    assert result["a"].equals(pdf["a"])  # column 'a' unchanged

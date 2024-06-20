from __future__ import annotations

import numpy as np
import pandas as pd

import dask.dataframe as dd
from dask.dataframe.utils import assert_eq
import dask.dataframe.methods as methods
from dask.dataframe._compat import PANDAS_GE_140


def test_assign_not_modifying_array_inplace():
    df = pd.DataFrame({"a": [1, 2, 3], "b": 1.5})
    result = methods.assign(df, "a", 5)
    assert not np.shares_memory(df["a"].values, result["a"].values)
    if PANDAS_GE_140:
        assert np.shares_memory(df["b"].values, result["b"].values)


def test_cumulative_empty_partitions():
    pdf = pd.DataFrame(
        {"x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]},
        index=pd.date_range("1995-02-26", periods=8, freq="5min"),
        dtype=float,
    )
    pdf2 = pdf.drop(pdf.between_time("00:10", "00:20").index)

    df = dd.from_pandas(pdf, npartitions=8)
    df2 = dd.from_pandas(pdf2, npartitions=1).repartition(df.divisions)

    assert_eq(df2.cumprod(), pdf2.cumprod())
    assert_eq(df2.cumsum(), pdf2.cumsum())
    assert_eq(df2.max(), pdf2.max())
    assert_eq(df2.min(), pdf2.min())

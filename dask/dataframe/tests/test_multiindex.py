import numpy as np
import pandas as pd
import pytest

import dask.dataframe as dd
from dask.dataframe import partitionquantiles


def test_from_pandas():
    df = pd.DataFrame(
        {"A": 1},
        index=pd.MultiIndex.from_product([["a", "b"], [0, 1, 2]], names=["l0", "l1"]),
    )
    ddf = dd.from_pandas(df, 2)
    assert ddf.index.names == df.index.names

    dd.utils.assert_eq(ddf, df)

    with pytest.raises(NotImplementedError):
        ddf.index.names = ["a", "b"]


def test_remove_unused_levels():
    levels = [np.array(["a", "b"]), np.array(["a", "b"])]
    codes = [np.array([0, 0]), np.array([0, 1])]
    a = pd.MultiIndex(levels, codes)
    b = dd.from_pandas(pd.Series(1, index=a), 2).index
    dd.utils.assert_eq(a, b)
    expected = a.remove_unused_levels()
    result = b.remove_unused_levels().compute()
    dd.utils.assert_eq(result, expected)


def test_set_index():
    df = pd.DataFrame(
        {
            "A": ["a", "b", "c", "a", "b", "c"],
            "B": [0, 0, 0, 1, 1, 1],
            "C": [0, 1, 2, 3, 4, 5],
        }
    )
    ddf = dd.from_pandas(df, 2)
    expected = df.set_index(["A", "B"]).sort_index()
    result = ddf.set_index(["A", "B"])
    dd.utils.assert_eq(result, expected)

    result = result.reset_index()
    dd.utils.assert_eq(result, expected.reset_index(), check_index=False)


def test_partition_quantiles_object():
    s = pd.Series([("a", 0), ("b", 0), ("c", 0), ("a", 1), ("b", 1), ("c", 1)])
    ds = dd.from_pandas(s, 2)
    result = partitionquantiles.partition_quantiles(ds, 2)
    expected = pd.Series([("a", 0), ("b", 0), ("c", 1)], index=[0.0, 0.5, 1.0])
    dd.utils.assert_eq(result, expected)

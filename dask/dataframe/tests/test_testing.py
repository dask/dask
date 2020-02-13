import pandas as pd
import dask.dataframe as dd

import pytest


def test_assert_divisions():
    df = pd.DataFrame({"A": [1, 2, 3]})
    ddf = dd.from_pandas(df, 2)

    dd.utils.assert_divisions(ddf)

    ddf.divisions = (1, 0)

    with pytest.raises(AssertionError):
        dd.utils.assert_divisions(ddf)


def test_assert_divisions_mutli():
    df = pd.DataFrame(
        {"A": [1, 2, 3]}, index=pd.MultiIndex.from_product([["a"], [1, 2, 3]])
    )
    ddf = dd.from_pandas(df, 2)

    dd.utils.assert_divisions(ddf)

    ddf.divisions = (("a", 1), ("a", 0))

    with pytest.raises(AssertionError):
        dd.utils.assert_divisions(ddf)

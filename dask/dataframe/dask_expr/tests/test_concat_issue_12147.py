from __future__ import annotations

from dask.dataframe.dask_expr import concat, from_pandas
from dask.dataframe.dask_expr.tests._util import _backend_library, assert_eq

pd = _backend_library()


def test_concat_empty_list_issue_12147():
    """Regression test for issue #12147: empty concat should not IndexError.

    This test is intentionally in a separate file to make the regression
    explicit and independent from other concat tests.
    """
    # Test case 1: All frames are empty
    empty_df1 = pd.DataFrame({"a": pd.Series([], dtype="int64")})
    empty_df2 = pd.DataFrame({"a": pd.Series([], dtype="int64")})
    expected = pd.concat([empty_df1, empty_df2])

    empty_ddf1 = from_pandas(empty_df1, npartitions=1)
    empty_ddf2 = from_pandas(empty_df2, npartitions=1)
    result = concat([empty_ddf1, empty_ddf2])

    assert_eq(expected, result)

    # Test case 2: Mix of empty and non-empty, but result is empty
    df = pd.DataFrame({"a": [1, 2, 3]}, dtype="int64")
    empty_df = pd.DataFrame({"a": pd.Series([], dtype="int64")})

    ddf = from_pandas(df, npartitions=1)
    empty_ddf = from_pandas(empty_df, npartitions=1)

    result = concat([ddf, empty_ddf])
    expected = pd.concat([df, empty_df])
    assert_eq(expected, result)

    result = concat([empty_ddf, ddf])
    expected = pd.concat([empty_df, df])
    assert_eq(expected, result)

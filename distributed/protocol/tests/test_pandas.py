import numpy as np
import pandas as pd
import pytest

from dask.dataframe.utils import assert_eq

from distributed.protocol import serialize, deserialize, decompress


dfs = [
    pd.DataFrame({}),
    pd.DataFrame({"x": [1, 2, 3]}),
    pd.DataFrame({"x": [1.0, 2.0, 3.0]}),
    pd.DataFrame({0: [1, 2, 3]}),
    pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [4.0, 5.0, 6.0]}),
    pd.DataFrame({"x": [1.0, 2.0, 3.0]}, index=pd.Index([4, 5, 6], name="bar")),
    pd.Series([1.0, 2.0, 3.0]),
    pd.Series([1.0, 2.0, 3.0], name="foo"),
    pd.Series([1.0, 2.0, 3.0], name="foo", index=[4, 5, 6]),
    pd.Series([1.0, 2.0, 3.0], name="foo", index=pd.Index([4, 5, 6], name="bar")),
    pd.DataFrame({"x": ["a", "b", "c"]}),
    pd.DataFrame({"x": [b"a", b"b", b"c"]}),
    pd.DataFrame({"x": pd.Categorical(["a", "b", "a"], ordered=True)}),
    pd.DataFrame({"x": pd.Categorical(["a", "b", "a"], ordered=False)}),
    pd.Index(pd.Categorical(["a"], categories=["a", "b"], ordered=True)),
    pd.date_range("2000", periods=12, freq="B"),
    pd.RangeIndex(10),
    pd.DataFrame(
        "a",
        index=pd.Index(["a", "b", "c", "d"], name="a"),
        columns=pd.Index(["A", "B", "C", "D"], name="columns"),
    ),
    pd.DataFrame(
        np.random.randn(10, 5), columns=list("ABCDE"), index=list("abcdefghij")
    ),
    pd.DataFrame(
        np.random.randn(10, 5), columns=list("ABCDE"), index=list("abcdefghij")
    ).where(lambda x: x > 0),
    pd.DataFrame(
        {
            "a": [0.0, 0.1],
            "B": [0.0, 1.0],
            "C": ["a", "b"],
            "D": pd.to_datetime(["2000", "2001"]),
        }
    ),
    pd.Series(["a", "b", "c"], index=["a", "b", "c"]),
    pd.DataFrame(
        np.random.randn(10, 5),
        columns=list("ABCDE"),
        index=pd.period_range("2000", periods=10, freq="B"),
    ),
    pd.DataFrame(
        np.random.randn(10, 5),
        columns=list("ABCDE"),
        index=pd.date_range("2000", periods=10, freq="B"),
    ),
    pd.Series(
        np.random.randn(10), name="a", index=pd.date_range("2000", periods=10, freq="B")
    ),
    pd.Index(["סשםקה7ךשץא", "8טלכז6לרפל"]),
]


@pytest.mark.parametrize("df", dfs)
def test_dumps_serialize_numpy(df):
    header, frames = serialize(df)
    if "compression" in header:
        frames = decompress(header, frames)
    df2 = deserialize(header, frames)

    assert_eq(df, df2)

from __future__ import print_function, division, absolute_import


import pandas as pd
import pandas.util.testing as tm
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
    tm.makeCategoricalIndex(),
    tm.makeCustomDataframe(5, 3),
    tm.makeDataFrame(),
    tm.makeDateIndex(),
    tm.makeMissingDataframe(),
    tm.makeMixedDataFrame(),
    tm.makeObjectSeries(),
    tm.makePeriodFrame(),
    tm.makeRangeIndex(),
    tm.makeTimeDataFrame(),
    tm.makeTimeSeries(),
    tm.makeUnicodeIndex(),
]


@pytest.mark.parametrize("df", dfs)
def test_dumps_serialize_numpy(df):
    header, frames = serialize(df)
    if "compression" in header:
        frames = decompress(header, frames)
    df2 = deserialize(header, frames)

    assert_eq(df, df2)

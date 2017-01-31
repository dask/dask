from __future__ import print_function, division, absolute_import

from zlib import crc32

import pandas as pd
import pandas.util.testing as tm
import pytest

from dask.dataframe.utils import assert_eq

from distributed.protocol import (serialize, deserialize, decompress, dumps,
        loads, to_serialize)
from distributed.protocol.utils import BIG_BYTES_SHARD_SIZE
from distributed.utils import tmpfile
from distributed.utils_test import slow
from distributed.protocol.compression import maybe_compress

dfs = [
    pd.DataFrame({}),
    pd.DataFrame({'x': [1, 2, 3]}),
    pd.DataFrame({'x': [1., 2., 3.]}),
    pd.DataFrame({0: [1, 2, 3]}),
    pd.DataFrame({'x': [1., 2., 3.], 'y': [4., 5., 6.]}),
    pd.DataFrame({'x': [1., 2., 3.]}, index=pd.Index([4, 5, 6], name='bar')),
    pd.Series([1., 2., 3.]),
    pd.Series([1., 2., 3.], name='foo'),
    pd.Series([1., 2., 3.], name='foo',
              index=[4, 5, 6]),
    pd.Series([1., 2., 3.], name='foo',
              index=pd.Index([4, 5, 6], name='bar')),
    pd.DataFrame({'x': ['a', 'b', 'c']}),
    pd.DataFrame({'x': [b'a', b'b', b'c']}),
    pd.DataFrame({'x': pd.Categorical(['a', 'b', 'a'], ordered=True)}),
    pd.DataFrame({'x': pd.Categorical(['a', 'b', 'a'], ordered=False)}),
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


@pytest.mark.parametrize('df', dfs)
def test_dumps_serialize_numpy(df):
    header, frames = serialize(df)
    if 'compression' in header:
        frames = decompress(header, frames)
    df2 = deserialize(header, frames)

    assert_eq(df, df2)

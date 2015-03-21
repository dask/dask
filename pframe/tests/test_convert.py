from pframe.convert import to_blocks, from_blocks
import pandas as pd
import numpy as np


def test_blocks():
    N = 10000
    df = pd.DataFrame({'A' : np.arange(N),
                       'B' : np.random.randn(N),
                       'C' : pd.date_range('20130101',freq='s',periods=N),
                       'D' : np.random.randn(N) + 100, 'E' : 'foobar'})


    data = to_blocks(df)

    df2 = from_blocks(**data)

    assert list(df.columns) == list(df2.columns)
    assert [a.values.ctypes.data == b.values.ctypes.data
            for a, b in zip(df._data.blocks, df2._data.blocks)]
    assert df.index.values.ctypes.data == df2.index.values.ctypes.data
    assert df2.equals(df)

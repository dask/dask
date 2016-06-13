import numpy as np
import pandas as pd
from dask.dataframe.utils import shard_df_on_index, nonempty_sample_df


def test_shard_df_on_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])

    result = list(shard_df_on_index(df, [20, 50]))
    assert list(result[0].index) == [10]
    assert list(result[1].index) == [20, 30, 40]
    assert list(result[2].index) == [50, 60]


def test_nonempty_sample_df():
    df1 = pd.DataFrame({'A': pd.Categorical(['Alice', 'Bob', 'Carol']),
                        'B': list('abc'),
                        'C': 'bar',
                        'D': 3.0,
                        'E': pd.Timestamp('2016-01-01'),
                        'F': pd.date_range('2016-01-01', periods=3,
                                           tz='America/New_York'),
                        'G': pd.Timedelta('1 hours'),
                        'H': np.void(b' ')},
                       columns=list('DCBAHGFE'))
    df2 = df1.iloc[0:0]
    df3 = nonempty_sample_df(df2)
    assert df3['A'][0] == 'Alice'
    assert df3['B'][0] == 'foo'
    assert df3['C'][0] == 'foo'
    assert df3['D'][0] == 1.0
    assert df3['E'][0] == pd.Timestamp('1970-01-01 00:00:00')
    assert df3['F'][0] == pd.Timestamp('1970-01-01 00:00:00',
                                       tz='America/New_York')
    assert df3['G'][0] == pd.Timedelta('1 days')
    assert df3['H'][0] == 'foo'

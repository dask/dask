import numpy as np
import pandas as pd
from dask.dataframe.utils import shard_df_on_index, nonempty_pd


def test_shard_df_on_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])

    result = list(shard_df_on_index(df, [20, 50]))
    assert list(result[0].index) == [10]
    assert list(result[1].index) == [20, 30, 40]
    assert list(result[2].index) == [50, 60]


def test_nonempty_pd():
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
    df3 = nonempty_pd(df2)
    assert df3['A'][0] == 'Alice'
    assert df3['B'][0] == 'foo'
    assert df3['C'][0] == 'foo'
    assert df3['D'][0] == 1.0
    assert df3['E'][0] == pd.Timestamp('1970-01-01 00:00:00')
    assert df3['F'][0] == pd.Timestamp('1970-01-01 00:00:00',
                                       tz='America/New_York')
    assert df3['G'][0] == pd.Timedelta('1 days')
    assert df3['H'][0] == 'foo'

    s = nonempty_pd(df2['A'])
    assert (df3['A'] == s).all()


def test_nonempty_pd_index():
    idx = pd.RangeIndex(1, name='foo')
    res = nonempty_pd(idx)
    assert type(res) is pd.RangeIndex
    assert res.name == idx.name

    idx = pd.Int64Index([1], name='foo')
    res = nonempty_pd(idx)
    assert type(res) is pd.Int64Index
    assert res.name == idx.name

    idx = pd.Index(['a'], name='foo')
    res = nonempty_pd(idx)
    assert type(res) is pd.Index
    assert res.name == idx.name

    idx = pd.DatetimeIndex(['1970-01-01'], freq='d',
                           tz='America/New_York', name='foo')
    res = nonempty_pd(idx)
    assert type(res) is pd.DatetimeIndex
    assert res.tz == idx.tz
    assert res.freq == idx.freq
    assert res.name == idx.name

    idx = pd.PeriodIndex(['1970-01-01'], freq='d', name='foo')
    res = nonempty_pd(idx)
    assert type(res) is pd.PeriodIndex
    assert res.freq == idx.freq
    assert res.name == idx.name

    idx = pd.TimedeltaIndex([np.timedelta64(1, 'D')], freq='d', name='foo')
    res = nonempty_pd(idx)
    assert type(res) is pd.TimedeltaIndex
    assert res.freq == idx.freq
    assert res.name == idx.name

    idx = pd.CategoricalIndex(['a'], ['a', 'b'], ordered=True, name='foo')
    res = nonempty_pd(idx)
    assert type(res) is pd.CategoricalIndex
    assert (res.categories == idx.categories).all()
    assert res.ordered == idx.ordered
    assert res.name == idx.name

    levels = [pd.Int64Index([1], name='a'),
              pd.Float64Index([1.0], name='b')]
    idx = pd.MultiIndex(levels=levels, labels=[[0], [0]], names=['a', 'b'])
    res = nonempty_pd(idx)
    assert type(res) is pd.MultiIndex
    for idx1, idx2 in zip(idx.levels, res.levels):
        assert type(idx1) is type(idx2)
        assert idx1.name == idx2.name
    assert res.names == idx.names

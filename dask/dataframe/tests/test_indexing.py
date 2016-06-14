
import pandas as pd
import pandas.util.testing as tm
import numpy as np

import dask
from dask.utils import raises
import dask.dataframe as dd

from dask.dataframe.core import _coerce_loc_index
from dask.dataframe.utils import eq


dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 5, 9, 9])
full = d.compute()


def test_loc():
    assert d.loc[3:8].divisions[0] == 3
    assert d.loc[3:8].divisions[-1] == 8

    assert d.loc[5].divisions == (5, 5)

    assert eq(d.loc[5], full.loc[5:5])
    assert eq(d.loc[3:8], full.loc[3:8])
    assert eq(d.loc[:8], full.loc[:8])
    assert eq(d.loc[3:], full.loc[3:])

    assert eq(d.a.loc[5], full.a.loc[5:5])
    assert eq(d.a.loc[3:8], full.a.loc[3:8])
    assert eq(d.a.loc[:8], full.a.loc[:8])
    assert eq(d.a.loc[3:], full.a.loc[3:])

    assert raises(KeyError, lambda: d.loc[1000])
    assert eq(d.loc[1000:], full.loc[1000:])
    assert eq(d.loc[-2000:-1000], full.loc[-2000:-1000])

    assert sorted(d.loc[5].dask) == sorted(d.loc[5].dask)
    assert sorted(d.loc[5].dask) != sorted(d.loc[6].dask)


def test_loc_non_informative_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4]}, index=[10, 20, 30, 40])
    ddf = dd.from_pandas(df, npartitions=2, sort=True)
    ddf.divisions = (None,) * 3
    assert not ddf.known_divisions

    ddf.loc[20:30].compute(get=dask.get)

    assert eq(ddf.loc[20:30], df.loc[20:30])

    df = pd.DataFrame({'x': [1, 2, 3, 4]}, index=[10, 20, 20, 40])
    ddf = dd.from_pandas(df, npartitions=2, sort=True)
    assert eq(ddf.loc[20], df.loc[20:20])


def test_loc_with_text_dates():
    A = tm.makeTimeSeries(10).iloc[:5]
    B = tm.makeTimeSeries(10).iloc[5:]
    s = dd.Series({('df', 0): A, ('df', 1): B}, 'df', None,
                  [A.index.min(), B.index.min(), B.index.max()])

    assert s.loc['2000': '2010'].divisions == s.divisions
    assert eq(s.loc['2000': '2010'], s)
    assert len(s.loc['2000-01-03': '2000-01-05'].compute()) == 3


def test_loc_with_series():
    assert eq(d.loc[d.a % 2 == 0], full.loc[full.a % 2 == 0])

    assert sorted(d.loc[d.a % 2].dask) == sorted(d.loc[d.a % 2].dask)
    assert sorted(d.loc[d.a % 2].dask) != sorted(d.loc[d.a % 3].dask)


def test_getitem():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                       'B': [9, 8, 7, 6, 5, 4, 3, 2, 1],
                       'C': [True, False, True] * 3},
                      columns=list('ABC'))
    ddf = dd.from_pandas(df, 2)
    assert eq(ddf['A'], df['A'])
    # check cache consistency
    tm.assert_series_equal(ddf['A']._pd, ddf._pd['A'])

    assert eq(ddf[['A', 'B']], df[['A', 'B']])
    tm.assert_frame_equal(ddf[['A', 'B']]._pd, ddf._pd[['A', 'B']])

    assert eq(ddf[ddf.C], df[df.C])
    tm.assert_series_equal(ddf.C._pd, ddf._pd.C)

    assert eq(ddf[ddf.C.repartition([0, 2, 5, 8])], df[df.C])

    assert raises(KeyError, lambda: df['X'])
    assert raises(KeyError, lambda: df[['A', 'X']])
    assert raises(AttributeError, lambda: df.X)

    # not str/unicode
    df = pd.DataFrame(np.random.randn(10, 5))
    ddf = dd.from_pandas(df, 2)
    assert eq(ddf[0], df[0])
    assert eq(ddf[[1, 2]], df[[1, 2]])

    assert raises(KeyError, lambda: df[8])
    assert raises(KeyError, lambda: df[[1, 8]])


def test_getitem_slice():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                       'B': [9, 8, 7, 6, 5, 4, 3, 2, 1],
                       'C': [True, False, True] * 3},
                      index=list('abcdefghi'))
    ddf = dd.from_pandas(df, 3)
    assert eq(ddf['a':'e'], df['a':'e'])
    assert eq(ddf['a':'b'], df['a':'b'])
    assert eq(ddf['f':], df['f':])


def test_loc_on_numpy_datetimes():
    df = pd.DataFrame({'x': [1, 2, 3]},
                      index=list(map(np.datetime64, ['2014', '2015', '2016'])))
    a = dd.from_pandas(df, 2)
    a.divisions = list(map(np.datetime64, a.divisions))

    assert eq(a.loc['2014': '2015'], a.loc['2014': '2015'])


def test_loc_on_pandas_datetimes():
    df = pd.DataFrame({'x': [1, 2, 3]},
                      index=list(map(pd.Timestamp, ['2014', '2015', '2016'])))
    a = dd.from_pandas(df, 2)
    a.divisions = list(map(pd.Timestamp, a.divisions))

    assert eq(a.loc['2014': '2015'], a.loc['2014': '2015'])


def test_coerce_loc_index():
    for t in [pd.Timestamp, np.datetime64]:
        assert isinstance(_coerce_loc_index([t('2014')], '2014'), t)


def test_loc_timestamp_str():

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='H', periods=100))
    ddf = dd.from_pandas(df, 10)

    # partial string slice
    assert eq(df.loc['2011-01-02'],
              ddf.loc['2011-01-02'])
    assert eq(df.loc['2011-01-02':'2011-01-10'],
              ddf.loc['2011-01-02':'2011-01-10'])
    # same reso, dask result is always DataFrame
    assert eq(df.loc['2011-01-02 10:00'].to_frame().T,
              ddf.loc['2011-01-02 10:00'])

    # series
    assert eq(df.A.loc['2011-01-02'],
              ddf.A.loc['2011-01-02'])
    assert eq(df.A.loc['2011-01-02':'2011-01-10'],
              ddf.A.loc['2011-01-02':'2011-01-10'])

    # slice with timestamp (dask result must be DataFrame)
    assert eq(df.loc[pd.Timestamp('2011-01-02')].to_frame().T,
              ddf.loc[pd.Timestamp('2011-01-02')])
    assert eq(df.loc[pd.Timestamp('2011-01-02'):pd.Timestamp('2011-01-10')],
              ddf.loc[pd.Timestamp('2011-01-02'):pd.Timestamp('2011-01-10')])
    assert eq(df.loc[pd.Timestamp('2011-01-02 10:00')].to_frame().T,
              ddf.loc[pd.Timestamp('2011-01-02 10:00')])

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='M', periods=100))
    ddf = dd.from_pandas(df, 50)
    assert eq(df.loc['2011-01'], ddf.loc['2011-01'])
    assert eq(df.loc['2011'], ddf.loc['2011'])

    assert eq(df.loc['2011-01':'2012-05'], ddf.loc['2011-01':'2012-05'])
    assert eq(df.loc['2011':'2015'], ddf.loc['2011':'2015'])

    # series
    assert eq(df.B.loc['2011-01'], ddf.B.loc['2011-01'])
    assert eq(df.B.loc['2011'], ddf.B.loc['2011'])

    assert eq(df.B.loc['2011-01':'2012-05'], ddf.B.loc['2011-01':'2012-05'])
    assert eq(df.B.loc['2011':'2015'], ddf.B.loc['2011':'2015'])


def test_getitem_timestamp_str():

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='H', periods=100))
    ddf = dd.from_pandas(df, 10)

    # partial string slice
    assert eq(df['2011-01-02'],
              ddf['2011-01-02'])
    assert eq(df['2011-01-02':'2011-01-10'],
              df['2011-01-02':'2011-01-10'])

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='D', periods=100))
    ddf = dd.from_pandas(df, 50)
    assert eq(df['2011-01'], ddf['2011-01'])
    assert eq(df['2011'], ddf['2011'])

    assert eq(df['2011-01':'2012-05'], ddf['2011-01':'2012-05'])
    assert eq(df['2011':'2015'], ddf['2011':'2015'])


def test_loc_period_str():
    # .loc with PeriodIndex doesn't support partial string indexing
    # https://github.com/pydata/pandas/issues/13429
    pass


def test_getitem_period_str():

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.period_range('2011-01-01', freq='H', periods=100))
    ddf = dd.from_pandas(df, 10)

    # partial string slice
    assert eq(df['2011-01-02'],
              ddf['2011-01-02'])
    assert eq(df['2011-01-02':'2011-01-10'],
              df['2011-01-02':'2011-01-10'])
    # same reso, dask result is always DataFrame

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.period_range('2011-01-01', freq='D', periods=100))
    ddf = dd.from_pandas(df, 50)
    assert eq(df['2011-01'], ddf['2011-01'])
    assert eq(df['2011'], ddf['2011'])

    assert eq(df['2011-01':'2012-05'], ddf['2011-01':'2012-05'])
    assert eq(df['2011':'2015'], ddf['2011':'2015'])

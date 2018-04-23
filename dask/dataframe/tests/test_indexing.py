import pandas as pd
import pandas.util.testing as tm
import numpy as np

import pytest

import dask
import dask.dataframe as dd

from dask.dataframe.core import IndexBounds
from dask.dataframe.indexing import _coerce_loc_index
from dask.dataframe.utils import assert_eq, make_meta
from dask.utils import Interval


dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
meta = make_meta({'a': 'i8', 'b': 'i8'}, index=pd.Index([], 'i8'))
d = dd.DataFrame(dsk, 'x', meta, [0, 5, 9, 9])
full = d.compute()


def test_loc():
    assert d.loc[3:8].index_bounds[0].start == 3
    assert d.loc[3:8].index_bounds[-1].stop == 8

    assert d.loc[5].index_bounds == IndexBounds((Interval.inclusive(5, 5),))

    assert_eq(d.loc[5], full.loc[5:5])
    assert_eq(d.loc[3:8], full.loc[3:8])
    assert_eq(d.loc[:8], full.loc[:8])
    assert_eq(d.loc[3:], full.loc[3:])
    assert_eq(d.loc[[5]], full.loc[[5]])
    assert_eq(d.loc[[3, 4, 1, 8]], full.loc[[3, 4, 1, 8]])
    assert_eq(d.loc[[3, 4, 1, 9]], full.loc[[3, 4, 1, 9]])
    assert_eq(d.loc[np.array([3, 4, 1, 9])], full.loc[np.array([3, 4, 1, 9])])

    assert_eq(d.a.loc[5], full.a.loc[5:5])
    assert_eq(d.a.loc[3:8], full.a.loc[3:8])
    assert_eq(d.a.loc[:8], full.a.loc[:8])
    assert_eq(d.a.loc[3:], full.a.loc[3:])
    assert_eq(d.a.loc[[5]], full.a.loc[[5]])
    assert_eq(d.a.loc[[3, 4, 1, 8]], full.a.loc[[3, 4, 1, 8]])
    assert_eq(d.a.loc[[3, 4, 1, 9]], full.a.loc[[3, 4, 1, 9]])
    assert_eq(d.a.loc[np.array([3, 4, 1, 9])], full.a.loc[np.array([3, 4, 1, 9])])
    assert_eq(d.a.loc[[]], full.a.loc[[]])
    assert_eq(d.a.loc[np.array([])], full.a.loc[np.array([])])

    pytest.raises(KeyError, lambda: d.loc[1000])
    assert_eq(d.loc[1000:], full.loc[1000:])
    assert_eq(d.loc[-2000:-1000], full.loc[-2000:-1000])

    assert sorted(d.loc[5].dask) == sorted(d.loc[5].dask)
    assert sorted(d.loc[5].dask) != sorted(d.loc[6].dask)


def test_loc_divisions():
    # https://github.com/dask/dask/issues/3411

    inclusive = Interval.inclusive
    df = pd.DataFrame({'a': range(10, 15)}).set_index('a')
    two_parts = dd.from_pandas(df, npartitions=2)
    assert two_parts.divisions == (10, 13, 14)

    # Slicing seems to treat divisions accurately
    assert two_parts[11:13].divisions == (11, 13, 13)
    assert two_parts[0:20].divisions == (10, 13, 14)

    one_part = dd.from_pandas(df, npartitions=1)
    assert one_part.index_bounds == IndexBounds((inclusive(10, 14),))
    assert one_part[11:12].index_bounds == IndexBounds((inclusive(11, 12),))
    assert one_part[:12].index_bounds == IndexBounds((inclusive(10, 12),))

    # Except when slicing 1 partition, beyond range of index on that side
    assert one_part[0:20].index_bounds == IndexBounds((inclusive(10, 14),))  # (0, 20) Should be (10, 14)
    assert two_parts[0:12].index_bounds == IndexBounds((inclusive(10, 12),))  # (0, 12) Should be (10, 12)
    assert two_parts[12:20].index_bounds == IndexBounds((
        inclusive(12, 12), inclusive(13, 14)))
    assert two_parts[13:20].index_bounds == IndexBounds((inclusive(13, 14),))  # (13, 20) whoops!

    # Out of index range stuff gives weird divisions
    # It's not clear whether bounds should match the slice or a given partition
    # so best to just test that it's empty here
    assert two_parts[20:].index_bounds[0].empty()  # (20, 14)


def test_loc_non_informative_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4]}, index=[10, 20, 30, 40])
    ddf = dd.from_pandas(df, npartitions=2, sort=True)
    ddf.index_bounds = IndexBounds((None,) * 2)
    assert not ddf.known_bounds

    ddf.loc[20:30].compute(get=dask.get)

    assert_eq(ddf.loc[20:30], df.loc[20:30])

    df = pd.DataFrame({'x': [1, 2, 3, 4]}, index=[10, 20, 20, 40])
    ddf = dd.from_pandas(df, npartitions=2, sort=True)
    assert_eq(ddf.loc[20], df.loc[20:20])


def test_loc_with_text_dates():
    A = tm.makeTimeSeries(10).iloc[:5]
    B = tm.makeTimeSeries(10).iloc[5:]
    s = dd.Series({('df', 0): A, ('df', 1): B}, 'df', A,
                  [A.index.min(), B.index.min(), B.index.max()])

    assert s.loc['2000': '2010'].index_bounds == s.index_bounds
    assert_eq(s.loc['2000': '2010'], s)
    assert len(s.loc['2000-01-03': '2000-01-05'].compute()) == 3


def test_loc_with_series():
    assert_eq(d.loc[d.a % 2 == 0], full.loc[full.a % 2 == 0])

    assert sorted(d.loc[d.a % 2].dask) == sorted(d.loc[d.a % 2].dask)
    assert sorted(d.loc[d.a % 2].dask) != sorted(d.loc[d.a % 3].dask)


def test_loc_with_series_different_partition():
    df = pd.DataFrame(np.random.randn(20, 5),
                      index=list('abcdefghijklmnopqrst'),
                      columns=list('ABCDE'))
    ddf = dd.from_pandas(df, 3)

    assert_eq(ddf.loc[ddf.A > 0], df.loc[df.A > 0])
    assert_eq(ddf.loc[(ddf.A > 0).repartition(['a', 'g', 'k', 'o', 't'])],
              df.loc[df.A > 0])


def test_loc2d():
    # index indexer is always regarded as slice for duplicated values
    assert_eq(d.loc[5, 'a'], full.loc[5:5, 'a'])
    # assert_eq(d.loc[[5], 'a'], full.loc[[5], 'a'])
    assert_eq(d.loc[5, ['a']], full.loc[5:5, ['a']])
    # assert_eq(d.loc[[5], ['a']], full.loc[[5], ['a']])

    assert_eq(d.loc[3:8, 'a'], full.loc[3:8, 'a'])
    assert_eq(d.loc[:8, 'a'], full.loc[:8, 'a'])
    assert_eq(d.loc[3:, 'a'], full.loc[3:, 'a'])
    assert_eq(d.loc[[8], 'a'], full.loc[[8], 'a'])

    assert_eq(d.loc[3:8, ['a']], full.loc[3:8, ['a']])
    assert_eq(d.loc[:8, ['a']], full.loc[:8, ['a']])
    assert_eq(d.loc[3:, ['a']], full.loc[3:, ['a']])
    assert_eq(d.loc[[3, 4, 3], ['a']], full.loc[[3, 4, 3], ['a']])

    # 3d
    with pytest.raises(pd.core.indexing.IndexingError):
        d.loc[3, 3, 3]

    # Series should raise
    with pytest.raises(pd.core.indexing.IndexingError):
        d.a.loc[3, 3]

    with pytest.raises(pd.core.indexing.IndexingError):
        d.a.loc[3:, 3]

    with pytest.raises(pd.core.indexing.IndexingError):
        d.a.loc[d.a % 2 == 0, 3]


def test_loc2d_with_known_divisions():
    df = pd.DataFrame(np.random.randn(20, 5),
                      index=list('abcdefghijklmnopqrst'),
                      columns=list('ABCDE'))
    ddf = dd.from_pandas(df, 3)

    assert_eq(ddf.loc['a', 'A'], df.loc[['a'], 'A'])
    assert_eq(ddf.loc['a', ['A']], df.loc[['a'], ['A']])
    assert_eq(ddf.loc['a':'o', 'A'], df.loc['a':'o', 'A'])
    assert_eq(ddf.loc['a':'o', ['A']], df.loc['a':'o', ['A']])
    assert_eq(ddf.loc[['n'], ['A']], df.loc[['n'], ['A']])
    assert_eq(ddf.loc[['a', 'c', 'n'], ['A']], df.loc[['a', 'c', 'n'], ['A']])
    assert_eq(ddf.loc[['t', 'b'], ['A']], df.loc[['t', 'b'], ['A']])
    assert_eq(ddf.loc[['r', 'r', 'c', 'g', 'h'], ['A']],
              df.loc[['r', 'r', 'c', 'g', 'h'], ['A']])


def test_loc2d_with_unknown_divisions():
    df = pd.DataFrame(np.random.randn(20, 5),
                      index=list('abcdefghijklmnopqrst'),
                      columns=list('ABCDE'))
    ddf = dd.from_pandas(df, 3)

    ddf.index_bounds = IndexBounds((None, ) * ddf.npartitions)
    assert ddf.known_bounds is False

    assert_eq(ddf.loc['a', 'A'], df.loc[['a'], 'A'])
    assert_eq(ddf.loc['a', ['A']], df.loc[['a'], ['A']])
    assert_eq(ddf.loc['a':'o', 'A'], df.loc['a':'o', 'A'])
    assert_eq(ddf.loc['a':'o', ['A']], df.loc['a':'o', ['A']])


def test_loc2d_duplicated_columns():
    df = pd.DataFrame(np.random.randn(20, 5),
                      index=list('abcdefghijklmnopqrst'),
                      columns=list('AABCD'))
    ddf = dd.from_pandas(df, 3)

    assert_eq(ddf.loc['a', 'A'], df.loc[['a'], 'A'])
    assert_eq(ddf.loc['a', ['A']], df.loc[['a'], ['A']])
    assert_eq(ddf.loc['j', 'B'], df.loc[['j'], 'B'])
    assert_eq(ddf.loc['j', ['B']], df.loc[['j'], ['B']])

    assert_eq(ddf.loc['a':'o', 'A'], df.loc['a':'o', 'A'])
    assert_eq(ddf.loc['a':'o', ['A']], df.loc['a':'o', ['A']])
    assert_eq(ddf.loc['j':'q', 'B'], df.loc['j':'q', 'B'])
    assert_eq(ddf.loc['j':'q', ['B']], df.loc['j':'q', ['B']])

    assert_eq(ddf.loc['a':'o', 'B':'D'], df.loc['a':'o', 'B':'D'])
    assert_eq(ddf.loc['a':'o', 'B':'D'], df.loc['a':'o', 'B':'D'])
    assert_eq(ddf.loc['j':'q', 'B':'A'], df.loc['j':'q', 'B':'A'])
    assert_eq(ddf.loc['j':'q', 'B':'A'], df.loc['j':'q', 'B':'A'])

    assert_eq(ddf.loc[ddf.B > 0, 'B'], df.loc[df.B > 0, 'B'])
    assert_eq(ddf.loc[ddf.B > 0, ['A', 'C']], df.loc[df.B > 0, ['A', 'C']])


def test_getitem():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                       'B': [9, 8, 7, 6, 5, 4, 3, 2, 1],
                       'C': [True, False, True] * 3},
                      columns=list('ABC'))
    ddf = dd.from_pandas(df, 2)
    assert_eq(ddf['A'], df['A'])
    # check cache consistency
    tm.assert_series_equal(ddf['A']._meta, ddf._meta['A'])

    assert_eq(ddf[['A', 'B']], df[['A', 'B']])
    tm.assert_frame_equal(ddf[['A', 'B']]._meta, ddf._meta[['A', 'B']])

    assert_eq(ddf[ddf.C], df[df.C])
    tm.assert_series_equal(ddf.C._meta, ddf._meta.C)

    assert_eq(ddf[ddf.C.repartition([0, 2, 5, 8])], df[df.C])

    pytest.raises(KeyError, lambda: df['X'])
    pytest.raises(KeyError, lambda: df[['A', 'X']])
    pytest.raises(AttributeError, lambda: df.X)

    # not str/unicode
    df = pd.DataFrame(np.random.randn(10, 5))
    ddf = dd.from_pandas(df, 2)
    assert_eq(ddf[0], df[0])
    assert_eq(ddf[[1, 2]], df[[1, 2]])

    pytest.raises(KeyError, lambda: df[8])
    pytest.raises(KeyError, lambda: df[[1, 8]])


def test_getitem_slice():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                       'B': [9, 8, 7, 6, 5, 4, 3, 2, 1],
                       'C': [True, False, True] * 3},
                      index=list('abcdefghi'))
    ddf = dd.from_pandas(df, 3)
    assert_eq(ddf['a':'e'], df['a':'e'])
    assert_eq(ddf['a':'b'], df['a':'b'])
    assert_eq(ddf['f':], df['f':])


def test_loc_on_numpy_datetimes():
    df = pd.DataFrame({'x': [1, 2, 3]},
                      index=list(map(np.datetime64, ['2014', '2015', '2016'])))
    a = dd.from_pandas(df, 2)
    a.index_bounds = a.index_bounds.map(np.datetime64)

    assert_eq(a.loc['2014': '2015'], a.loc['2014': '2015'])


def test_loc_on_pandas_datetimes():
    df = pd.DataFrame({'x': [1, 2, 3]},
                      index=list(map(pd.Timestamp, ['2014', '2015', '2016'])))
    a = dd.from_pandas(df, 2)
    a.index_bounds = a.index_bounds.map(pd.Timestamp)

    assert_eq(a.loc['2014': '2015'], a.loc['2014': '2015'])


def test_loc_datetime_no_freq():
    # https://github.com/dask/dask/issues/2389

    datetime_index = pd.date_range('2016-01-01', '2016-01-31', freq='12h')
    datetime_index.freq = None  # FORGET FREQUENCY
    df = pd.DataFrame({'num': range(len(datetime_index))}, index=datetime_index)

    ddf = dd.from_pandas(df, npartitions=1)
    slice_ = slice('2016-01-03', '2016-01-05')
    result = ddf.loc[slice_, :]
    expected = df.loc[slice_, :]
    assert_eq(result, expected)


def test_coerce_loc_index():
    for t in [pd.Timestamp, np.datetime64]:
        assert isinstance(_coerce_loc_index([t('2014')], '2014'), t)


def test_loc_timestamp_str():

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='H', periods=100))
    ddf = dd.from_pandas(df, 10)

    # partial string slice
    assert_eq(df.loc['2011-01-02'],
              ddf.loc['2011-01-02'])
    assert_eq(df.loc['2011-01-02':'2011-01-10'],
              ddf.loc['2011-01-02':'2011-01-10'])
    # same reso, dask result is always DataFrame
    assert_eq(df.loc['2011-01-02 10:00'].to_frame().T,
              ddf.loc['2011-01-02 10:00'])

    # series
    assert_eq(df.A.loc['2011-01-02'],
              ddf.A.loc['2011-01-02'])
    assert_eq(df.A.loc['2011-01-02':'2011-01-10'],
              ddf.A.loc['2011-01-02':'2011-01-10'])

    # slice with timestamp (dask result must be DataFrame)
    assert_eq(df.loc[pd.Timestamp('2011-01-02')].to_frame().T,
              ddf.loc[pd.Timestamp('2011-01-02')])
    assert_eq(df.loc[pd.Timestamp('2011-01-02'):pd.Timestamp('2011-01-10')],
              ddf.loc[pd.Timestamp('2011-01-02'):pd.Timestamp('2011-01-10')])
    assert_eq(df.loc[pd.Timestamp('2011-01-02 10:00')].to_frame().T,
              ddf.loc[pd.Timestamp('2011-01-02 10:00')])

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='M', periods=100))
    ddf = dd.from_pandas(df, 50)
    assert_eq(df.loc['2011-01'], ddf.loc['2011-01'])
    assert_eq(df.loc['2011'], ddf.loc['2011'])

    assert_eq(df.loc['2011-01':'2012-05'], ddf.loc['2011-01':'2012-05'])
    assert_eq(df.loc['2011':'2015'], ddf.loc['2011':'2015'])

    # series
    assert_eq(df.B.loc['2011-01'], ddf.B.loc['2011-01'])
    assert_eq(df.B.loc['2011'], ddf.B.loc['2011'])

    assert_eq(df.B.loc['2011-01':'2012-05'], ddf.B.loc['2011-01':'2012-05'])
    assert_eq(df.B.loc['2011':'2015'], ddf.B.loc['2011':'2015'])


def test_getitem_timestamp_str():

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='H', periods=100))
    ddf = dd.from_pandas(df, 10)

    # partial string slice
    assert_eq(df['2011-01-02'],
              ddf['2011-01-02'])
    assert_eq(df['2011-01-02':'2011-01-10'],
              df['2011-01-02':'2011-01-10'])

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.date_range('2011-01-01', freq='D', periods=100))
    ddf = dd.from_pandas(df, 50)
    assert_eq(df['2011-01'], ddf['2011-01'])
    assert_eq(df['2011'], ddf['2011'])

    assert_eq(df['2011-01':'2012-05'], ddf['2011-01':'2012-05'])
    assert_eq(df['2011':'2015'], ddf['2011':'2015'])


def test_loc_period_str():
    # .loc with PeriodIndex doesn't support partial string indexing
    # https://github.com/pydata/pandas/issues/13429
    pass


def test_getitem_period_str():

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.period_range('2011-01-01', freq='H', periods=100))
    ddf = dd.from_pandas(df, 10)

    # partial string slice
    assert_eq(df['2011-01-02'],
              ddf['2011-01-02'])
    assert_eq(df['2011-01-02':'2011-01-10'],
              df['2011-01-02':'2011-01-10'])
    # same reso, dask result is always DataFrame

    df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)},
                      index=pd.period_range('2011-01-01', freq='D', periods=100))
    ddf = dd.from_pandas(df, 50)
    assert_eq(df['2011-01'], ddf['2011-01'])
    assert_eq(df['2011'], ddf['2011'])

    assert_eq(df['2011-01':'2012-05'], ddf['2011-01':'2012-05'])
    assert_eq(df['2011':'2015'], ddf['2011':'2015'])


def test_index_bounds():
    inclusive = Interval.inclusive
    exclusive = Interval.exclusive

    for test, expected in [
        ((inclusive(5, 6), inclusive(7, 8)), True),
        ((inclusive(7, 8), inclusive(5, 6)), False),
        ((inclusive(5, 6), inclusive(6, 8)), False),
        ((exclusive(5, 6), inclusive(6, 8)), True),
        ((inclusive(5, 6), Interval(6, True, 8, True)), True),
    ]:
        assert IndexBounds(test).is_strictly_sorted() is expected

    assert (
            IndexBounds(
                (exclusive(0, 5), inclusive(5, 9))).map(lambda a: a + 1) ==
            IndexBounds(
                (exclusive(1, 6), inclusive(6, 10)))
            )
    assert (
            IndexBounds(
                (exclusive(0, 5), inclusive(5, 9))
            ).map_all(lambda a: np.asarray(a).cumsum()) ==
            IndexBounds(
                (exclusive(0, 5), inclusive(10, 19)))
            )

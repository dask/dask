import dask.dataframe as dd
import pandas.util.testing as tm
import pandas as pd


def test_make_timeseries():
    df = dd.demo.make_timeseries('2000', '2015', {'A': float, 'B': int, 'C': str},
                                 freq='2D', partition_freq='6M')

    assert df.divisions[0] == pd.Timestamp('2000-01-31', offset='6M')
    assert df.divisions[-1] == pd.Timestamp('2014-07-31', offset='6M')
    assert df.columns == ('A', 'B', 'C')
    assert df['A'].head().dtype == float
    assert df['B'].head().dtype == int
    assert df['C'].head().dtype == object
    assert df.divisions == tuple(pd.DatetimeIndex(start='2000', end='2015',
                                                  freq='6M'))

    tm.assert_frame_equal(df.head(), df.head())

    a = dd.demo.make_timeseries('2000', '2015', {'A': float, 'B': int, 'C': str},
                                 freq='2D', partition_freq='6M', seed=123)
    b = dd.demo.make_timeseries('2000', '2015', {'A': float, 'B': int, 'C': str},
                                 freq='2D', partition_freq='6M', seed=123)
    tm.assert_frame_equal(a.head(), b.head())

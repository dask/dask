from pframe.core import pframe, shard_df_on_index

import pandas as pd
import shutil
from pandas.util import testing as tm
from pframe.utils import raises

df1 = pd.DataFrame({'a': [1, 2, 3],
                    'b': [4, 5, 6],
                    'c': [1., 2., 3.]}, index=[1, 3, 5])
df2 = pd.DataFrame({'a': [10, 20, 30],
                    'b': [40, 50, 60],
                    'c': [10., 20., 30.]}, index=[2, 4, 6])

df1.index.name = 'i'
df2.index.name = 'i'

pf = pframe(like=df1, blockdivs=[4])
pf.append(df1)
pf.append(df2)


def test_append():
    a = pd.DataFrame({'a': [1, 2, 10],
                      'b': [4, 5, 40],
                      'c': [1., 2., 10.]}, index=[1, 3, 2])
    a.index.name = 'i'
    b = pd.DataFrame({'a': [3, 20, 30],
                      'b': [6, 50, 60],
                      'c': [3., 20., 30.]}, index=[5, 4, 6])
    b.index.name = 'i'

    tm.assert_frame_equal(pf.get_partition(0), a)
    tm.assert_frame_equal(pf.get_partition(1), b)

    tm.assert_frame_equal(pf.get_partition(1, ['c', 'b']), b[['c', 'b']])


def test_head():
    assert isinstance(pf.head(), pd.DataFrame)


def test_metadata():
    assert list(pf.columns) == list(df1.columns)
    assert list(pf.dtypes) == list(df1.dtypes)


def test_n_c_bytes():
    assert 0 < pf.nbytes < 10000000
    assert 0 < pf.cbytes < 10000000


def test_npartitions():
    assert pf.npartitions == 2


def test_categoricals():
    df = pd.DataFrame({'a': pd.Categorical(['Alice', 'Bob', 'Alice']),
                       'b': pd.Categorical([1, 2, 3])})
    pf = pframe(like=df, blockdivs=[2])
    pf.append(df)
    assert pf.partitions[0].blocks['a'].dtype == 'i1'

    tm.assert_frame_equal(pf.to_dataframe(), df)


def test_raise_on_object_dtype():
    df = pd.DataFrame({'a': ['Alice', 'Bob', 'Alice']})
    assert raises(Exception, lambda: pframe(like=df, blockdivs=['Bob']))


def test_shard_df_on_index():
    f = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]},
                      index=[1, 2, 3, 4, 4])

    result = list(shard_df_on_index(f, [2, 7]))
    tm.assert_frame_equal(result[0], f.loc[[1]])
    tm.assert_frame_equal(result[1], f.loc[[2, 3, 4]])
    tm.assert_frame_equal(result[2], pd.DataFrame(columns=['a', 'b'],
                                                  dtype=f.dtypes))

    f = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]},
                      index=['a', 'b', 'c', 'd', 'e'])
    result = list(shard_df_on_index(f, ['b', 'd']))
    tm.assert_frame_equal(result[0], f.iloc[:1])
    tm.assert_frame_equal(result[1], f.iloc[1:3])
    tm.assert_frame_equal(result[2], f.iloc[3:])


    f = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 2, 6]},
                     index=[0, 1, 3]).set_index('b').sort()

    result = list(shard_df_on_index(f, [4, 9]))
    tm.assert_frame_equal(result[0], f.iloc[0:1])
    tm.assert_frame_equal(result[1], f.iloc[1:3])
    tm.assert_frame_equal(result[2], f.iloc[3:])

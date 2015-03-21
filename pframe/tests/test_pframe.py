from pframe.core import pframe

import pandas as pd
import shutil
from pandas.util import testing as tm

df1 = pd.DataFrame({'a': [1, 2, 3],
                    'b': [4, 5, 6],
                    'c': [1., 2., 3.]}, index=[1, 3, 5])
df2 = pd.DataFrame({'a': [10, 20, 30],
                    'b': [40, 50, 60],
                    'c': [10., 20., 30.]}, index=[2, 4, 6])

pf = pframe(like=df1, blockdivs=[4])
pf.append(df1)
pf.append(df2)


def test_append():
    a = pd.DataFrame({'a': [1, 2, 10],
                      'b': [4, 5, 40],
                      'c': [1., 2., 10.]}, index=[1, 3, 2])
    b = pd.DataFrame({'a': [3, 20, 30],
                      'b': [6, 50, 60],
                      'c': [3., 20., 30.]}, index=[5, 4, 6])

    tm.assert_frame_equal(pf.partitions[0].to_dataframe(), a)
    tm.assert_frame_equal(pf.partitions[1].to_dataframe(), b)

def test_head():
    assert isinstance(pf.head(), pd.DataFrame)


def test_metadata():
    assert list(pf.columns) == list(df1.columns)
    assert list(pf.dtypes) == list(df1.dtypes)


def test_n_c_bytes():
    assert 0 < pf.nbytes < 1000000
    assert 0 < pf.cbytes < 1000000


def test_categoricals():
    df = pd.DataFrame({'a': pd.Categorical(['Alice', 'Bob', 'Alice'])})
    pf = pframe(like=df, blockdivs=[2])
    pf.append(df)
    assert pf.partitions[0].blocks[0].dtype == 'i1'

    tm.assert_frame_equal(pf.to_dataframe(), df)

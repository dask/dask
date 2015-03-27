from pframe.categories import reapply_categories, strip_categories, categorical_metadata
from pandas.util import testing as tm
import pandas as pd


def test_reapply_categories():
    df = pd.DataFrame({'a': [0, 1, 0], 'b': [1, 0, 0]})
    metadata = {'a': {'ordered': True,
                      'categories': pd.Index(['Alice', 'Bob'], dtype='object')}}


    assert list(reapply_categories(df[['a']], metadata).columns) == ['a']
    assert list(reapply_categories(df.a, metadata)) == ['Alice', 'Bob', 'Alice']
    assert list(reapply_categories(df.b, metadata)) == [1, 0, 00]


def test_index_categorical():
    df = pd.DataFrame({'a': [1, 2, 3]},
                      index=pd.Categorical(['Alice', 'Bob', 'Alice']))

    df2 = strip_categories(df.copy())
    assert df2.index.equals(pd.Index([0, 1, 0], dtype='i2'))

    metadata = categorical_metadata(df)

    assert metadata['_index']['categories'].equals(df.index.categories)

    df3 = reapply_categories(df2.copy(), metadata)

    assert df.equals(df3)


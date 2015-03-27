from pframe.categories import reapply_categories
from pandas.util import testing as tm
import pandas as pd


def test_reapply_categories():
    df = pd.DataFrame({'a': [0, 1, 0], 'b': [1, 0, 0]})
    metadata = {'a': {'ordered': True,
                      'categories': pd.Index(['Alice', 'Bob'], dtype='object')}}


    assert list(reapply_categories(df[['a']], metadata).columns) == ['a']
    assert list(reapply_categories(df.a, metadata)) == ['Alice', 'Bob', 'Alice']
    assert list(reapply_categories(df.b, metadata)) == [1, 0, 00]

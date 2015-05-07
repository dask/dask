from pframe.cframe import cframe

import pandas as pd
import shutil
import pickle
from pandas.util import testing as tm


def test_convert():
    df = pd.DataFrame({'a': [1, 2, 3],
                       'b': [4, 5, 6],
                       'c': [1., 2., 3.]}, index=[5, 3, 3])
    cf = cframe(df)
    assert all(cf.blocks[col].dtype == df.dtypes[col] for col in df.columns)

    cf.append(df)
    assert all(len(cf.blocks[col]) == len(df) for col in cf.columns)

    df2 = cf.to_dataframe()
    tm.assert_frame_equal(df, df2)

    # Test column access
    assert list(cf.to_dataframe(columns=['c', 'a']).columns) == ['c', 'a']
    tm.assert_frame_equal(cf.to_dataframe(columns=['c', 'a']), df[['c', 'a']])
    assert isinstance(cf.to_dataframe(columns='a'), pd.Series)

    cf.append(df)
    assert len(cf) == 2*len(df)

    assert list(cf.to_dataframe()['a']) == [1, 2, 3, 1, 2, 3]

    cf2 = pickle.loads(pickle.dumps(cf))

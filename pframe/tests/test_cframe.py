from pframe.cframe import cframe

import pandas as pd
import shutil
from pandas.util import testing as tm


def test_convert():
    df = pd.DataFrame({'a': [1, 2, 3],
                       'b': [4, 5, 6],
                       'c': [1., 2., 3.]}, index=[5, 3, 3])
    cf = cframe(df)
    assert cf.blocks[0].dtype == df._data.blocks[0].dtype
    assert set([cf.blocks[0].shape[1], cf.blocks[1].shape[1]]) == set([1, 2])

    cf.append(df)
    assert len(cf.blocks[0]) == len(df)

    df2 = cf.to_dataframe()
    tm.assert_frame_equal(df, df2)

    cf.append(df)
    assert len(cf.blocks[0]) == 2*len(df)

    assert list(cf.to_dataframe()['a']) == [1, 2, 3, 1, 2, 3]

import pandas as pd
from dask.dataframe.utils import (shard_df_on_index, get_categories,
        _categorize, strip_categories)
import pandas.util.testing as tm


def test_shard_df_on_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    s = df.y

    result = list(shard_df_on_index(df, [20, 50]))
    assert list(result[0].index) == [10]
    assert list(result[1].index) == [20, 30, 40]
    assert list(result[2].index) == [50, 60]


def test_categories():
    df = pd.DataFrame({'x': [1, 2, 3, 4],
                       'y': pd.Categorical(['a', 'b', 'a', 'c'])},
                      index=pd.CategoricalIndex(['x', 'x', 'y', 'y']))

    categories = get_categories(df)
    assert set(categories.keys()) == set(['y', '.index'])
    assert list(categories['y']) == ['a', 'b', 'c']
    assert list(categories['.index']) == ['x', 'y']

    df2 = strip_categories(df)
    assert not get_categories(df2)

    df3 = _categorize(categories, df2)

    tm.assert_frame_equal(df, df3)

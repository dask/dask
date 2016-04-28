import pandas as pd
from dask.dataframe.utils import shard_df_on_index


def test_shard_df_on_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])

    result = list(shard_df_on_index(df, [20, 50]))
    assert list(result[0].index) == [10]
    assert list(result[1].index) == [20, 30, 40]
    assert list(result[2].index) == [50, 60]


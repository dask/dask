import pandas as pd
import numpy as np
'''
df = pd.DataFrame({'animal':['snake', 'bat', 'tiger', 'lion',
                   'fox', 'eagle', 'shark', 'dog', 'deer'], 'animal_copy':['snake', 'bat', 'tiger', 'lion',
                   'fox', 'eagle', 'shark', 'dog', 'deer']})
for _, series in df.items():
    print(_, type(_))
    print(series, type(series))

#############
import pandas as pd
df = pd.DataFrame({'animal':['snake', 'bat', 'tiger', 'lion',
                   'fox', 'eagle', 'shark', 'dog', 'deer'], 'animal_copy':['snake', 'bat', 'tiger', 'lion',
                   'fox', 'eagle', 'shark', 'dog', 'deer']})
cols=['animal']
df3=df[cols]
df3
df2 = pd.util.hash_pandas_object(df3, index=True, encoding="utf8", hash_key=None, categorize=True)
print(type(df2))
'''
df = pd.DataFrame({'a': [1, 2] * 3, 'b': [True, False] * 3, 'c': [1.0, 2.0] * 3})
from dask import dataframe as dd
import pandas as pd
from dask.distributed import Client

if __name__ == '__main__':
    client = Client()
    client

    #df = {'col-0': [1, 2, 3], 'col-1': [4, 5, 6]}
    #df = {'col-0': list(range(1000)), 'col-1': list(range(1000))}
    df = {'col-0': list(range(1000)), 'col-1':['cat' for i in range(1000)]}#'col-1': list(np.random.randint(low=3, high=899, size=1000))}
    df = pd.DataFrame(df)

    #df1 = {'col-0': [1, 2, 3], 'col-3': [7, 8, 9]}
    #df1 = {'col-0': list(range(1000)), 'col-3': list(range(1000))}
    df1 = {'col-0': list(range(1000)), 'col-3': list(np.random.randint(low=3, high=1000, size=1000))}
    df1 = pd.DataFrame(df1)

    ddf = dd.from_pandas(df, 2)
    ddf2 = dd.from_pandas(df1, 2)
    print('----------------------------------------------------------')
    merge_df = ddf.merge(ddf2, shuffle='tasks')
    print(merge_df.compute())


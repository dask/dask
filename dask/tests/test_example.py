import dask
from dask import dataframe as dd

import pandas as pd

#df = pd.DataFrame({'animal':['snake', 'bat', 'tiger', 'lion','fox', 'eagle', 'shark', 'dog', 'deer'], 'animal_copy':['snake', 'bat', 'tiger', 'lion','fox', 'eagle', 'shark', 'dog', 'deer']})
#df = {'col-0': [1, 2, 3], 'col-1': [4, 5, 6]}
df = {'col-0': list(range(1000)), 'col-3':list(range(1000))}
df = pd.DataFrame(df)

print(df)
print(df.iloc[0:2])#return rows 0 and 1
ddf = dd.from_pandas(df, 2)

#df1 = pd.DataFrame({'animal':['snake', 'bat', 'tiger', 'lion','fox', 'eagle', 'shark', 'dog', 'deer'], 'animal_copy_2':['snake', 'bat', 'tiger', 'lion','fox', 'eagle', 'shark', 'dog', 'deer']})
#df1 = {'col-0': [1, 2, 3], 'col-3': [7, 8, 9]}
df1 = {'col-0': list(range(1000)), 'col-1':list(range(1000))}
df1 = pd.DataFrame(df1)

ddf2 = dd.from_pandas(df1, 2)

df.merge(df1)

print('-----------------------------')
merge_df = ddf.merge(ddf2, shuffle='tasks')#put tasks to make consistent with dask_cylon.merge(shuffle=tasks), otherwise default none
#print(merge_df)
#merge_df.compute()

print(merge_df.compute())
a=0
if a is not None and 1 is not None:
    print('####')


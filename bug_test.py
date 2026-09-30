import pandas as pd
import dask.dataframe as dd

# صاوب MultiIndex مسمى
mi = pd.MultiIndex.from_tuples([(1, 2)], names=['a', 'b'])
df = pd.DataFrame({'x': [100]}, index=mi)

# صاوب نسخة خاوية
empty_df = df.iloc[:0]
ddf = dd.from_pandas(empty_df, npartitions=1)

# جرب دير concat وشوف واش السميات 'a' و 'b' غيبقاو
result = dd.concat([ddf])
print(result.index.names)

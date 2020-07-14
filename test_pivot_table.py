import pandas as pd
import dask.dataframe as ds
df = pd.DataFrame({'A':[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],'B':['1th','2th','3th','4th','5th','1th','1th','3th','3th','3th','1th','2th','3th','4th','5th','1th','1th','3th','3th','3th'],
'C':['aa','bb','cc','aa','cc','aa','bb','cc','aa','cc','aa','bb','cc','aa','cc','aa','bb','cc','aa','cc'],
'D':[25,50,45,14,12,89,7,11,32,10,25,50,45,14,12,89,7,11,32,10]})
pt = df.pivot_table(index='B', columns='C', values='D')
pt

dsdf = ds.from_pandas(df,npartitions=2)
dsdf['C'] = dsdf['C'].astype('category').cat.as_known()
print(dsdf.pivot_table(index='B',columns='C',values='D', aggfunc='sum').compute(scheduler='sync'))
from dask.sizeof import sizeof
import dask.bag as db

b = db.from_sequence([1, 2, 3, 4, 5, 6])
size = sizeof(b)
# 48

new = b.repartition(partition_size=size)
new.npartitions
# 18
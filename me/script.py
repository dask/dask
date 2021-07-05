import dask.array as da

a = da.ones((20), chunks=(2))

b = da.dot(a, a) + da.dot(a, a) + da.dot(a, a)
c = da.dot(a, a) + da.dot(a, a) + da.dot(a, a)

c = c + 10

c = c + da.dot(a, a)

c = c - 5

c.dask.visualize(filename="hlg")
c.visualize(filename="llg")

# from dask.datasets import timeseries

# ddf = timeseries().shuffle("id", shuffle="tasks").head(compute=False)
# ddf.dask.visualize(filename="hlg")
# # ddf.visualize(filename="llg")


# import dask.array as da
# x = da.arange(1000).reshape(1, 1000)
# y = x.T
# z = x * y
# result = da.sum(z)

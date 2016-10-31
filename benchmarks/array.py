from dask import array as da


class ArraySuite(object):

    goal_time = 0.2

    def setup(self):
        self.z = da.ones((30, 30), chunks=(30, 1))

    def time_cull(self):
        z = self.z
        z = z.rechunk((1, 30))
        z = z.rechunk((30, 1))
        z = z.rechunk((1, 30))
        z = z.rechunk((30, 1))
        z = z.rechunk((1, 30))
        z.compute()

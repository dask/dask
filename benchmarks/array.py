from dask import array as da
import numpy as np


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

    def time_fancy(self):
        a = da.empty(shape=(2000000, 200, 2), dtype='i1',
                     chunks=(10000, 100, 2))
        c = np.random.randint(0, 2, size=a.shape[0], dtype=bool)
        s = sorted(np.random.choice(a.shape[1], size=100, replace=False))
        a[c][:, s]


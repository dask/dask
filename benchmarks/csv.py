import string

import numpy as np
import pandas as pd
import dask
import dask.multiprocessing
import dask.dataframe as dd


class CSVSuite(object):
    params = [dask.get, dask.multiprocessing.get, dask.threaded.get]

    def setup_cache(self):
        N = 1000
        n_files = 50
        df = pd.DataFrame(np.random.randn(N, 4), columns=list('abcd'))
        df['e'] = np.random.choice(list(string.ascii_letters), N)
        for i in range(n_files):
            df.to_csv('asv-csv-{}.csv'.format(i), index=False)

    def time_read_meta(self, get):
        return dd.read_csv('asv-csv-*.csv')

    def time_read_csv(self, get):
        return dd.read_csv('asv-csv-*.csv').compute(get=get)

import dask.dataframe as dd
from distributed import Executor
from distributed.utils_test import cluster, _test_cluster
from distributed.collections import (_futures_to_dask_dataframe,
        futures_to_dask_dataframe)
import pandas as pd

from tornado import gen
from tornado.ioloop import IOLoop


def test__futures_to_dask_dataframe():
    dfs = [pd.DataFrame({'x': [1, 2, 3]}, index=[0, 10, 20]),
           pd.DataFrame({'x': [4, 5, 6]}, index=[30, 40, 50]),
           pd.DataFrame({'x': [7, 8, 9]}, index=[60, 70, 80])]

    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False)
        IOLoop.current().spawn_callback(e._go)

        remote_dfs = e.map(lambda x: x, dfs)
        ddf = yield _futures_to_dask_dataframe(e, remote_dfs, divisions=True)

        assert isinstance(ddf, dd.DataFrame)
        assert ddf.divisions == (0, 30, 60, 80)
        expr = ddf.x.sum()
        result = yield e._get(expr.dask, expr._keys())
        assert result == [sum([df.x.sum() for df in dfs])]

        yield e._shutdown()

    _test_cluster(f)


def test_futures_to_dask_dataframe():
    dfs = [pd.DataFrame({'x': [1, 2, 3]}, index=[0, 10, 20]),
           pd.DataFrame({'x': [4, 5, 6]}, index=[30, 40, 50]),
           pd.DataFrame({'x': [7, 8, 9]}, index=[60, 70, 80])]

    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            remote_dfs = e.map(lambda x: x, dfs)
            ddf = futures_to_dask_dataframe(e, remote_dfs, divisions=True)

            assert isinstance(ddf, dd.DataFrame)
            assert ddf.x.sum().compute(get=e.get) == sum([df.x.sum() for df in dfs])



import dask
import dask.dataframe as dd
from distributed import Executor
from distributed.utils_test import cluster, _test_cluster, slow
from distributed.collections import (_futures_to_dask_dataframe,
        futures_to_dask_dataframe)
import numpy as np
import pandas as pd
import pandas.util.testing as tm

from tornado import gen
from tornado.ioloop import IOLoop


dfs = [pd.DataFrame({'x': [1, 2, 3]}, index=[0, 10, 20]),
       pd.DataFrame({'x': [4, 5, 6]}, index=[30, 40, 50]),
       pd.DataFrame({'x': [7, 8, 9]}, index=[60, 70, 80])]


def assert_equal(a, b):
    assert type(a) == type(b)
    if isinstance(a, pd.DataFrame):
        tm.assert_frame_equal(a, b)
    elif isinstance(a, pd.Series):
        tm.assert_series_equal(a, b)
    elif isinstance(a, pd.Index):
        tm.assert_index_equal(a, b)
    else:
        assert a == b


def test__futures_to_dask_dataframe():
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
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            remote_dfs = e.map(lambda x: x, dfs)
            ddf = futures_to_dask_dataframe(e, remote_dfs, divisions=True)

            assert isinstance(ddf, dd.DataFrame)
            assert ddf.x.sum().compute(get=e.get) == sum([df.x.sum() for df in dfs])


@slow
def test_dataframes():
    dfs = [pd.DataFrame({'x': np.random.random(100),
                         'y': np.random.random(100)},
                        index=list(range(i, i + 100)))
           for i in range(0, 100*10, 100)]
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port'])) as e:
            remote_dfs = e.map(lambda x: x, dfs)
            rdf = futures_to_dask_dataframe(e, remote_dfs, divisions=True)
            name = 'foo'
            ldf = dd.DataFrame({(name, i): df for i, df in enumerate(dfs)},
                               name, dfs[0].columns,
                               list(range(0, 1000, 100)) + [999])

            assert rdf.divisions == ldf.divisions
            tm.assert_frame_equal(rdf.compute(get=e.get),
                                  ldf.compute(get=dask.get))

            exprs = [lambda df: df.x.mean(),
                     lambda df: df.y.std(),
                     lambda df: df.assign(z=df.x + df.y).drop_duplicates(),
                     lambda df: df.index,
                     lambda df: df.x,
                     lambda df: df.x.cumsum(),
                     lambda df: df.loc[50:75]]
            for f in exprs:
                local = f(ldf).compute(get=dask.get)
                remote = f(rdf).compute(get=e.get)
                assert_equal(local, remote)

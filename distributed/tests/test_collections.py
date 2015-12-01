
import dask
import dask.dataframe as dd
from distributed import Executor
from distributed.utils_test import cluster, _test_cluster, slow, loop
from distributed.collections import (_futures_to_dask_dataframe,
        futures_to_dask_dataframe, _futures_to_dask_array,
        futures_to_dask_array, _stack, stack)
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


def test__futures_to_dask_dataframe(loop):
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        remote_dfs = e.map(lambda x: x, dfs)
        ddf = yield _futures_to_dask_dataframe(remote_dfs, divisions=True,
                executor=e)

        assert isinstance(ddf, dd.DataFrame)
        assert ddf.divisions == (0, 30, 60, 80)
        expr = ddf.x.sum()
        result = yield e._get(expr.dask, expr._keys())
        assert result == [sum([df.x.sum() for df in dfs])]

        yield e._shutdown()
    _test_cluster(f)


def test_futures_to_dask_dataframe(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            remote_dfs = e.map(lambda x: x, dfs)
            ddf = futures_to_dask_dataframe(remote_dfs, divisions=True)

            assert isinstance(ddf, dd.DataFrame)
            assert ddf.x.sum().compute(get=e.get) == sum([df.x.sum() for df in dfs])


@slow
def test_dataframes(loop):
    dfs = [pd.DataFrame({'x': np.random.random(100),
                         'y': np.random.random(100)},
                        index=list(range(i, i + 100)))
           for i in range(0, 100*10, 100)]
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            remote_dfs = e.map(lambda x: x, dfs)
            rdf = futures_to_dask_dataframe(remote_dfs, divisions=True)
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


def test__futures_to_dask_array(loop):
    import dask.array as da
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        remote_arrays = [[[e.submit(np.full, (2, 3, 4), i + j + k)
                            for i in range(2)]
                            for j in range(2)]
                            for k in range(4)]

        x = yield _futures_to_dask_array(remote_arrays, executor=e)
        assert x.chunks == ((2, 2, 2, 2), (3, 3), (4, 4))
        assert x.dtype == np.full((), 0).dtype

        assert isinstance(x, da.Array)
        expr = x.sum()
        result = yield e._get(expr.dask, expr._keys())
        assert isinstance(result[0], np.number)

        yield e._shutdown()
    _test_cluster(f)


def test__stack(loop):
    import dask.array as da
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        arrays = e.map(np.ones, [(5, 5)] * 6)
        y = yield _stack(arrays, axis=0)
        assert y.shape == (6, 5, 5)
        assert y.chunks == ((1, 1, 1, 1, 1, 1), (5,), (5,))

        y_results = yield e._get(y.dask, y._keys())
        yy = da.Array._finalize(y, y_results)

        assert isinstance(yy, np.ndarray)
        assert yy.shape == y.shape
        assert (yy == 1).all()

        yield e._shutdown()
    _test_cluster(f)


def test_stack(loop):
    import dask.array as da
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            arrays = [np.random.random((3, 3)) for i in range(4)]
            remotes = e.scatter(arrays)
            local = np.concatenate([a[None, ...] for a in arrays], axis=0) # np.stack(arrays, axis=0)
            remote = stack(remotes, axis=0)

            assert isinstance(remote, da.Array)
            assert (remote.compute(get=e.get) == local).all()

            assert isinstance(remote[2, :, 1].compute(get=e.get), np.ndarray)


def test__dask_array_collections(loop):
    import dask.array as da
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port), start=False, loop=loop)
        yield e._start()

        x_dsk = {('x', i, j): np.random.random((3, 3)) for i in range(3)
                                                       for j in range(2)}
        y_dsk = {('y', i, j): np.random.random((3, 3)) for i in range(2)
                                                       for j in range(3)}
        x_futures = yield e._scatter(x_dsk)
        y_futures = yield e._scatter(y_dsk)

        dt = np.random.random(0).dtype
        x_local = da.Array(x_dsk, 'x', ((3, 3, 3), (3, 3)), dt)
        y_local = da.Array(y_dsk, 'y', ((3, 3), (3, 3, 3)), dt)

        x_remote = da.Array(x_futures, 'x', ((3, 3, 3), (3, 3)), dt)
        y_remote = da.Array(y_futures, 'y', ((3, 3), (3, 3, 3)), dt)

        exprs = [lambda x, y: x.T + y,
                 lambda x, y: x.mean() + y.mean(),
                 lambda x, y: x.dot(y).std(axis=0),
                 lambda x, y: x - x.mean(axis=1)[:, None]]

        for expr in exprs:
            local = expr(x_local, y_local)
            local_results = dask.get(local.dask, local._keys())
            local_result = da.Array._finalize(local, local_results)

            remote = expr(x_remote, y_remote)
            remote_results = yield e._get(remote.dask, remote._keys())
            remote_result = da.Array._finalize(remote, remote_results)

            assert np.all(local_result == remote_result)

        yield e._shutdown()
    _test_cluster(f)


def test_futures_to_dask_array(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            remote_arrays = [[e.submit(np.full, (3, 3), i + j)
                                for i in range(3)]
                                for j in range(3)]

            x = futures_to_dask_array(remote_arrays, executor=e)
            assert x.chunks == ((3, 3, 3), (3, 3, 3))
            assert x.dtype == np.full((), 0).dtype

            assert x.sum().compute(get=e.get) == 162
            assert (x + x.T).sum().compute(get=e.get) == 162 * 2

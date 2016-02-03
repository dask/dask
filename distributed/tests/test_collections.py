from datetime import timedelta
import sys

import dask
import dask.dataframe as dd
from distributed import Executor
from distributed.utils_test import cluster, loop, gen_cluster
from distributed.collections import (_futures_to_dask_dataframe,
        futures_to_dask_dataframe, _futures_to_dask_array,
        futures_to_dask_array, _stack, stack, _futures_to_collection,
        _futures_to_dask_bag, futures_to_dask_bag,
        futures_to_collection)
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest
from toolz import identity

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


@gen_cluster()
def test__futures_to_dask_dataframe(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    remote_dfs = e.map(identity, dfs)
    ddf = yield _futures_to_dask_dataframe(remote_dfs, divisions=True,
            executor=e)

    assert isinstance(ddf, dd.DataFrame)
    assert ddf.divisions == (0, 30, 60, 80)
    expr = ddf.x.sum()
    result = yield e._get(expr.dask, expr._keys())
    assert result == [sum([df.x.sum() for df in dfs])]

    yield e._shutdown()


@gen_cluster()
def test_no_divisions(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()
    dfs = e.map(tm.makeTimeDataFrame, range(5, 10))

    df = yield _futures_to_dask_dataframe(dfs)
    assert not df.known_divisions
    assert list(df.columns) == list(tm.makeTimeDataFrame(5).columns)

    yield e._shutdown()


def test_futures_to_dask_dataframe(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            remote_dfs = e.map(lambda x: x, dfs)
            ddf = futures_to_dask_dataframe(remote_dfs, divisions=True)

            assert isinstance(ddf, dd.DataFrame)
            assert ddf.x.sum().compute(get=e.get) == sum([df.x.sum() for df in dfs])

            ddf2 = futures_to_collection(remote_dfs, divisions=True)
            assert type(ddf) == type(ddf2)
            assert ddf.dask == ddf2.dask


@gen_cluster(timeout=120)
def test_dataframes(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    dfs = [pd.DataFrame({'x': np.random.random(100),
                         'y': np.random.random(100)},
                        index=list(range(i, i + 100)))
           for i in range(0, 100*10, 100)]

    remote_dfs = e.map(lambda x: x, dfs)
    rdf = yield _futures_to_dask_dataframe(remote_dfs, divisions=True)
    name = 'foo'
    ldf = dd.DataFrame({(name, i): df for i, df in enumerate(dfs)},
                       name, dfs[0].columns,
                       list(range(0, 1000, 100)) + [999])

    assert rdf.divisions == ldf.divisions

    remote, = e.compute(rdf)
    result = yield remote._result()

    tm.assert_frame_equal(result,
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
        remote, = e.compute(f(rdf))
        remote = yield gen.with_timeout(timedelta(seconds=5), remote._result())
        assert_equal(local, remote)

    yield e._shutdown()


@gen_cluster(timeout=60)
def test__futures_to_dask_array(s, a, b):
    import dask.array as da
    e = Executor((s.ip, s.port), start=False)
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


@gen_cluster()
def test__stack(s, a, b):
    import dask.array as da
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    arrays = e.map(np.ones, [(5, 5)] * 6)
    y = yield _stack(arrays, axis=0)
    assert y.shape == (6, 5, 5)
    assert y.chunks == ((1, 1, 1, 1, 1, 1), (5,), (5,))

    y_result, = e.compute(y)
    yy = yield y_result._result()

    assert isinstance(yy, np.ndarray)
    assert yy.shape == y.shape
    assert (yy == 1).all()

    yield e._shutdown()


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


@gen_cluster()
def test__dask_array_collections(s, a, b):
    import dask.array as da
    e = Executor((s.ip, s.port), start=False)
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
        local = expr(x_local, y_local).compute(get=dask.get)

        remote, = e.compute(expr(x_remote, y_remote))
        remote = yield remote._result()

        assert np.all(local == remote)

    yield e._shutdown()


@gen_cluster()
def test__futures_to_dask_bag(s, a, b):
    import dask.bag as db
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    L = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    futures = yield e._scatter(L)

    rb = yield _futures_to_dask_bag(futures)
    assert isinstance(rb, db.Bag)
    assert rb.npartitions == len(L)

    lb = db.from_sequence([1, 2, 3, 4, 5, 6, 7, 8, 9], npartitions=3)

    exprs = [lambda x: x.map(lambda x: x + 1).sum(),
             lambda x: x.filter(lambda x: x % 2)]

    for expr in exprs:
        local = expr(lb).compute(get=dask.get)
        remote, = e.compute(expr(rb))
        remote = yield remote._result()

        assert local == remote

    yield e._shutdown()


def test_futures_to_dask_bag(loop):
    import dask.bag as db
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
            futures = e.scatter(data)
            b = futures_to_dask_bag(futures)

            assert isinstance(b, db.Bag)
            assert b.map(lambda x: x + 1).sum().compute(get=e.get) == sum(range(2, 11))


@pytest.mark.skipif(sys.platform!='linux',
                    reason='KQueue error - uncertain cause')
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

            y = futures_to_collection(remote_arrays, executor=e)
            assert x.dask == y.dask


@gen_cluster()
def test__futures_to_collection(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    remote_dfs = e.map(identity, dfs)
    ddf = yield _futures_to_collection(remote_dfs, divisions=True)
    ddf2 = yield _futures_to_dask_dataframe(remote_dfs, divisions=True)
    assert isinstance(ddf, dd.DataFrame)

    assert ddf.dask == ddf2.dask

    remote_arrays = e.map(np.arange, range(3, 5))
    x = yield _futures_to_collection(remote_arrays)
    y = yield _futures_to_dask_array(remote_arrays)

    assert type(x) == type(y)
    assert x.dask == y.dask

    remote_lists = yield e._scatter([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    b = yield _futures_to_collection(remote_lists)
    c = yield _futures_to_dask_bag(remote_lists)

    assert type(b) == type(c)
    assert b.dask == b.dask

    yield e._shutdown()

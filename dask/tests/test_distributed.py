import pytest
distributed = pytest.importorskip('distributed')

from functools import partial
import inspect
from operator import add
from tornado import gen

import dask
from dask import persist, delayed, compute
from dask.delayed import Delayed
from dask.utils import tmpdir
from distributed.client import wait, Client
from distributed.utils_test import gen_cluster, inc, cluster, loop  # flake8: noqa


if 'should_check_state' in inspect.getargspec(gen_cluster).args:
    gen_cluster = partial(gen_cluster, should_check_state=False)
    cluster = partial(cluster, should_check_state=False)


def test_can_import_client():
    from dask.distributed import Client  # noqa: F401


@gen_cluster(client=True)
def test_persist(c, s, a, b):
    x = delayed(inc)(1)
    x2, = persist(x)

    yield wait(x2)
    assert x2.key in a.data or x2.key in b.data

    y = delayed(inc)(10)
    y2, one = persist(y, 1)

    yield wait(y2)
    assert y2.key in a.data or y2.key in b.data


def test_persist_nested(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop):
            a = delayed(1) + 5
            b = a + 1
            c = a + 2
            result = persist({'a': a, 'b': [1, 2, b]}, (c, 2), 4, [5])
            assert isinstance(result[0]['a'], Delayed)
            assert isinstance(result[0]['b'][2], Delayed)
            assert isinstance(result[1][0], Delayed)

            sol = ({'a': 6, 'b': [1, 2, 7]}, (8, 2), 4, [5])
            assert compute(*result) == sol

            res = persist([a, b], c, 4, [5], traverse=False)
            assert res[0][0] is a
            assert res[0][1] is b
            assert res[1].compute() == 8
            assert res[2:] == (4, [5])


def test_futures_to_delayed_dataframe(loop):
    pd = pytest.importorskip('pandas')
    dd = pytest.importorskip('dask.dataframe')
    df = pd.DataFrame({'x': [1, 2, 3]})
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.scatter([df, df])
            ddf = dd.from_delayed(futures)
            dd.utils.assert_eq(ddf.compute(), pd.concat([df, df], axis=0))

            with pytest.raises(TypeError):
                ddf = dd.from_delayed([1, 2])


def test_futures_to_delayed_bag(loop):
    db = pytest.importorskip('dask.bag')
    L = [1, 2, 3]
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.scatter([L, L])
            b = db.from_delayed(futures)
            assert list(b) == L + L


def test_futures_to_delayed_array(loop):
    da = pytest.importorskip('dask.array')
    from dask.array.utils import assert_eq
    np = pytest.importorskip('numpy')
    x = np.arange(5)
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            futures = c.scatter([x, x])
            A = da.concatenate([da.from_delayed(f, shape=x.shape, dtype=x.dtype)
                                for f in futures], axis=0)
            assert_eq(A.compute(), np.concatenate([x, x], axis=0))


@gen_cluster(client=True)
def test_local_get_with_distributed_active(c, s, a, b):
    with dask.config.set(scheduler='sync'):
        x = delayed(inc)(1).persist()
    yield gen.sleep(0.01)
    assert not s.tasks # scheduler hasn't done anything

    y = delayed(inc)(2).persist(scheduler='sync')
    yield gen.sleep(0.01)
    assert not s.tasks # scheduler hasn't done anything



def test_to_hdf_distributed(loop):
    from ..dataframe.io.tests.test_hdf import test_to_hdf
    with cluster() as (s, [a, b]):
        with distributed.Client(s['address'], loop=loop):
            test_to_hdf()


@pytest.mark.xfail(reason='HDF not multi-process safe')
@pytest.mark.parametrize('npartitions', [1, 4, 10])
def test_to_hdf_scheduler_distributed(npartitions, loop):
    from ..dataframe.io.tests.test_hdf import test_to_hdf_schedulers
    with cluster() as (s, [a, b]):
        with distributed.Client(s['address'], loop=loop):
            test_to_hdf_schedulers(None, npartitions)


@gen_cluster(client=True)
def test_serializable_groupby_agg(c, s, a, b):
    pd = pytest.importorskip('pandas')
    dd = pytest.importorskip('dask.dataframe')
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [1, 0, 1, 0]})
    ddf = dd.from_pandas(df, npartitions=2)

    result = ddf.groupby('y').agg('count')

    yield c.compute(result)


def test_futures_in_graph(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x, y = delayed(1), delayed(2)
            xx = delayed(add)(x, x)
            yy = delayed(add)(y, y)
            xxyy = delayed(add)(xx, yy)

            xxyy2 = c.persist(xxyy)
            xxyy3 = delayed(add)(xxyy2, 10)

            assert xxyy3.compute(scheduler='dask.distributed') == ((1 + 1) + (2 + 2)) + 10


def test_zarr_distributed_roundtrip(loop):
    da = pytest.importorskip('dask.array')
    zarr = pytest.importorskip('zarr')
    assert_eq = da.utils.assert_eq
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with tmpdir() as d:
                a = da.zeros((3, 3), chunks=(1, 1))
                a.to_zarr(d)
                a2 = da.from_zarr(d)
                assert_eq(a, a2)
                assert a2.chunks == a.chunks


def test_zarr_in_memory_distributed_err(loop):
    da = pytest.importorskip('dask.array')
    zarr = pytest.importorskip('zarr')
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop,
                    client_kwargs={'set_as_default': True}) as c:
            with pytest.raises(RuntimeError):
                c = (1, 1)
                a = da.ones((3, 3), chunks=c)
                z = zarr.zeros_like(a, chunks=c)
                a.to_zarr(z)


def test_scheduler_equals_client(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:
            x = delayed(lambda: 1)()
            assert x.compute(scheduler=client) == 1
            assert client.run_on_scheduler(lambda dask_scheduler: dask_scheduler.story(x.key))

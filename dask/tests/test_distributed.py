import pytest
distributed = pytest.importorskip('distributed')

from functools import partial
import inspect
from tornado import gen

import dask
from dask import persist, delayed
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
    with dask.set_options(get=dask.get):
        x = delayed(inc)(1).persist()
    yield gen.sleep(0.01)
    assert not s.tasks # scheduler hasn't done anything

    y = delayed(inc)(2).persist(get=dask.get)
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

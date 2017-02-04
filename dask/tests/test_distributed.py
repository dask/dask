import pytest
pytest.importorskip('distributed')

from dask import persist, delayed
from distributed.client import _wait, Client
from distributed.utils_test import gen_cluster, inc, cluster, loop  # flake8: noqa


def test_can_import_client():
    from dask.distributed import Client # noqa: F401


@gen_cluster(client=True)
def test_persist(c, s, a, b):
    x = delayed(inc)(1)
    x2, = persist(x)

    yield _wait(x2)
    assert x2.key in a.data or x2.key in b.data

    y = delayed(inc)(10)
    y2, one = persist(y, 1)

    yield _wait(y2)
    assert y2.key in a.data or y2.key in b.data


def test_futures_to_delayed_dataframe(loop):
    pd = pytest.importorskip('pandas')
    dd = pytest.importorskip('dask.dataframe')
    df = pd.DataFrame({'x': [1, 2, 3]})
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:  # flake8: noqa
            futures = c.scatter([df, df])
            ddf = dd.from_delayed(futures)
            dd.utils.assert_eq(ddf.compute(), pd.concat([df, df], axis=0))

            with pytest.raises(TypeError):
                ddf = dd.from_delayed([1, 2])


def test_futures_to_delayed_bag(loop):
    db = pytest.importorskip('dask.bag')
    L = [1, 2, 3]
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:  # flake8: noqa
            futures = c.scatter([L, L])
            b = db.from_delayed(futures)
            assert list(b) == L + L


def test_futures_to_delayed_array(loop):
    da = pytest.importorskip('dask.array')
    from dask.array.utils import assert_eq
    np = pytest.importorskip('numpy')
    x = np.arange(5)
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:  # flake8: noqa
            futures = c.scatter([x, x])
            A = da.concatenate([da.from_delayed(f, shape=x.shape, dtype=x.dtype)
                                for f in futures], axis=0)
            assert_eq(A.compute(), np.concatenate([x, x], axis=0))

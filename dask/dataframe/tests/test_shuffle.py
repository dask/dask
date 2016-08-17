import pandas as pd
import pandas.util.testing as tm
import pytest
import numpy as np

import dask.dataframe as dd
from dask.dataframe.shuffle import (shuffle, hash_series, partitioning_index,
        rearrange_by_column, rearrange_by_divisions)
from dask.async import get_sync
from dask.dataframe.utils import eq, make_meta

dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [1, 4, 7]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [2, 5, 8]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [3, 6, 9]},
                              index=[9, 9, 9])}
meta = make_meta({'a': 'i8', 'b': 'i8'}, index=pd.Index([], 'i8'))
d = dd.DataFrame(dsk, 'x', meta, [0, 4, 9, 9])
full = d.compute()


shuffle_func = shuffle  # conflicts with keyword argument


@pytest.mark.parametrize('shuffle', ['disk', 'tasks'])
def test_shuffle(shuffle):
    s = shuffle_func(d, d.b, shuffle=shuffle)
    assert isinstance(s, dd.DataFrame)
    assert s.npartitions == d.npartitions

    x = get_sync(s.dask, (s._name, 0))
    y = get_sync(s.dask, (s._name, 1))

    assert not (set(x.b) & set(y.b))  # disjoint
    assert set(s.dask).issuperset(d.dask)

    assert shuffle_func(d, d.b)._name == shuffle_func(d, d.b)._name


def test_default_partitions():
    assert shuffle(d, d.b).npartitions == d.npartitions


def test_shuffle_npatitions_task():
    df = pd.DataFrame({'x': np.random.random(100)})
    ddf = dd.from_pandas(df, npartitions=10)
    s = shuffle(ddf, ddf.x, shuffle='tasks', npartitions=17, max_branch=4)
    sc = s.compute(get=get_sync)
    assert s.npartitions == 17
    assert set(s.dask).issuperset(set(ddf.dask))

    assert len(sc) == len(df)
    assert list(s.columns) == list(df.columns)
    assert set(map(tuple, sc.values.tolist())) == \
           set(map(tuple, df.values.tolist()))


@pytest.mark.parametrize('method', ['disk', 'tasks'])
def test_index_with_non_series(method):
    tm.assert_frame_equal(shuffle(d, d.b, shuffle=method).compute(),
                          shuffle(d, 'b', shuffle=method).compute())


@pytest.mark.parametrize('method', ['disk', 'tasks'])
def test_index_with_dataframe(method):
    assert sorted(shuffle(d, d[['b']], shuffle=method).compute().values.tolist()) ==\
           sorted(shuffle(d, ['b'], shuffle=method).compute().values.tolist()) ==\
           sorted(shuffle(d, 'b', shuffle=method).compute().values.tolist())


@pytest.mark.parametrize('method', ['disk', 'tasks'])
def test_shuffle_from_one_partition_to_one_other(method):
    df = pd.DataFrame({'x': [1, 2, 3]})
    a = dd.from_pandas(df, 1)

    for i in [1, 2]:
        b = shuffle(a, 'x', npartitions=i, shuffle=method)
        assert len(a.compute(get=get_sync)) == len(b.compute(get=get_sync))


@pytest.mark.parametrize('method', ['disk', 'tasks'])
def test_shuffle_empty_partitions(method):
    df = pd.DataFrame({'x': [1, 2, 3] * 10})
    ddf = dd.from_pandas(df, npartitions=3)
    s = shuffle(ddf, ddf.x, npartitions=6, shuffle=method)
    parts = s._get(s.dask, s._keys())
    for p in parts:
        assert s.columns == p.columns


df2 = pd.DataFrame({'i32': np.array([1, 2, 3] * 3, dtype='int32'),
                    'f32': np.array([None, 2.5, 3.5] * 3, dtype='float32'),
                    'cat': pd.Series(['a', 'b', 'c'] * 3).astype('category'),
                    'obj': pd.Series(['d', 'e', 'f'] * 3),
                    'bool': np.array([True, False, True]*3),
                    'dt': pd.Series(pd.date_range('20130101', periods=9)),
                    'dt_tz': pd.Series(pd.date_range('20130101', periods=9, tz='US/Eastern')),
                    'td': pd.Series(pd.timedelta_range('2000', periods=9))})


def test_hash_series():
    for name, s in df2.iteritems():
        np.testing.assert_equal(hash_series(s), hash_series(s))


def test_partitioning_index():
    res = partitioning_index(df2.i32, 3)
    exp = np.array([1, 2, 0] * 3)
    np.testing.assert_equal(res, exp)

    res = partitioning_index(df2[['i32']], 3)
    np.testing.assert_equal(res, exp)

    res = partitioning_index(df2[['cat', 'bool', 'f32']], 2)
    assert ((0 <= res) & (res < 2)).all()

    res = partitioning_index(df2.index, 4)
    exp = np.array([0, 1, 2, 3, 0, 1, 2, 3, 0])
    np.testing.assert_equal(res, exp)


@pytest.mark.parametrize('npartitions', [1, 4, 7, pytest.mark.slow(23)])
def test_set_partition_tasks(npartitions):
    df = pd.DataFrame({'x': np.random.random(100),
                       'y': np.random.random(100) // 0.2},
                       index=np.random.random(100))

    ddf = dd.from_pandas(df, npartitions=npartitions)

    eq(df.set_index('x'),
       ddf.set_index('x', shuffle='tasks'))

    eq(df.set_index('y'),
       ddf.set_index('y', shuffle='tasks'))

    eq(df.set_index(df.x),
       ddf.set_index(ddf.x, shuffle='tasks'))

    eq(df.set_index(df.x + df.y),
       ddf.set_index(ddf.x + ddf.y, shuffle='tasks'))

    eq(df.set_index(df.x + 1),
       ddf.set_index(ddf.x + 1, shuffle='tasks'))

    eq(df.set_index(df.index),
       ddf.set_index(ddf.index, shuffle='tasks'))


@pytest.mark.parametrize('shuffle', ['disk', 'tasks'])
def test_set_index_self_index(shuffle):
    df = pd.DataFrame({'x': np.random.random(100),
                       'y': np.random.random(100) // 0.2},
                       index=np.random.random(100))

    a = dd.from_pandas(df, npartitions=4)
    b = a.set_index(a.index, shuffle=shuffle)
    assert a is b

    eq(b, df.set_index(df.index))


@pytest.mark.parametrize('shuffle', ['tasks'])
def test_set_partition_names(shuffle):
    df = pd.DataFrame({'x': np.random.random(100),
                       'y': np.random.random(100) // 0.2},
                       index=np.random.random(100))

    ddf = dd.from_pandas(df, npartitions=4)

    assert (set(ddf.set_index('x', shuffle=shuffle).dask) ==
            set(ddf.set_index('x', shuffle=shuffle).dask))
    assert (set(ddf.set_index('x', shuffle=shuffle).dask) !=
            set(ddf.set_index('y', shuffle=shuffle).dask))
    assert (set(ddf.set_index('x', max_branch=4, shuffle=shuffle).dask) !=
            set(ddf.set_index('x', max_branch=3, shuffle=shuffle).dask))
    assert (set(ddf.set_index('x', drop=True, shuffle=shuffle).dask) !=
            set(ddf.set_index('x', drop=False, shuffle=shuffle).dask))


@pytest.mark.parametrize('shuffle', ['disk', 'tasks'])
def test_set_partition_tasks_2(shuffle):
    df = dd.demo.make_timeseries('2000', '2004',
            {'value': float, 'name': str, 'id': int},
            freq='2H', partition_freq='1M', seed=1)

    df2 = df.set_index('name', shuffle=shuffle)
    df2.value.sum().compute(get=get_sync)


@pytest.mark.parametrize('shuffle', ['disk', 'tasks'])
def test_set_partition_tasks_3(shuffle):
    df = pd.DataFrame(np.random.random((10, 2)), columns=['x', 'y'])
    ddf = dd.from_pandas(df, npartitions=5)

    ddf2 = ddf.set_index('x', shuffle=shuffle, max_branch=2)
    df2 = df.set_index('x')
    eq(df2, ddf2)
    assert ddf2.npartitions == ddf.npartitions


@pytest.mark.parametrize('shuffle', ['tasks', 'disk'])
def test_shuffle_sort(shuffle):
    df = pd.DataFrame({'x': [1, 2, 3, 2, 1], 'y': [9, 8, 7, 1, 5]})
    ddf = dd.from_pandas(df, npartitions=3)

    df2 = df.set_index('x').sort_index()
    ddf2 = ddf.set_index('x', shuffle=shuffle)

    eq(ddf2.loc[2:3], df2.loc[2:3])


@pytest.mark.parametrize('shuffle', ['tasks', 'disk'])
def test_rearrange(shuffle):
    df = pd.DataFrame({'x': range(10)})
    ddf = dd.from_pandas(df, npartitions=4)
    ddf2 = ddf.assign(y=ddf.x % 4)

    result = rearrange_by_column(ddf2, 'y', max_branch=32, shuffle=shuffle)
    assert result.npartitions == ddf.npartitions
    assert set(ddf.dask).issubset(result.dask)

    # Every value in exactly one partition
    a = result.compute()
    parts = get_sync(result.dask, result._keys())
    for i in a.y.drop_duplicates():
        assert sum(i in part.y for part in parts) == 1


def test_rearrange_by_column_with_narrow_divisions():
    from dask.dataframe.tests.test_multi import list_eq
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    df = rearrange_by_divisions(a, 'x', (0, 2, 5))
    list_eq(df, a)

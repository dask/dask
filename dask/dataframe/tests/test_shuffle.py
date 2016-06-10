import pandas as pd
import pandas.util.testing as tm
import pytest
import numpy as np

import dask.dataframe as dd
from dask.dataframe.shuffle import shuffle, hash_series, partitioning_index
from dask.async import get_sync
from dask.dataframe.utils import eq

dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [1, 4, 7]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [2, 5, 8]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [3, 6, 9]},
                              index=[9, 9, 9])}
d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
full = d.compute()


def test_shuffle():
    s = shuffle(d, d.b, npartitions=2)
    assert isinstance(s, dd.DataFrame)
    assert s.npartitions == 2

    x = get_sync(s.dask, (s._name, 0))
    y = get_sync(s.dask, (s._name, 1))

    assert not (set(x.b) & set(y.b))  # disjoint

    assert shuffle(d, d.b, npartitions=2)._name == shuffle(d, d.b, npartitions=2)._name


def test_default_partitions():
    assert shuffle(d, d.b).npartitions == d.npartitions


def test_index_with_non_series():
    tm.assert_frame_equal(shuffle(d, d.b).compute(),
                          shuffle(d, 'b').compute())


def test_index_with_dataframe():
    assert sorted(shuffle(d, d[['b']]).compute().values.tolist()) ==\
           sorted(shuffle(d, ['b']).compute().values.tolist()) ==\
           sorted(shuffle(d, 'b').compute().values.tolist())


def test_shuffle_from_one_partition_to_one_other():
    df = pd.DataFrame({'x': [1, 2, 3]})
    a = dd.from_pandas(df, 1)

    for i in [1, 2]:
        b = shuffle(a, 'x', i)
        assert len(a.compute(get=get_sync)) == len(b.compute(get=get_sync))


def test_shuffle_empty_partitions():
    df = pd.DataFrame({'x': [1, 2, 3] * 10})
    ddf = dd.from_pandas(df, npartitions=3)
    s = shuffle(ddf, ddf.x, npartitions=6)
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
    exp = np.array([1, 1, 0] * 3)
    np.testing.assert_equal(res, exp)

    res = partitioning_index(df2.index, 4)
    exp = np.array([0, 1, 2, 3, 0, 1, 2, 3, 0])
    np.testing.assert_equal(res, exp)


@pytest.mark.parametrize('npartitions', [1, 4, 7, pytest.mark.slow(23)])
def test_set_partition_tasks(npartitions):
    df = pd.DataFrame({'x': np.random.random(100),
                       'y': np.random.random(100) // 0.2},
                       index=np.random.random(100))

    ddf = dd.from_pandas(df, npartitions=npartitions)

    divisions = [0, .25, .50, .75, 1.0]

    eq(df.set_index('x'),
       ddf.set_index('x', method='tasks'))

    eq(df.set_index('y'),
       ddf.set_index('y', method='tasks'))

    eq(df.set_index(df.x),
       ddf.set_index(ddf.x, method='tasks'))

    eq(df.set_index(df.x + df.y),
       ddf.set_index(ddf.x + ddf.y, method='tasks'))

    eq(df.set_index(df.x + 1),
       ddf.set_index(ddf.x + 1, method='tasks'))

    # eq(df.set_index(df.index),
    #    ddf.set_index(ddf.index, method='tasks'))

def test_set_partition_tasks_names():
    df = pd.DataFrame({'x': np.random.random(100),
                       'y': np.random.random(100) // 0.2},
                       index=np.random.random(100))

    ddf = dd.from_pandas(df, npartitions=4)

    divisions = [0, .25, .50, .75, 1.0]

    assert (set(ddf.set_index('x', method='tasks').dask) ==
            set(ddf.set_index('x', method='tasks').dask))
    assert (set(ddf.set_index('x', method='tasks').dask) !=
            set(ddf.set_index('y', method='tasks').dask))
    assert (set(ddf.set_index('x', max_branch=4, method='tasks').dask) !=
            set(ddf.set_index('x', max_branch=3, method='tasks').dask))
    assert (set(ddf.set_index('x', drop=True, method='tasks').dask) !=
            set(ddf.set_index('x', drop=False, method='tasks').dask))


def test_set_partition_tasks_2():
    df = dd.demo.make_timeseries('2000', '2004',
            {'value': float, 'name': str, 'id': int},
            freq='2H', partition_freq='1M', seed=1)

    df2 = df.set_index('name', method='tasks')
    df2.value.sum().compute(get=get_sync)


def test_set_partition_tasks_3():
    npartitions = 5
    df = pd.DataFrame(np.random.random((10, 2)), columns=['x', 'y'])
    ddf = dd.from_pandas(df, npartitions=5)

    ddf2 = ddf.set_index('x', method='tasks', max_branch=2)
    df2 = df.set_index('x')
    eq(df2, ddf2)
    assert ddf2.npartitions == ddf.npartitions


def test_shuffle_pre_partition():
    from dask.dataframe.shuffle import (shuffle_pre_partition_scalar,
                                        shuffle_pre_partition_series)
    df = pd.DataFrame({'x': np.random.random(10),
                       'y': np.random.random(10)},
                       index=np.random.random(10))
    divisions = df.x.quantile([0, 0.2, 0.4, 0.6, 0.8, 1.0]).tolist()

    for ind, pre in [('x', shuffle_pre_partition_scalar),
                     (df.x, shuffle_pre_partition_series)]:
        result = pre(df, ind, divisions, True)
        assert list(result.columns)[:2] == ['x', 'y']
        assert result.index.name == 'partitions'
        for x, part in result.reset_index()[['x', 'partitions']].values.tolist():
            part = int(part)
            if x == divisions[-1]:
                assert part == len(divisions) - 2
            else:
                assert divisions[part] <= x < divisions[part + 1]


@pytest.mark.parametrize('method', ['tasks', 'disk'])
def test_shuffle_sort(method):
    df = pd.DataFrame({'x': [1, 2, 3, 2, 1], 'y': [9, 8, 7, 1, 5]})
    ddf = dd.from_pandas(df, npartitions=3)

    df2 = df.set_index('x').sort_index()
    ddf2 = ddf.set_index('x', method=method)

    eq(ddf2.loc[2:3], df2.loc[2:3])

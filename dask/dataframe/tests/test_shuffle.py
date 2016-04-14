import pandas as pd
import pandas.util.testing as tm
import numpy as np

import dask.dataframe as dd
from dask.dataframe.shuffle import shuffle, hash_series, partitioning_index
from dask.async import get_sync

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

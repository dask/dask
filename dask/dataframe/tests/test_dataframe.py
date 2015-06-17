from operator import getitem

import pandas as pd
import pandas.util.testing as tm
import numpy as np

import dask
from dask.utils import raises
import dask.dataframe as dd
from dask.dataframe.core import get, concat, pd_split


def eq(a, b):
    if hasattr(a, 'dask'):
        a = a.compute(get=dask.get)
    if hasattr(b, 'dask'):
        b = b.compute(get=dask.get)
    if isinstance(a, pd.DataFrame):
        a = a.sort_index()
        b = b.sort_index()
        tm.assert_frame_equal(a, b)
        return True
    if isinstance(a, pd.Series):
        tm.assert_series_equal(a, b)
        return True
    assert np.allclose(a, b)
    return True


dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
d = dd.DataFrame(dsk, 'x', ['a', 'b'], [4, 9])
full = d.compute()


def test_Dataframe():
    result = (d['a'] + 1).compute()
    expected = pd.Series([2, 3, 4, 5, 6, 7, 8, 9, 10],
                        index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
                        name='a')

    assert eq(result, expected)

    assert list(d.columns) == list(['a', 'b'])

    assert eq(d.head(2), dsk[('x', 0)].head(2))
    assert eq(d['a'].head(2), dsk[('x', 0)]['a'].head(2))

    full = d.compute()
    assert eq(d[d['b'] > 2], full[full['b'] > 2])
    assert eq(d[['a', 'b']], full[['a', 'b']])
    assert eq(d.a, full.a)
    assert d.b.mean().compute() == full.b.mean()
    assert np.allclose(d.b.var().compute(), full.b.var())
    assert np.allclose(d.b.std().compute(), full.b.std())

    assert d.index._name == d.index._name  # this is deterministic

    assert repr(d)


def test_Series():
    assert isinstance(d.a, dd.Series)
    assert isinstance(d.a + 1, dd.Series)
    assert raises(Exception, lambda: d + 1)


def test_attributes():
    assert 'a' in dir(d)
    assert 'foo' not in dir(d)
    assert raises(AttributeError, lambda: d.foo)


def test_column_names():
    assert d.columns == ('a', 'b')
    assert d[['b', 'a']].columns == ('b', 'a')
    assert d['a'].columns == ('a',)
    assert (d['a'] + 1).columns == ('a',)
    assert (d['a'] + d['b']).columns == (None,)


def test_set_index():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 2, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 5, 8]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [9, 1, 8]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [4, 9])
    full = d.compute()

    d2 = d.set_index('b', npartitions=3)
    assert d2.npartitions == 3
    # assert eq(d2, full.set_index('b').sort())
    assert str(d2.compute().sort(['a'])) == str(full.set_index('b').sort(['a']))

    d3 = d.set_index(d.b, npartitions=3)
    assert d3.npartitions == 3
    # assert eq(d3, full.set_index(full.b).sort())
    assert str(d3.compute().sort(['a'])) == str(full.set_index(full.b).sort(['a']))

    d2 = d.set_index('b')
    assert str(d2.compute().sort(['a'])) == str(full.set_index('b').sort(['a']))


def test_split_apply_combine_on_series():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 6], 'b': [4, 2., 7]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 2, 6], 'b': [3, 3, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [4, 3, 7], 'b': [1, 1, 3]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [4, 9])
    full = d.compute()

    assert eq(d.groupby('b').a.sum(), full.groupby('b').a.sum())
    assert eq(d.groupby(d.b + 1).a.sum(), full.groupby(full.b + 1).a.sum())
    assert eq(d.groupby('b').a.min(), full.groupby('b').a.min())
    assert eq(d.groupby('a').b.max(), full.groupby('a').b.max())
    assert eq(d.groupby('b').a.count(), full.groupby('b').a.count())

    assert eq(d.groupby('a').b.mean(), full.groupby('a').b.mean())
    assert eq(d.groupby(d.a > 3).b.mean(), full.groupby(full.a > 3).b.mean())


def test_arithmetic():
    assert eq(d.a + d.b, full.a + full.b)
    assert eq(d.a * d.b, full.a * full.b)
    assert eq(d.a - d.b, full.a - full.b)
    assert eq(d.a / d.b, full.a / full.b)
    assert eq(d.a & d.b, full.a & full.b)
    assert eq(d.a | d.b, full.a | full.b)
    assert eq(d.a ^ d.b, full.a ^ full.b)
    assert eq(d.a // d.b, full.a // full.b)
    assert eq(d.a ** d.b, full.a ** full.b)
    assert eq(d.a % d.b, full.a % full.b)
    assert eq(d.a > d.b, full.a > full.b)
    assert eq(d.a < d.b, full.a < full.b)
    assert eq(d.a >= d.b, full.a >= full.b)
    assert eq(d.a <= d.b, full.a <= full.b)
    assert eq(d.a == d.b, full.a == full.b)
    assert eq(d.a != d.b, full.a != full.b)

    assert eq(d.a + 2, full.a + 2)
    assert eq(d.a * 2, full.a * 2)
    assert eq(d.a - 2, full.a - 2)
    assert eq(d.a / 2, full.a / 2)
    assert eq(d.a & True, full.a & True)
    assert eq(d.a | True, full.a | True)
    assert eq(d.a ^ True, full.a ^ True)
    assert eq(d.a // 2, full.a // 2)
    assert eq(d.a ** 2, full.a ** 2)
    assert eq(d.a % 2, full.a % 2)
    assert eq(d.a > 2, full.a > 2)
    assert eq(d.a < 2, full.a < 2)
    assert eq(d.a >= 2, full.a >= 2)
    assert eq(d.a <= 2, full.a <= 2)
    assert eq(d.a == 2, full.a == 2)
    assert eq(d.a != 2, full.a != 2)

    assert eq(2 + d.b, 2 + full.b)
    assert eq(2 * d.b, 2 * full.b)
    assert eq(2 - d.b, 2 - full.b)
    assert eq(2 / d.b, 2 / full.b)
    assert eq(True & d.b, True & full.b)
    assert eq(True | d.b, True | full.b)
    assert eq(True ^ d.b, True ^ full.b)
    assert eq(2 // d.b, 2 // full.b)
    assert eq(2 ** d.b, 2 ** full.b)
    assert eq(2 % d.b, 2 % full.b)
    assert eq(2 > d.b, 2 > full.b)
    assert eq(2 < d.b, 2 < full.b)
    assert eq(2 >= d.b, 2 >= full.b)
    assert eq(2 <= d.b, 2 <= full.b)
    assert eq(2 == d.b, 2 == full.b)
    assert eq(2 != d.b, 2 != full.b)

    assert eq(-d.a, -full.a)
    assert eq(abs(d.a), abs(full.a))
    assert eq(~(d.a == d.b), ~(full.a == full.b))
    assert eq(~(d.a == d.b), ~(full.a == full.b))


def test_reductions():
    assert eq(d.b.sum(), full.b.sum())
    assert eq(d.b.min(), full.b.min())
    assert eq(d.b.max(), full.b.max())
    assert eq(d.b.count(), full.b.count())
    assert eq(d.b.std(), full.b.std())
    assert eq(d.b.var(), full.b.var())
    assert eq(d.b.mean(), full.b.mean())


def test_map_blocks_multi_argument():
    assert eq(dd.map_blocks(lambda a, b: a + b, 'c', d.a, d.b),
              full.a + full.b)
    assert eq(dd.map_blocks(lambda a, b, c: a + b + c, 'c', d.a, d.b, 1),
              full.a + full.b + 1)


def test_map_blocks():
    assert eq(d.map_blocks(lambda df: df, 'a'), full)


def test_drop_duplicates():
    assert eq(d.a.drop_duplicates(), full.a.drop_duplicates())


def test_full_groupby():
    assert raises(Exception, lambda: d.groupby('does_not_exist'))
    assert raises(Exception, lambda: d.groupby('a').does_not_exist)
    assert 'b' in dir(d.groupby('a'))
    def func(df):
        df['b'] = df.b - df.b.mean()
        return df
    # assert eq(d.groupby('a').apply(func), full.groupby('a').apply(func))


def test_groupby_on_index():
    e = d.set_index('a')
    assert eq(d.groupby('a').b.mean(), e.groupby(e.index).b.mean())

    def func(df):
        df['b'] = df.b - df.b.mean()
        return df

    assert eq(d.groupby('a').apply(func).set_index('a'),
              e.groupby(e.index).apply(func))


def test_set_partition():
    d2 = d.set_partition('b', [2])
    assert d2.divisions == (2,)
    expected = full.set_index('b').sort(ascending=True)
    assert eq(d2.compute().sort(ascending=True), expected)


def test_categorize():
    dsk = {('x', 0): pd.DataFrame({'a': ['Alice', 'Bob', 'Alice'],
                                   'b': ['C', 'D', 'E']},
                                   index=[0, 1, 2]),
           ('x', 1): pd.DataFrame({'a': ['Bob', 'Charlie', 'Charlie'],
                                   'b': ['A', 'A', 'B']},
                                   index=[3, 4, 5])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [3])
    full = d.compute()

    c = d.categorize('a')
    cfull = c.compute()
    assert cfull.dtypes['a'] == 'category'
    assert cfull.dtypes['b'] == 'O'

    assert list(cfull.a.astype('O')) == list(full.a)

    assert (get(c.dask, c._keys()[:1])[0].dtypes == cfull.dtypes).all()

    assert (d.categorize().compute().dtypes == 'category').all()


def test_dtype():
    assert (d.dtypes == full.dtypes).all()


def test_cache():
    d2 = d.cache()
    assert all(task[0] == getitem for task in d2.dask.values())


def test_value_counts():
    result = d.b.value_counts().compute()
    expected = full.b.value_counts()
    assert eq(result.sort_index(), expected.sort_index())


def test_isin():
    assert eq(d.a.isin([0, 1, 2]), full.a.isin([0, 1, 2]))


def test_len():
    assert len(d) == len(full)


def test_quantiles():
    result = d.b.quantiles([30, 70]).compute()
    assert len(result) == 2
    assert result[0] == 0
    assert 3 < result[1] < 7


def test_index():
    assert eq(d.index, full.index)


def test_loc():
    assert eq(d.loc[5], full.loc[5])
    assert eq(d.loc[3:8], full.loc[3:8])
    assert eq(d.loc[:8], full.loc[:8])
    assert eq(d.loc[3:], full.loc[3:])


def test_loc_with_text_dates():
    A = tm.makeTimeSeries(10).iloc[:5]
    B = tm.makeTimeSeries(10).iloc[5:]
    s = dd.Series({('df', 0): A, ('df', 1): B}, 'df', None, [A.index.max()])

    assert eq(s.loc['2000': '2010'], s)
    assert len(s.loc['2000-01-03': '2000-01-05'].compute()) == 3


def test_iloc_raises():
    assert raises(AttributeError, lambda: d.iloc[:5])


def test_assign():
    assert eq(d.assign(c=d.a + 1, e=d.a + d.b),
              full.assign(c=full.a + 1, e=full.a + full.b))


def test_map():
    assert eq(d.a.map(lambda x: x + 1), full.a.map(lambda x: x + 1))


def test_concat():
    x = concat([pd.DataFrame(columns=['a', 'b']),
                pd.DataFrame(columns=['a', 'b'])])
    assert list(x.columns) == ['a', 'b']
    assert len(x) == 0


def test_args():
    e = d.assign(c=d.a + 1)
    f = type(e)(*e._args)
    assert eq(e, f)
    assert eq(d.a, type(d.a)(*d.a._args))
    assert eq(d.a.sum(), type(d.a.sum())(*d.a.sum()._args))


def test_known_divisions():
    assert d.known_divisions

    df = dd.DataFrame({('x', 0): 'foo', ('x', 1): 'bar'}, 'x',
                      ['a', 'b'], divisions=[None])
    assert not df.known_divisions

    df = dd.DataFrame({('x', 0): 'foo'}, 'x',
                      ['a', 'b'], divisions=[])
    assert d.known_divisions

def test_unkonwn_divisions():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]}),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]})}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [None, None])
    full = d.compute(get=dask.get)

    assert eq(d.a.sum(), full.a.sum())
    assert eq(d.a + d.b + 1, full.a + full.b + 1)

    assert raises(ValueError, lambda: d.loc[3])


def test_concat():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]}),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]})}
    a = dd.DataFrame(dsk, 'x', ['a', 'b'], [None, None])
    dsk = {('y', 0): pd.DataFrame({'a': [10, 20, 30], 'b': [40, 50, 60]}),
           ('y', 1): pd.DataFrame({'a': [40, 50, 60], 'b': [30, 20, 10]}),
           ('y', 2): pd.DataFrame({'a': [70, 80, 90], 'b': [0, 0, 0]})}
    b = dd.DataFrame(dsk, 'y', ['a', 'b'], [None, None])

    c = dd.concat([a, b])

    assert c.npartitions == a.npartitions + b.npartitions

    assert eq(pd.concat([a.compute(), b.compute()]), c)


def test_dataframe_series_are_dillable():
    try:
        import dill
    except ImportError:
        return
    e = d.groupby(d.a).b.sum()
    f = dill.loads(dill.dumps(e))
    assert eq(e, f)


def test_random_partitions():
    a, b = d.random_split([0.5, 0.5])
    assert isinstance(a, dd.DataFrame)
    assert isinstance(b, dd.DataFrame)

    assert len(a.compute()) + len(b.compute()) == len(full)


def test_from_dataframe():
    a = list('aaaaaaabbbbbbbbccccccc')
    df = pd.DataFrame(dict(a=a, b=np.random.randn(len(a))))
    ddf = dd.from_dataframe(df, 3)
    assert len(ddf.dask) == 3
    tm.assert_frame_equal(df, ddf.compute())

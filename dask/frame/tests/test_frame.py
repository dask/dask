import dask.frame as df
from dask.frame.core import linecount, compute, get
from dask.frame.shuffle import shard_df_on_index
import pandas.util.testing as tm
from operator import getitem
import pandas as pd
import numpy as np
from dask.utils import filetext, raises
import dask

def eq(a, b):
    if isinstance(a, df.Frame):
        a = a.compute(get=dask.get)
    if isinstance(b, df.Frame):
        b = b.compute(get=dask.get)
    if isinstance(a, pd.DataFrame):
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
d = df.Frame(dsk, 'x', ['a', 'b'], [4, 9])
full = d.compute()

def test_frame():
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

    assert repr(d)



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


text = """
name,amount
Alice,100
Bob,-200
Charlie,300
Dennis,400
Edith,-500
Frank,600
""".strip()


def test_linecount():
    with filetext(text) as fn:
        assert linecount(fn) == 7


def test_read_csv():
    with filetext(text) as fn:
        f = df.read_csv(fn, chunksize=3)
        assert list(f.columns) == ['name', 'amount']
        assert f.npartitions == 2
        assert eq(f, pd.read_csv(fn))

    with filetext(text) as fn:
        f = df.read_csv(fn, chunksize=4)
        assert f.npartitions == 2

        f = df.read_csv(fn)


def test_set_index():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 2, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 5, 8]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [9, 1, 8]},
                                  index=[9, 9, 9])}
    d = df.Frame(dsk, 'x', ['a', 'b'], [4, 9])
    full = d.compute()

    d2 = d.set_index('b', npartitions=3, out_chunksize=3)
    assert d2.npartitions == 3
    # assert eq(d2, full.set_index('b').sort())
    assert str(d2.compute()) == str(full.set_index('b').sort())

    d3 = d.set_index(d.b, npartitions=3, out_chunksize=3)
    assert d3.npartitions == 3
    # assert eq(d3, full.set_index(full.b).sort())
    assert str(d3.compute()) == str(full.set_index(full.b).sort())


def test_shard_df_on_index():
    f = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]},
                      index=[1, 2, 3, 4, 4])

    result = list(df.shuffle.shard_df_on_index(f, [2, 7]))
    assert eq(result[0], f.loc[[1]])
    assert eq(result[1], f.loc[[2, 3, 4]])
    assert eq(result[2], pd.DataFrame(columns=['a', 'b'], dtype=f.dtypes))


def test_shard_df_on_index():
    f = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]},
                      index=['a', 'b', 'c', 'd', 'e'])
    result = list(shard_df_on_index(f, ['b', 'd']))
    assert eq(result[0], f.iloc[:1])
    assert eq(result[1], f.iloc[1:3])
    assert eq(result[2], f.iloc[3:])


    f = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 2, 6]},
                     index=[0, 1, 3]).set_index('b').sort()

    result = list(shard_df_on_index(f, [4, 9]))
    assert eq(result[0], f.iloc[0:1])
    assert eq(result[1], f.iloc[1:3])
    assert eq(result[2], f.iloc[3:])


def test_from_array():
    x = np.array([(i, i*10) for i in range(10)],
                 dtype=[('a', 'i4'), ('b', 'i4')])
    d = df.from_array(x, chunksize=4)

    assert list(d.columns) == ['a', 'b']
    assert d.blockdivs == (4, 8)

    assert (d.compute().to_records(index=False) == x).all()


def test_split_apply_combine_on_series():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 6], 'b': [4, 2., 7]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 2, 6], 'b': [3, 3, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [4, 3, 7], 'b': [1, 1, 3]},
                                  index=[9, 9, 9])}
    d = df.Frame(dsk, 'x', ['a', 'b'], [4, 9])
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


def test_map_blocks():
    assert eq(d.map_blocks(lambda df: df), full)


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


def test_set_partition():
    d2 = d.set_partition('b', [2])
    assert d2.blockdivs == (2,)
    expected = full.set_index('b').sort(ascending=True)
    assert eq(d2, expected)


def test_categorize():
    dsk = {('x', 0): pd.DataFrame({'a': ['Alice', 'Bob', 'Alice'],
                                   'b': ['C', 'D', 'E']},
                                   index=[0, 1, 2]),
           ('x', 1): pd.DataFrame({'a': ['Bob', 'Charlie', 'Charlie'],
                                   'b': ['A', 'A', 'B']},
                                   index=[3, 4, 5])}
    d = df.Frame(dsk, 'x', ['a', 'b'], [3])
    full = d.compute()

    c = d.categorize('a')
    cfull = c.compute()
    assert cfull.dtypes['a'] == 'category'
    assert cfull.dtypes['b'] == 'O'

    assert list(cfull.a.astype('string')) == list(full.a)

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

import dask.dataframe as dd
from dask.dataframe.core import (linecount, compute, get,
        dataframe_from_ctable, rewrite_rules, concat)
from toolz import valmap
import pandas.util.testing as tm
from operator import getitem
import pandas as pd
import numpy as np
from dask.utils import filetext, raises, tmpfile
import gzip
import bz2
import dask
import bcolz
from pframe import pframe

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


def test_linecount_bz2():
    with tmpfile('bz2') as fn:
        f = bz2.BZ2File(fn, 'wb')
        for line in text.split('\n'):
            f.write(line.encode('ascii'))
            f.write(b'\n')
        f.close()
        assert linecount(fn) == 7


def test_linecount_gzip():
    with tmpfile('gz') as fn:
        f = gzip.open(fn, 'wb')
        for line in text.split('\n'):
            f.write(line.encode('ascii'))
            f.write(b'\n')
        f.close()
        assert linecount(fn) == 7


def test_read_csv():
    with filetext(text) as fn:
        f = dd.read_csv(fn, chunksize=3)
        assert list(f.columns) == ['name', 'amount']
        assert f.npartitions == 2
        assert eq(f, pd.read_csv(fn))

    with filetext(text) as fn:
        f = dd.read_csv(fn, chunksize=4)
        assert f.npartitions == 2

        f = dd.read_csv(fn)


def test_read_csv_categorize():
    with filetext(text) as fn:
        f = dd.read_csv(fn, chunksize=3, categorize=True)
        assert list(f.dtypes) == ['category', 'i8']

        expected = pd.read_csv(fn)
        expected['name'] = expected.name.astype('category')
        assert eq(f, expected)


datetime_csv_file = """
name,amount,when
Alice,100,2014-01-01
Bob,200,2014-01-01
Charlie,300,2014-01-01
""".strip()

def test_read_csv_categorize_with_parse_dates():
    with filetext(datetime_csv_file) as fn:
        f = dd.read_csv(fn, chunksize=2, categorize=True, parse_dates=['when'])
        assert list(f.dtypes) == ['category', 'i8', 'M8[ns]']


def test_read_csv_categorize_and_index():
    with filetext(text) as fn:
        f = dd.read_csv(fn, chunksize=3, index='amount')
        assert f.index.compute().name == 'amount'

        expected = pd.read_csv(fn).set_index('amount')
        expected['name'] = expected.name.astype('category')
        assert eq(f, expected)


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


def test_from_array():
    x = np.array([(i, i*10) for i in range(10)],
                 dtype=[('a', 'i4'), ('b', 'i4')])
    d = dd.from_array(x, chunksize=4)

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


def test_groupby_on_index():
    e = d.set_index('a')
    assert eq(d.groupby('a').b.mean(), e.groupby(e.index).b.mean())

    def func(df):
        df['b'] = df.b - df.b.mean()
        return df

    assert eq(d.groupby('a').apply(func),
              e.groupby(e.index).apply(func))


def test_set_partition():
    d2 = d.set_partition('b', [2])
    assert d2.blockdivs == (2,)
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


def test_from_bcolz():
    try:
        import bcolz
    except ImportError:
        pass
    else:
        t = bcolz.ctable([[1, 2, 3], [1., 2., 3.], ['a', 'b', 'a']],
                         names=['x', 'y', 'a'])
        d = dd.from_bcolz(t, chunksize=2)
        assert d.npartitions == 2
        assert str(d.dtypes['a']) == 'category'
        assert list(d.x.compute(get=dask.get)) == [1, 2, 3]
        assert list(d.a.compute(get=dask.get)) == ['a', 'b', 'a']

        d = dd.from_bcolz(t, chunksize=2, index='x')
        assert list(d.index.compute()) == [1, 2, 3]


def test_loc():
    assert eq(d.loc[5], full.loc[5])
    assert eq(d.loc[3:8], full.loc[3:8])
    assert eq(d.loc[:8], full.loc[:8])
    assert eq(d.loc[3:], full.loc[3:])


def test_iloc_raises():
    assert raises(AttributeError, lambda: d.iloc[:5])


#####################
# Play with PFrames #
#####################


dfs = list(dsk.values())
pf = pframe(like=dfs[0], blockdivs=[5])
for df in dfs:
    pf.append(df)


def test_from_pframe():
    d = dd.from_pframe(pf)
    assert list(d.columns) == list(dfs[0].columns)
    assert list(d.blockdivs) == list(pf.blockdivs)


def test_column_optimizations_with_pframe_and_rewrite():
    dsk2 = dict((('x', i), (getitem,
                             (pframe.get_partition, pf, i),
                             (list, ['a', 'b'])))
            for i in [1, 2, 3])

    expected = dict((('x', i),
                     (pframe.get_partition, pf, i, (list, ['a', 'b'])))
            for i in [1, 2, 3])
    result = valmap(rewrite_rules.rewrite, dsk2)

    assert result == expected


def test_column_optimizations_with_bcolz_and_rewrite():
    bc = bcolz.ctable([[1, 2, 3], [10, 20, 30]], names=['a', 'b'])
    func = lambda x: x
    for cols in [None, 'abc', ['abc']]:
        dsk2 = dict((('x', i),
                     (func,
                       (getitem,
                         (dataframe_from_ctable, bc, slice(0, 2), cols, {}),
                         (list, ['a', 'b']))))
                for i in [1, 2, 3])

        expected = dict((('x', i), (func, (dataframe_from_ctable,
                                     bc, slice(0, 2), (list, ['a', 'b']), {})))
                for i in [1, 2, 3])
        result = valmap(rewrite_rules.rewrite, dsk2)

        assert result == expected


def test_column_store_from_pframe():
    d = dd.from_pframe(pf)
    assert eq(d[['a']].head(), pd.DataFrame({'a': [1, 2, 3]}, index=[0, 1, 3]))
    assert eq(d.a.head(), pd.Series([1, 2, 3], index=[0, 1, 3], name='a'))


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

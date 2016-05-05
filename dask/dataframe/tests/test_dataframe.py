from operator import getitem
from distutils.version import LooseVersion

import pandas as pd
import pandas.util.testing as tm
import numpy as np
import pytest

import dask
from dask.async import get_sync
from dask.utils import raises, ignoring
import dask.dataframe as dd

from dask.dataframe.core import (repartition_divisions, _loc,
        _coerce_loc_index, aca, reduction, _concat, _Frame)
from dask.dataframe.utils import eq


dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 5, 9, 9])
full = d.compute()


def test_Dataframe():
    expected = pd.Series([2, 3, 4, 5, 6, 7, 8, 9, 10],
                         index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
                         name='a')

    assert eq(d['a'] + 1, expected)

    tm.assert_index_equal(d.columns, pd.Index(['a', 'b']))

    assert eq(d[d['b'] > 2], full[full['b'] > 2])
    assert eq(d[['a', 'b']], full[['a', 'b']])
    assert eq(d.a, full.a)
    assert d.b.mean().compute() == full.b.mean()
    assert np.allclose(d.b.var().compute(), full.b.var())
    assert np.allclose(d.b.std().compute(), full.b.std())

    assert d.index._name == d.index._name  # this is deterministic

    assert repr(d)


def test_head_tail():
    assert eq(d.head(2), full.head(2))
    assert eq(d.head(3), full.head(3))
    assert eq(d.head(2), dsk[('x', 0)].head(2))
    assert eq(d['a'].head(2), full['a'].head(2))
    assert eq(d['a'].head(3), full['a'].head(3))
    assert eq(d['a'].head(2), dsk[('x', 0)]['a'].head(2))
    assert (sorted(d.head(2, compute=False).dask) ==
            sorted(d.head(2, compute=False).dask))
    assert (sorted(d.head(2, compute=False).dask) !=
            sorted(d.head(3, compute=False).dask))

    assert eq(d.tail(2), full.tail(2))
    assert eq(d.tail(3), full.tail(3))
    assert eq(d.tail(2), dsk[('x', 2)].tail(2))
    assert eq(d['a'].tail(2), full['a'].tail(2))
    assert eq(d['a'].tail(3), full['a'].tail(3))
    assert eq(d['a'].tail(2), dsk[('x', 2)]['a'].tail(2))
    assert (sorted(d.tail(2, compute=False).dask) ==
            sorted(d.tail(2, compute=False).dask))
    assert (sorted(d.tail(2, compute=False).dask) !=
            sorted(d.tail(3, compute=False).dask))


def test_index_head():
    assert eq(d.index.head(2), full.index[:2])
    assert eq(d.index.head(3), full.index[:3])


def test_Series():
    assert isinstance(d.a, dd.Series)
    assert isinstance(d.a + 1, dd.Series)
    assert eq((d + 1), full + 1)
    assert repr(d.a).startswith('dd.Series')


def test_repr():
    df = pd.DataFrame({'x': list(range(100))})
    ddf = dd.from_pandas(df, 3)

    for x in [ddf, ddf.index, ddf.x]:
        assert type(x).__name__ in repr(x)
        assert x._name[:5] in repr(x)
        assert str(x.npartitions) in repr(x)
        assert len(repr(x)) < 80


def test_Index():
    for case in [pd.DataFrame(np.random.randn(10, 5), index=list('abcdefghij')),
                 pd.DataFrame(np.random.randn(10, 5),
                    index=pd.date_range('2011-01-01', freq='D', periods=10))]:
        ddf = dd.from_pandas(case, 3)
        assert eq(ddf.index, case.index)
        assert repr(ddf.index).startswith('dd.Index')
        assert raises(AttributeError, lambda: ddf.index.index)


def test_attributes():
    assert 'a' in dir(d)
    assert 'foo' not in dir(d)
    assert raises(AttributeError, lambda: d.foo)


def test_column_names():
    tm.assert_index_equal(d.columns, pd.Index(['a', 'b']))
    tm.assert_index_equal(d[['b', 'a']].columns, pd.Index(['b', 'a']))
    assert d['a'].name == 'a'
    assert (d['a'] + 1).name == 'a'
    assert (d['a'] + d['b']).name is None


def test_index_names():
    assert d.index.name is None

    idx = pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name='x')
    df = pd.DataFrame(np.random.randn(10, 5), idx)
    ddf = dd.from_pandas(df, 3)
    assert ddf.index.name == 'x'
    assert ddf.index.compute().name == 'x'


def test_set_index():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 2, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 5, 8]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [9, 1, 8]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    full = d.compute()

    d2 = d.set_index('b', npartitions=3)
    assert d2.npartitions == 3
    assert d2.index.name == 'b'
    assert eq(d2, full.set_index('b'))

    d3 = d.set_index(d.b, npartitions=3)
    assert d3.npartitions == 3
    assert d3.index.name == 'b'
    assert eq(d3, full.set_index(full.b))

    d4 = d.set_index('b')
    assert d4.index.name == 'b'
    assert eq(d4, full.set_index('b'))


@pytest.mark.parametrize('drop', [True, False])
def test_set_index_drop(drop):

    pdf = pd.DataFrame({'A': list('ABAABBABAA'),
                        'B': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        'C': [1, 2, 3, 2, 1, 3, 2, 4, 2, 3]})
    ddf = dd.from_pandas(pdf, 3)

    assert eq(ddf.set_index('A', drop=drop),
              pdf.set_index('A', drop=drop))
    assert eq(ddf.set_index('B', drop=drop),
              pdf.set_index('B', drop=drop))
    assert eq(ddf.set_index('C', drop=drop),
              pdf.set_index('C', drop=drop))
    assert eq(ddf.set_index(ddf.A, drop=drop),
              pdf.set_index(pdf.A, drop=drop))
    assert eq(ddf.set_index(ddf.B, drop=drop),
              pdf.set_index(pdf.B, drop=drop))
    assert eq(ddf.set_index(ddf.C, drop=drop),
              pdf.set_index(pdf.C, drop=drop))

    # numeric columns
    pdf = pd.DataFrame({0: list('ABAABBABAA'),
                        1: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        2: [1, 2, 3, 2, 1, 3, 2, 4, 2, 3]})
    ddf = dd.from_pandas(pdf, 3)
    assert eq(ddf.set_index(0, drop=drop),
              pdf.set_index(0, drop=drop))
    assert eq(ddf.set_index(2, drop=drop),
              pdf.set_index(2, drop=drop))


def test_set_index_raises_error_on_bad_input():
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                       'b': [7, 6, 5, 4, 3, 2, 1]})
    ddf = dd.from_pandas(df, 2)

    msg = r"Dask dataframe does not yet support multi-indexes"
    with tm.assertRaisesRegexp(NotImplementedError, msg):
        ddf.set_index(['a', 'b'])


def test_rename_columns():
    # GH 819
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                       'b': [7, 6, 5, 4, 3, 2, 1]})
    ddf = dd.from_pandas(df, 2)

    ddf.columns = ['x', 'y']
    df.columns = ['x', 'y']
    tm.assert_index_equal(ddf.columns, pd.Index(['x', 'y']))
    tm.assert_index_equal(ddf._pd.columns, pd.Index(['x', 'y']))
    assert eq(ddf, df)

    msg = r"Length mismatch: Expected axis has 2 elements, new values have 4 elements"
    with tm.assertRaisesRegexp(ValueError, msg):
        ddf.columns = [1, 2, 3, 4]


def test_rename_series():
    # GH 819
    s = pd.Series([1, 2, 3, 4, 5, 6, 7], name='x')
    ds = dd.from_pandas(s, 2)

    s.name = 'renamed'
    ds.name = 'renamed'
    assert s.name == 'renamed'
    assert eq(ds, s)


def test_describe():
    # prepare test case which approx quantiles will be the same as actuals
    s = pd.Series(list(range(20)) * 4)
    df = pd.DataFrame({'a': list(range(20)) * 4, 'b': list(range(4)) * 20})

    ds = dd.from_pandas(s, 4)
    ddf = dd.from_pandas(df, 4)

    assert eq(s.describe(), ds.describe())
    assert eq(df.describe(), ddf.describe())

    # remove string columns
    df = pd.DataFrame({'a': list(range(20)) * 4, 'b': list(range(4)) * 20,
                       'c': list('abcd') * 20})
    ddf = dd.from_pandas(df, 4)
    assert eq(df.describe(), ddf.describe())


def test_cumulative():
    pdf = pd.DataFrame(np.random.randn(100, 5), columns=list('abcde'))
    ddf = dd.from_pandas(pdf, 5)

    assert eq(ddf.cumsum(), pdf.cumsum())
    assert eq(ddf.cumprod(), pdf.cumprod())
    assert eq(ddf.cummin(), pdf.cummin())
    assert eq(ddf.cummax(), pdf.cummax())

    assert eq(ddf.cumsum(axis=1), pdf.cumsum(axis=1))
    assert eq(ddf.cumprod(axis=1), pdf.cumprod(axis=1))
    assert eq(ddf.cummin(axis=1), pdf.cummin(axis=1))
    assert eq(ddf.cummax(axis=1), pdf.cummax(axis=1))

    assert eq(ddf.a.cumsum(), pdf.a.cumsum())
    assert eq(ddf.a.cumprod(), pdf.a.cumprod())
    assert eq(ddf.a.cummin(), pdf.a.cummin())
    assert eq(ddf.a.cummax(), pdf.a.cummax())


def test_dropna():
    df = pd.DataFrame({'x': [np.nan, 2,      3, 4, np.nan,      6],
                       'y': [1,      2, np.nan, 4, np.nan, np.nan],
                       'z': [1,      2,      3, 4, np.nan, np.nan]},
                      index=[10, 20, 30, 40, 50, 60])
    ddf = dd.from_pandas(df, 3)

    assert eq(ddf.x.dropna(), df.x.dropna())
    assert eq(ddf.y.dropna(), df.y.dropna())
    assert eq(ddf.z.dropna(), df.z.dropna())

    assert eq(ddf.dropna(), df.dropna())
    assert eq(ddf.dropna(how='all'), df.dropna(how='all'))
    assert eq(ddf.dropna(subset=['x']), df.dropna(subset=['x']))
    assert eq(ddf.dropna(subset=['y', 'z']), df.dropna(subset=['y', 'z']))
    assert eq(ddf.dropna(subset=['y', 'z'], how='all'),
              df.dropna(subset=['y', 'z'], how='all'))


def test_where_mask():
    pdf1 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                         'b': [3, 5, 2, 5, 7, 2, 4, 2, 4]})
    ddf1 = dd.from_pandas(pdf1, 2)
    pdf2 = pd.DataFrame({'a': [True, False, True] * 3,
                         'b': [False, False, True] * 3})
    ddf2 = dd.from_pandas(pdf2, 2)

    # different index
    pdf3 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                         'b': [3, 5, 2, 5, 7, 2, 4, 2, 4]},
                        index=[0, 1, 2, 3, 4, 5, 6, 7, 8])
    ddf3 = dd.from_pandas(pdf3, 2)
    pdf4 = pd.DataFrame({'a': [True, False, True] * 3,
                         'b': [False, False, True] * 3},
                        index=[5, 6, 7, 8, 9, 10, 11, 12, 13])
    ddf4 = dd.from_pandas(pdf4, 2)

    # different columns
    pdf5 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                         'b': [9, 4, 2, 6, 2, 3, 1, 6, 2],
                         'c': [5, 6, 7, 8, 9, 10, 11, 12, 13]},
                        index=[0, 1, 2, 3, 4, 5, 6, 7, 8])
    ddf5 = dd.from_pandas(pdf5, 2)
    pdf6 = pd.DataFrame({'a': [True, False, True] * 3,
                         'b': [False, False, True] * 3,
                         'd': [False] * 9,
                         'e': [True] * 9},
                        index=[5, 6, 7, 8, 9, 10, 11, 12, 13])
    ddf6 = dd.from_pandas(pdf6, 2)

    cases = [(ddf1, ddf2, pdf1, pdf2),
             (ddf1.repartition([0, 3, 6, 8]), ddf2, pdf1, pdf2),
             (ddf1, ddf4, pdf3, pdf4),
             (ddf3.repartition([0, 4, 6, 8]), ddf4.repartition([5, 9, 10, 13]),
              pdf3, pdf4),
             (ddf5, ddf6, pdf5, pdf6),
             (ddf5.repartition([0, 4, 7, 8]), ddf6, pdf5, pdf6),

             # use pd.DataFrame as cond
             (ddf1, pdf2, pdf1, pdf2),
             (ddf1, pdf4, pdf3, pdf4),
             (ddf5, pdf6, pdf5, pdf6)]

    for ddf, ddcond, pdf, pdcond in cases:
        assert isinstance(ddf, dd.DataFrame)
        assert isinstance(ddcond, (dd.DataFrame, pd.DataFrame))
        assert isinstance(pdf, pd.DataFrame)
        assert isinstance(pdcond, pd.DataFrame)

        assert eq(ddf.where(ddcond), pdf.where(pdcond))
        assert eq(ddf.mask(ddcond), pdf.mask(pdcond))
        assert eq(ddf.where(ddcond, -ddf), pdf.where(pdcond, -pdf))
        assert eq(ddf.mask(ddcond, -ddf), pdf.mask(pdcond, -pdf))

        # ToDo: Should work on pandas 0.17
        # https://github.com/pydata/pandas/pull/10283
        # assert eq(ddf.where(ddcond.a, -ddf), pdf.where(pdcond.a, -pdf))
        # assert eq(ddf.mask(ddcond.a, -ddf), pdf.mask(pdcond.a, -pdf))

        assert eq(ddf.a.where(ddcond.a), pdf.a.where(pdcond.a))
        assert eq(ddf.a.mask(ddcond.a), pdf.a.mask(pdcond.a))
        assert eq(ddf.a.where(ddcond.a, -ddf.a), pdf.a.where(pdcond.a, -pdf.a))
        assert eq(ddf.a.mask(ddcond.a, -ddf.a), pdf.a.mask(pdcond.a, -pdf.a))


def test_map_partitions_multi_argument():
    assert eq(dd.map_partitions(lambda a, b: a + b, None, d.a, d.b),
              full.a + full.b)
    assert eq(dd.map_partitions(lambda a, b, c: a + b + c, None, d.a, d.b, 1),
              full.a + full.b + 1)


def test_map_partitions():
    assert eq(d.map_partitions(lambda df: df, columns=d.columns), full)
    assert eq(d.map_partitions(lambda df: df), full)
    result = d.map_partitions(lambda df: df.sum(axis=1), columns=None)
    assert eq(result, full.sum(axis=1))


def test_map_partitions_names():
    func = lambda x: x
    assert sorted(dd.map_partitions(func, d.columns, d).dask) == \
           sorted(dd.map_partitions(func, d.columns, d).dask)
    assert sorted(dd.map_partitions(lambda x: x, d.columns, d, token=1).dask) == \
           sorted(dd.map_partitions(lambda x: x, d.columns, d, token=1).dask)

    func = lambda x, y: x
    assert sorted(dd.map_partitions(func, d.columns, d, d).dask) == \
           sorted(dd.map_partitions(func, d.columns, d, d).dask)


def test_map_partitions_column_info():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    b = dd.map_partitions(lambda x: x, a.columns, a)
    tm.assert_index_equal(b.columns, a.columns)
    assert eq(df, b)

    b = dd.map_partitions(lambda x: x, a.x.name, a.x)
    assert b.name == a.x.name
    assert eq(df.x, b)

    b = dd.map_partitions(lambda x: x, a.x.name, a.x)
    assert b.name == a.x.name
    assert eq(df.x, b)

    b = dd.map_partitions(lambda df: df.x + df.y, None, a)
    assert b.name is None
    assert isinstance(b, dd.Series)

    b = dd.map_partitions(lambda df: df.x + 1, 'x', a)
    assert isinstance(b, dd.Series)
    assert b.name == 'x'


def test_map_partitions_method_names():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    b = a.map_partitions(lambda x: x)
    assert isinstance(b, dd.DataFrame)
    tm.assert_index_equal(b.columns, a.columns)

    b = a.map_partitions(lambda df: df.x + 1, columns=None)
    assert isinstance(b, dd.Series)
    assert b.name is None

    b = a.map_partitions(lambda df: df.x + 1, columns='x')
    assert isinstance(b, dd.Series)
    assert b.name == 'x'


def test_map_partitions_keeps_kwargs_in_dict():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    def f(s, x=1):
        return s + x

    b = a.x.map_partitions(f, x=5)

    assert "'x': 5" in str(b.dask)
    eq(df.x + 5, b)

    assert a.x.map_partitions(f, x=5)._name != a.x.map_partitions(f, x=6)._name


def test_drop_duplicates():
    # can't detect duplicates only from cached data
    assert eq(d.a.drop_duplicates(), full.a.drop_duplicates())
    assert eq(d.drop_duplicates(), full.drop_duplicates())
    assert eq(d.index.drop_duplicates(), full.index.drop_duplicates())


def test_drop_duplicates_subset():
    df = pd.DataFrame({'x': [1, 2, 3, 1, 2, 3],
                       'y': ['a', 'a', 'b', 'b', 'c', 'c']})
    ddf = dd.from_pandas(df, npartitions=2)

    if pd.__version__ < '0.17':
        kwargs = [{'take_last': False}, {'take_last': True}]
    else:
        kwargs = [{'keep': 'first'}, {'keep': 'last'}]

    for kwarg in kwargs:
        assert eq(df.x.drop_duplicates(**kwarg),
                  ddf.x.drop_duplicates(**kwarg))
        for ss in [['x'], 'y', ['x', 'y']]:
            assert eq(df.drop_duplicates(subset=ss, **kwarg),
                      ddf.drop_duplicates(subset=ss, **kwarg))


def test_set_partition():
    d2 = d.set_partition('b', [0, 2, 9])
    assert d2.divisions == (0, 2, 9)
    expected = full.set_index('b')
    assert eq(d2, expected)


def test_set_partition_compute():
    d2 = d.set_partition('b', [0, 2, 9])
    d3 = d.set_partition('b', [0, 2, 9], compute=True)

    assert eq(d2, d3)
    assert eq(d2, full.set_index('b'))
    assert eq(d3, full.set_index('b'))
    assert len(d2.dask) > len(d3.dask)

    d4 = d.set_partition(d.b, [0, 2, 9])
    d5 = d.set_partition(d.b, [0, 2, 9], compute=True)
    exp = full.copy()
    exp.index = exp.b
    assert eq(d4, d5)
    assert eq(d4, exp)
    assert eq(d5, exp)
    assert len(d4.dask) > len(d5.dask)


def test_get_division():
    pdf = pd.DataFrame(np.random.randn(10, 5), columns=list('abcde'))
    ddf = dd.from_pandas(pdf, 3)
    assert ddf.divisions == (0, 4, 8, 9)

    # DataFrame
    div1 = ddf.get_division(0)
    assert isinstance(div1, dd.DataFrame)
    assert eq(div1, pdf.loc[0:3])
    div2 = ddf.get_division(1)
    assert eq(div2, pdf.loc[4:7])
    div3 = ddf.get_division(2)
    assert eq(div3, pdf.loc[8:9])
    assert len(div1) + len(div2) + len(div3) == len(pdf)

    # Series
    div1 = ddf.a.get_division(0)
    assert isinstance(div1, dd.Series)
    assert eq(div1, pdf.a.loc[0:3])
    div2 = ddf.a.get_division(1)
    assert eq(div2, pdf.a.loc[4:7])
    div3 = ddf.a.get_division(2)
    assert eq(div3, pdf.a.loc[8:9])
    assert len(div1) + len(div2) + len(div3) == len(pdf.a)

    with tm.assertRaises(ValueError):
        ddf.get_division(-1)

    with tm.assertRaises(ValueError):
        ddf.get_division(3)


def test_ndim():
    assert (d.ndim == 2)
    assert (d.a.ndim == 1)
    assert (d.index.ndim == 1)


def test_dtype():
    assert (d.dtypes == full.dtypes).all()


def test_cache():
    d2 = d.cache()
    assert all(task[0] == getitem for task in d2.dask.values())

    assert eq(d2.a, d.a)


def test_value_counts():
    df = pd.DataFrame({'x': [1, 2, 1, 3, 3, 1, 4]})
    a = dd.from_pandas(df, npartitions=3)
    result = a.x.value_counts()
    expected = df.x.value_counts()
    # because of pandas bug, value_counts doesn't hold name (fixed in 0.17)
    # https://github.com/pydata/pandas/pull/10419
    assert eq(result, expected, check_names=False)


def test_unique():
    pdf = pd.DataFrame({'x': [1, 2, 1, 3, 3, 1, 4, 2, 3, 1],
                        'y': ['a', 'c', 'b', np.nan, 'c',
                              'b', 'a', 'd', np.nan, 'a']})
    ddf = dd.from_pandas(pdf, npartitions=3)
    assert eq(ddf.x.unique(), pd.Series(pdf.x.unique(), name='x'))
    assert eq(ddf.y.unique(), pd.Series(pdf.y.unique(), name='y'))


def test_isin():
    assert eq(d.a.isin([0, 1, 2]), full.a.isin([0, 1, 2]))
    assert eq(d.a.isin(pd.Series([0, 1, 2])),
              full.a.isin(pd.Series([0, 1, 2])))


def test_len():
    assert len(d) == len(full)
    assert len(d.a) == len(full.a)


def test_quantile():
    # series / multiple
    result = d.b.quantile([.3, .7])
    exp = full.b.quantile([.3, .7])  # result may different
    assert len(result) == 2
    assert result.divisions == (.3, .7)
    assert eq(result.index, exp.index)
    assert isinstance(result, dd.Series)

    result = result.compute()
    assert isinstance(result, pd.Series)
    assert result.iloc[0] == 0
    assert 5 < result.iloc[1] < 6

    # index
    s = pd.Series(np.arange(10), index=np.arange(10))
    ds = dd.from_pandas(s, 2)

    result = ds.index.quantile([.3, .7])
    exp = s.quantile([.3, .7])
    assert len(result) == 2
    assert result.divisions == (.3, .7)
    assert eq(result.index, exp.index)
    assert isinstance(result, dd.Series)

    result = result.compute()
    assert isinstance(result, pd.Series)
    assert 1 < result.iloc[0] < 2
    assert 7 < result.iloc[1] < 8

    # series / single
    result = d.b.quantile(.5)
    exp = full.b.quantile(.5)  # result may different
    assert isinstance(result, dd.core.Scalar)
    result = result.compute()
    assert 4 < result < 6


def test_empty_quantile():
    result = d.b.quantile([])
    exp = full.b.quantile([])
    assert result.divisions == (None, None)

    # because of a pandas bug, name is not preserved
    # https://github.com/pydata/pandas/pull/10881
    assert result.name == 'b'
    assert result.compute().name == 'b'
    assert eq(result, exp, check_names=False)


def test_dataframe_quantile():

    # column X is for test column order and result division
    df = pd.DataFrame({'A': np.arange(20),
                       'X': np.arange(20, 40),
                       'B': np.arange(10, 30),
                       'C': ['a', 'b', 'c', 'd'] * 5},
                      columns=['A', 'X', 'B', 'C'])
    ddf = dd.from_pandas(df, 3)

    result = ddf.quantile()
    assert result.npartitions == 1
    assert result.divisions == ('A', 'X')

    result = result.compute()
    assert isinstance(result, pd.Series)
    tm.assert_index_equal(result.index, pd.Index(['A', 'X', 'B']))
    assert (result > pd.Series([16, 36, 26], index=['A', 'X', 'B'])).all()
    assert (result < pd.Series([17, 37, 27], index=['A', 'X', 'B'])).all()

    result = ddf.quantile([0.25, 0.75])
    assert result.npartitions == 1
    assert result.divisions == (0.25, 0.75)

    result = result.compute()
    assert isinstance(result, pd.DataFrame)
    tm.assert_index_equal(result.index, pd.Index([0.25, 0.75]))
    tm.assert_index_equal(result.columns, pd.Index(['A', 'X', 'B']))
    minexp = pd.DataFrame([[1, 21, 11], [17, 37, 27]],
                          index=[0.25, 0.75], columns=['A', 'X', 'B'])
    assert (result > minexp).all().all()
    maxexp = pd.DataFrame([[2, 22, 12], [18, 38, 28]],
                          index=[0.25, 0.75], columns=['A', 'X', 'B'])
    assert (result < maxexp).all().all()

    assert eq(ddf.quantile(axis=1), df.quantile(axis=1))
    assert raises(ValueError, lambda: ddf.quantile([0.25, 0.75], axis=1))


def test_index():
    assert eq(d.index, full.index)


def test_loc():
    assert d.loc[3:8].divisions[0] == 3
    assert d.loc[3:8].divisions[-1] == 8

    assert d.loc[5].divisions == (5, 5)

    assert eq(d.loc[5], full.loc[5:5])
    assert eq(d.loc[3:8], full.loc[3:8])
    assert eq(d.loc[:8], full.loc[:8])
    assert eq(d.loc[3:], full.loc[3:])

    assert eq(d.a.loc[5], full.a.loc[5:5])
    assert eq(d.a.loc[3:8], full.a.loc[3:8])
    assert eq(d.a.loc[:8], full.a.loc[:8])
    assert eq(d.a.loc[3:], full.a.loc[3:])

    assert raises(KeyError, lambda: d.loc[1000])
    assert eq(d.loc[1000:], full.loc[1000:])
    assert eq(d.loc[-2000:-1000], full.loc[-2000:-1000])

    assert sorted(d.loc[5].dask) == sorted(d.loc[5].dask)
    assert sorted(d.loc[5].dask) != sorted(d.loc[6].dask)


def test_loc_non_informative_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4]}, index=[10, 20, 30, 40])
    ddf = dd.from_pandas(df, npartitions=2, sort=True)
    ddf.divisions = (None,) * 3
    assert not ddf.known_divisions

    ddf.loc[20:30].compute(get=dask.get)

    assert eq(ddf.loc[20:30], df.loc[20:30])

    df = pd.DataFrame({'x': [1, 2, 3, 4]}, index=[10, 20, 20, 40])
    ddf = dd.from_pandas(df, npartitions=2, sort=True)
    assert eq(ddf.loc[20], df.loc[20:20])


def test_loc_with_text_dates():
    A = tm.makeTimeSeries(10).iloc[:5]
    B = tm.makeTimeSeries(10).iloc[5:]
    s = dd.Series({('df', 0): A, ('df', 1): B}, 'df', None,
                  [A.index.min(), B.index.min(), B.index.max()])

    assert s.loc['2000': '2010'].divisions == s.divisions
    assert eq(s.loc['2000': '2010'], s)
    assert len(s.loc['2000-01-03': '2000-01-05'].compute()) == 3


def test_loc_with_series():
    assert eq(d.loc[d.a % 2 == 0], full.loc[full.a % 2 == 0])

    assert sorted(d.loc[d.a % 2].dask) == sorted(d.loc[d.a % 2].dask)
    assert sorted(d.loc[d.a % 2].dask) != sorted(d.loc[d.a % 3].dask)


def test_iloc_raises():
    assert raises(NotImplementedError, lambda: d.iloc[:5])


def test_getitem():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                       'B': [9, 8, 7, 6, 5, 4, 3, 2, 1],
                       'C': [True, False, True] * 3},
                      columns=list('ABC'))
    ddf = dd.from_pandas(df, 2)
    assert eq(ddf['A'], df['A'])
    tm.assert_series_equal(ddf['A']._pd, ddf._pd['A'])# check cache consistency

    assert eq(ddf[['A', 'B']], df[['A', 'B']])
    tm.assert_frame_equal(ddf[['A', 'B']]._pd, ddf._pd[['A', 'B']])

    assert eq(ddf[ddf.C], df[df.C])
    tm.assert_series_equal(ddf.C._pd, ddf._pd.C)

    assert eq(ddf[ddf.C.repartition([0, 2, 5, 8])], df[df.C])

    assert raises(KeyError, lambda: df['X'])
    assert raises(KeyError, lambda: df[['A', 'X']])
    assert raises(AttributeError, lambda: df.X)

    # not str/unicode
    df = pd.DataFrame(np.random.randn(10, 5))
    ddf = dd.from_pandas(df, 2)
    assert eq(ddf[0], df[0])
    assert eq(ddf[[1, 2]], df[[1, 2]])

    assert raises(KeyError, lambda: df[8])
    assert raises(KeyError, lambda: df[[1, 8]])


def test_assign():
    assert eq(d.assign(c=d.a + 1, e=d.a + d.b),
              full.assign(c=full.a + 1, e=full.a + full.b))


def test_map():
    assert eq(d.a.map(lambda x: x + 1), full.a.map(lambda x: x + 1))
    lk = dict((v, v + 1) for v in full.a.values)
    assert eq(d.a.map(lk), full.a.map(lk))
    assert eq(d.b.map(lk), full.b.map(lk))
    lk = pd.Series(lk)
    assert eq(d.a.map(lk), full.a.map(lk))
    assert eq(d.b.map(lk), full.b.map(lk))
    assert raises(TypeError, lambda: d.a.map(d.b))


def test_concat():
    x = _concat([pd.DataFrame(columns=['a', 'b']),
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
                      ['a', 'b'], divisions=[None, None, None])
    assert not df.known_divisions

    df = dd.DataFrame({('x', 0): 'foo'}, 'x',
                      ['a', 'b'], divisions=[0, 1])
    assert d.known_divisions


def test_unknown_divisions():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]}),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]})}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [None, None, None, None])
    full = d.compute(get=dask.get)

    assert eq(d.a.sum(), full.a.sum())
    assert eq(d.a + d.b + 1, full.a + full.b + 1)


def test_concat2():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]}),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]})}
    a = dd.DataFrame(dsk, 'x', ['a', 'b'], [None, None])
    dsk = {('y', 0): pd.DataFrame({'a': [10, 20, 30], 'b': [40, 50, 60]}),
           ('y', 1): pd.DataFrame({'a': [40, 50, 60], 'b': [30, 20, 10]}),
           ('y', 2): pd.DataFrame({'a': [70, 80, 90], 'b': [0, 0, 0]})}
    b = dd.DataFrame(dsk, 'y', ['a', 'b'], [None, None])

    dsk = {('y', 0): pd.DataFrame({'b': [10, 20, 30], 'c': [40, 50, 60]}),
           ('y', 1): pd.DataFrame({'b': [40, 50, 60], 'c': [30, 20, 10]})}
    c = dd.DataFrame(dsk, 'y', ['b', 'c'], [None, None])

    dsk = {('y', 0): pd.DataFrame({'b': [10, 20, 30], 'c': [40, 50, 60],
                                   'd': [70, 80, 90]}),
           ('y', 1): pd.DataFrame({'b': [40, 50, 60], 'c': [30, 20, 10],
                                   'd': [90, 80, 70]},
                                  index=[3, 4, 5])}
    d = dd.DataFrame(dsk, 'y', ['b', 'c', 'd'], [0, 3, 5])

    cases = [[a, b], [a, c], [a, d]]
    assert dd.concat([a]) is a
    for case in cases:
        result = dd.concat(case)
        pdcase = [c.compute() for c in case]

        assert result.npartitions == case[0].npartitions + case[1].npartitions
        assert result.divisions == (None, ) * (result.npartitions + 1)
        assert eq(pd.concat(pdcase), result)
        assert result.dask == dd.concat(case).dask

        result = dd.concat(case, join='inner')
        assert result.npartitions == case[0].npartitions + case[1].npartitions
        assert result.divisions == (None, ) * (result.npartitions + 1)
        assert eq(pd.concat(pdcase, join='inner'), result)
        assert result.dask == dd.concat(case, join='inner').dask

        msg = ('Unable to concatenate DataFrame with unknown division '
               'specifying axis=1')
        with tm.assertRaisesRegexp(ValueError, msg):
            dd.concat(case, axis=1)


def test_concat3():
    pdf1 = pd.DataFrame(np.random.randn(6, 5),
                        columns=list('ABCDE'), index=list('abcdef'))
    pdf2 = pd.DataFrame(np.random.randn(6, 5),
                        columns=list('ABCFG'), index=list('ghijkl'))
    pdf3 = pd.DataFrame(np.random.randn(6, 5),
                        columns=list('ABCHI'), index=list('mnopqr'))
    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)

    result = dd.concat([ddf1, ddf2])
    assert result.divisions == ddf1.divisions[:-1] + ddf2.divisions
    assert result.npartitions == ddf1.npartitions + ddf2.npartitions
    assert eq(result, pd.concat([pdf1, pdf2]))

    assert eq(dd.concat([ddf1, ddf2], interleave_partitions=True),
              pd.concat([pdf1, pdf2]))

    result = dd.concat([ddf1, ddf2, ddf3])
    assert result.divisions == (ddf1.divisions[:-1] + ddf2.divisions[:-1] +
                                ddf3.divisions)
    assert result.npartitions == (ddf1.npartitions + ddf2.npartitions +
                                  ddf3.npartitions)
    assert eq(result, pd.concat([pdf1, pdf2, pdf3]))

    assert eq(dd.concat([ddf1, ddf2, ddf3], interleave_partitions=True),
              pd.concat([pdf1, pdf2, pdf3]))


def test_concat4_interleave_partitions():
    pdf1 = pd.DataFrame(np.random.randn(10, 5),
                        columns=list('ABCDE'), index=list('abcdefghij'))
    pdf2 = pd.DataFrame(np.random.randn(13, 5),
                        columns=list('ABCDE'), index=list('fghijklmnopqr'))
    pdf3 = pd.DataFrame(np.random.randn(13, 6),
                        columns=list('CDEXYZ'), index=list('fghijklmnopqr'))

    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)

    msg = ('All inputs have known divisions which cannnot be '
           'concatenated in order. Specify '
           'interleave_partitions=True to ignore order')

    cases = [[ddf1, ddf1], [ddf1, ddf2], [ddf1, ddf3], [ddf2, ddf1],
             [ddf2, ddf3], [ddf3, ddf1], [ddf3, ddf2]]
    for case in cases:
        pdcase = [c.compute() for c in case]

        with tm.assertRaisesRegexp(ValueError, msg):
            dd.concat(case)

        assert eq(dd.concat(case, interleave_partitions=True),
                  pd.concat(pdcase))
        assert eq(dd.concat(case, join='inner', interleave_partitions=True),
                  pd.concat(pdcase, join='inner'))

    msg = "'join' must be 'inner' or 'outer'"
    with tm.assertRaisesRegexp(ValueError, msg):
        dd.concat([ddf1, ddf1], join='invalid', interleave_partitions=True)


def test_concat5():
    pdf1 = pd.DataFrame(np.random.randn(7, 5),
                        columns=list('ABCDE'), index=list('abcdefg'))
    pdf2 = pd.DataFrame(np.random.randn(7, 6),
                        columns=list('FGHIJK'), index=list('abcdefg'))
    pdf3 = pd.DataFrame(np.random.randn(7, 6),
                        columns=list('FGHIJK'), index=list('cdefghi'))
    pdf4 = pd.DataFrame(np.random.randn(7, 5),
                        columns=list('FGHAB'), index=list('cdefghi'))
    pdf5 = pd.DataFrame(np.random.randn(7, 5),
                        columns=list('FGHAB'), index=list('fklmnop'))

    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)
    ddf4 = dd.from_pandas(pdf4, 2)
    ddf5 = dd.from_pandas(pdf5, 3)

    cases = [[ddf1, ddf2], [ddf1, ddf3], [ddf1, ddf4], [ddf1, ddf5],
             [ddf3, ddf4], [ddf3, ddf5], [ddf5, ddf1, ddf4], [ddf5, ddf3],
             [ddf1.A, ddf4.A], [ddf2.F, ddf3.F], [ddf4.A, ddf5.A],
             [ddf1.A, ddf4.F], [ddf2.F, ddf3.H], [ddf4.A, ddf5.B],
             [ddf1, ddf4.A], [ddf3.F, ddf2], [ddf5, ddf1.A, ddf2]]

    for case in cases:
        pdcase = [c.compute() for c in case]

        assert eq(dd.concat(case, interleave_partitions=True),
                  pd.concat(pdcase))

        assert eq(dd.concat(case, join='inner', interleave_partitions=True),
                  pd.concat(pdcase, join='inner'))

        assert eq(dd.concat(case, axis=1), pd.concat(pdcase, axis=1))

        assert eq(dd.concat(case, axis=1, join='inner'),
                  pd.concat(pdcase, axis=1, join='inner'))

    # Dask + pandas
    cases = [[ddf1, pdf2], [ddf1, pdf3], [pdf1, ddf4],
             [pdf1.A, ddf4.A], [ddf2.F, pdf3.F],
             [ddf1, pdf4.A], [ddf3.F, pdf2], [ddf2, pdf1, ddf3.F]]

    for case in cases:
        pdcase = [c.compute() if isinstance(c, _Frame) else c for c in case]

        assert eq(dd.concat(case, interleave_partitions=True),
                  pd.concat(pdcase))

        assert eq(dd.concat(case, join='inner', interleave_partitions=True),
                  pd.concat(pdcase, join='inner'))

        assert eq(dd.concat(case, axis=1), pd.concat(pdcase, axis=1))

        assert eq(dd.concat(case, axis=1, join='inner'),
                  pd.concat(pdcase, axis=1, join='inner'))


def test_append():
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
                       'b': [1, 2, 3, 4, 5, 6]})
    df2 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
                        'b': [1, 2, 3, 4, 5, 6]},
                       index=[6, 7, 8, 9, 10, 11])
    df3 = pd.DataFrame({'b': [1, 2, 3, 4, 5, 6],
                        'c': [1, 2, 3, 4, 5, 6]},
                       index=[6, 7, 8, 9, 10, 11])

    ddf = dd.from_pandas(df, 2)
    ddf2 = dd.from_pandas(df2, 2)
    ddf3 = dd.from_pandas(df3, 2)

    s = pd.Series([7, 8], name=6, index=['a', 'b'])
    assert eq(ddf.append(s), df.append(s))

    assert eq(ddf.append(ddf2), df.append(df2))
    assert eq(ddf.a.append(ddf2.a), df.a.append(df2.a))
    # different columns
    assert eq(ddf.append(ddf3), df.append(df3))
    assert eq(ddf.a.append(ddf3.b), df.a.append(df3.b))

    # dask + pandas
    assert eq(ddf.append(df2), df.append(df2))
    assert eq(ddf.a.append(df2.a), df.a.append(df2.a))

    assert eq(ddf.append(df3), df.append(df3))
    assert eq(ddf.a.append(df3.b), df.a.append(df3.b))

    df4 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
                        'b': [1, 2, 3, 4, 5, 6]},
                       index=[4, 5, 6, 7, 8, 9])
    ddf4 = dd.from_pandas(df4, 2)
    msg = ("Unable to append two dataframes to each other with known "
           "divisions if those divisions are not ordered. "
           "The divisions/index of the second dataframe must be "
           "greater than the divisions/index of the first dataframe.")
    with tm.assertRaisesRegexp(ValueError, msg):
        ddf.append(ddf4)


def test_append2():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]}),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]})}
    ddf1 = dd.DataFrame(dsk, 'x', ['a', 'b'], [None, None])

    dsk = {('y', 0): pd.DataFrame({'a': [10, 20, 30], 'b': [40, 50, 60]}),
           ('y', 1): pd.DataFrame({'a': [40, 50, 60], 'b': [30, 20, 10]}),
           ('y', 2): pd.DataFrame({'a': [70, 80, 90], 'b': [0, 0, 0]})}
    ddf2 = dd.DataFrame(dsk, 'y', ['a', 'b'], [None, None])

    dsk = {('y', 0): pd.DataFrame({'b': [10, 20, 30], 'c': [40, 50, 60]}),
           ('y', 1): pd.DataFrame({'b': [40, 50, 60], 'c': [30, 20, 10]})}
    ddf3 = dd.DataFrame(dsk, 'y', ['b', 'c'], [None, None])

    assert eq(ddf1.append(ddf2), ddf1.compute().append(ddf2.compute()))
    assert eq(ddf2.append(ddf1), ddf2.compute().append(ddf1.compute()))
    # Series + DataFrame
    assert eq(ddf1.a.append(ddf2), ddf1.a.compute().append(ddf2.compute()))
    assert eq(ddf2.a.append(ddf1), ddf2.a.compute().append(ddf1.compute()))

    # different columns
    assert eq(ddf1.append(ddf3), ddf1.compute().append(ddf3.compute()))
    assert eq(ddf3.append(ddf1), ddf3.compute().append(ddf1.compute()))
    # Series + DataFrame
    assert eq(ddf1.a.append(ddf3), ddf1.a.compute().append(ddf3.compute()))
    assert eq(ddf3.b.append(ddf1), ddf3.b.compute().append(ddf1.compute()))

    # Dask + pandas
    assert eq(ddf1.append(ddf2.compute()), ddf1.compute().append(ddf2.compute()))
    assert eq(ddf2.append(ddf1.compute()), ddf2.compute().append(ddf1.compute()))
    # Series + DataFrame
    assert eq(ddf1.a.append(ddf2.compute()), ddf1.a.compute().append(ddf2.compute()))
    assert eq(ddf2.a.append(ddf1.compute()), ddf2.a.compute().append(ddf1.compute()))

    # different columns
    assert eq(ddf1.append(ddf3.compute()), ddf1.compute().append(ddf3.compute()))
    assert eq(ddf3.append(ddf1.compute()), ddf3.compute().append(ddf1.compute()))
    # Series + DataFrame
    assert eq(ddf1.a.append(ddf3.compute()), ddf1.a.compute().append(ddf3.compute()))
    assert eq(ddf3.b.append(ddf1.compute()), ddf3.b.compute().append(ddf1.compute()))


def test_dataframe_series_are_pickleable():
    import pickle
    cloudpickle = pytest.importorskip('cloudpickle')

    dumps = cloudpickle.dumps
    loads = pickle.loads

    e = d.groupby(d.a).b.sum()
    f = loads(dumps(e))
    assert eq(e, f)


def test_random_partitions():
    a, b = d.random_split([0.5, 0.5])
    assert isinstance(a, dd.DataFrame)
    assert isinstance(b, dd.DataFrame)

    assert len(a.compute()) + len(b.compute()) == len(full)


def test_series_nunique():
    ps = pd.Series(list('aaabbccccdddeee'), name='a')
    s = dd.from_pandas(ps, npartitions=3)
    assert eq(s.nunique(), ps.nunique())


def test_set_partition_2():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')})
    ddf = dd.from_pandas(df, 2)

    result = ddf.set_partition('y', ['a', 'c', 'd'])
    assert result.divisions == ('a', 'c', 'd')

    assert list(result.compute(get=get_sync).index[-2:]) == ['d', 'd']


def test_repartition():
    def _check_split_data(orig, d):
        """Check data is split properly"""
        keys = [k for k in d.dask if k[0].startswith('repartition-split')]
        keys = sorted(keys)
        sp = pd.concat([d._get(d.dask, k) for k in keys])
        assert eq(orig, sp)
        assert eq(orig, d)

    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    a = dd.from_pandas(df, 2)

    b = a.repartition(divisions=[10, 20, 50, 60])
    assert b.divisions == (10, 20, 50, 60)
    assert eq(a, b)
    assert eq(a._get(b.dask, (b._name, 0)), df.iloc[:1])


    for div in [[20, 60], [10, 50], [1], # first / last element mismatch
                [0, 60], [10, 70], # do not allow to expand divisions by default
                [10, 50, 20, 60],  # not sorted
                [10, 10, 20, 60]]: # not unique (last element can be duplicated)

        assert raises(ValueError, lambda: a.repartition(divisions=div))

    pdf = pd.DataFrame(np.random.randn(7, 5), columns=list('abxyz'))
    for p in range(1, 7):
        ddf = dd.from_pandas(pdf, p)
        assert eq(ddf, pdf)
        for div in [[0, 6], [0, 6, 6], [0, 5, 6], [0, 4, 6, 6],
                    [0, 2, 6], [0, 2, 6, 6],
                    [0, 2, 3, 6, 6], [0, 1, 2, 3, 4, 5, 6, 6]]:
            rddf = ddf.repartition(divisions=div)
            _check_split_data(ddf, rddf)
            assert rddf.divisions == tuple(div)
            assert eq(pdf, rddf)

            rds = ddf.x.repartition(divisions=div)
            _check_split_data(ddf.x, rds)
            assert rds.divisions == tuple(div)
            assert eq(pdf.x, rds)

        # expand divisions
        for div in [[-5, 10], [-2, 3, 5, 6], [0, 4, 5, 9, 10]]:
            rddf = ddf.repartition(divisions=div, force=True)
            _check_split_data(ddf, rddf)
            assert rddf.divisions == tuple(div)
            assert eq(pdf, rddf)

            rds = ddf.x.repartition(divisions=div, force=True)
            _check_split_data(ddf.x, rds)
            assert rds.divisions == tuple(div)
            assert eq(pdf.x, rds)

    pdf = pd.DataFrame({'x': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                        'y': [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]},
                       index=list('abcdefghij'))
    for p in range(1, 7):
        ddf = dd.from_pandas(pdf, p)
        assert eq(ddf, pdf)
        for div in [list('aj'), list('ajj'), list('adj'),
                    list('abfj'), list('ahjj'), list('acdj'), list('adfij'),
                    list('abdefgij'), list('abcdefghij')]:
            rddf = ddf.repartition(divisions=div)
            _check_split_data(ddf, rddf)
            assert rddf.divisions == tuple(div)
            assert eq(pdf, rddf)

            rds = ddf.x.repartition(divisions=div)
            _check_split_data(ddf.x, rds)
            assert rds.divisions == tuple(div)
            assert eq(pdf.x, rds)

        # expand divisions
        for div in [list('Yadijm'), list('acmrxz'), list('Yajz')]:
            rddf = ddf.repartition(divisions=div, force=True)
            _check_split_data(ddf, rddf)
            assert rddf.divisions == tuple(div)
            assert eq(pdf, rddf)

            rds = ddf.x.repartition(divisions=div, force=True)
            _check_split_data(ddf.x, rds)
            assert rds.divisions == tuple(div)
            assert eq(pdf.x, rds)


def test_repartition_divisions():
    result = repartition_divisions([0, 6], [0, 6, 6], 'a', 'b', 'c')
    assert result == {('b', 0): (_loc, ('a', 0), 0, 6, False),
                      ('b', 1): (_loc, ('a', 0), 6, 6, True),
                      ('c', 0): ('b', 0),
                      ('c', 1): ('b', 1)}

    result = repartition_divisions([1, 3, 7], [1, 4, 6, 7], 'a', 'b', 'c')
    assert result == {('b', 0): (_loc, ('a', 0), 1, 3, False),
                      ('b', 1): (_loc, ('a', 1), 3, 4, False),
                      ('b', 2): (_loc, ('a', 1), 4, 6, False),
                      ('b', 3): (_loc, ('a', 1), 6, 7, True),
                      ('c', 0): (pd.concat, (list, [('b', 0), ('b', 1)])),
                      ('c', 1): ('b', 2),
                      ('c', 2): ('b', 3)}


def test_repartition_on_pandas_dataframe():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    ddf = dd.repartition(df, divisions=[10, 20, 50, 60])
    assert isinstance(ddf, dd.DataFrame)
    assert ddf.divisions == (10, 20, 50, 60)
    assert eq(ddf, df)

    ddf = dd.repartition(df.y, divisions=[10, 20, 50, 60])
    assert isinstance(ddf, dd.Series)
    assert ddf.divisions == (10, 20, 50, 60)
    assert eq(ddf, df.y)


def test_repartition_npartitions():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    for n in [1, 2, 4, 5]:
        for k in [1, 2, 4, 5]:
            if k > n:
                continue
            a = dd.from_pandas(df, npartitions=n)
            k = min(a.npartitions, k)

            b = a.repartition(npartitions=k)
            eq(a, b)
            assert b.npartitions == k

    a = dd.from_pandas(df, npartitions=1)
    with pytest.raises(ValueError):
        a.repartition(npartitions=5)


def test_embarrassingly_parallel_operations():
    df = pd.DataFrame({'x': [1, 2, 3, 4, None, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    a = dd.from_pandas(df, 2)

    assert eq(a.x.astype('float32'), df.x.astype('float32'))
    assert a.x.astype('float32').compute().dtype == 'float32'

    assert eq(a.x.dropna(), df.x.dropna())

    assert eq(a.x.fillna(100), df.x.fillna(100))
    assert eq(a.fillna(100), df.fillna(100))

    assert eq(a.x.between(2, 4), df.x.between(2, 4))

    assert eq(a.x.clip(2, 4), df.x.clip(2, 4))

    assert eq(a.x.notnull(), df.x.notnull())
    assert eq(a.x.isnull(), df.x.isnull())

    assert len(a.sample(0.5).compute()) < len(df)


def test_sample():
    df = pd.DataFrame({'x': [1, 2, 3, 4, None, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    a = dd.from_pandas(df, 2)

    b = a.sample(0.5)

    assert eq(b, b)

    c = a.sample(0.5, random_state=1234)
    d = a.sample(0.5, random_state=1234)
    assert eq(c, d)

    assert a.sample(0.5)._name != a.sample(0.5)._name


def test_sample_without_replacement():
    df = pd.DataFrame({'x': [1, 2, 3, 4, None, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    a = dd.from_pandas(df, 2)
    b = a.sample(0.7, replace=False)
    bb = b.index.compute()
    assert len(bb) == len(set(bb))


def test_datetime_accessor():
    df = pd.DataFrame({'x': [1, 2, 3, 4]})
    df['x'] = df.x.astype('M8[us]')

    a = dd.from_pandas(df, 2)

    assert 'date' in dir(a.x.dt)

    # pandas loses Series.name via datetime accessor
    # see https://github.com/pydata/pandas/issues/10712
    assert eq(a.x.dt.date, df.x.dt.date, check_names=False)
    assert (a.x.dt.to_pydatetime().compute() == df.x.dt.to_pydatetime()).all()

    assert a.x.dt.date.dask == a.x.dt.date.dask
    assert a.x.dt.to_pydatetime().dask == a.x.dt.to_pydatetime().dask


def test_str_accessor():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'D']})

    a = dd.from_pandas(df, 2)

    assert 'upper' in dir(a.x.str)

    assert eq(a.x.str.upper(), df.x.str.upper())

    assert a.x.str.upper().dask == a.x.str.upper().dask


def test_empty_max():
    a = dd.DataFrame({('x', 0): pd.DataFrame({'x': [1]}),
                      ('x', 1): pd.DataFrame({'x': []})}, 'x',
                      ['x'], [None, None, None])
    assert eq(a.x.max(), 1)


def test_loc_on_numpy_datetimes():
    df = pd.DataFrame({'x': [1, 2, 3]},
                      index=list(map(np.datetime64, ['2014', '2015', '2016'])))
    a = dd.from_pandas(df, 2)
    a.divisions = list(map(np.datetime64, a.divisions))

    assert eq(a.loc['2014': '2015'], a.loc['2014': '2015'])


def test_loc_on_pandas_datetimes():
    df = pd.DataFrame({'x': [1, 2, 3]},
                      index=list(map(pd.Timestamp, ['2014', '2015', '2016'])))
    a = dd.from_pandas(df, 2)
    a.divisions = list(map(pd.Timestamp, a.divisions))

    assert eq(a.loc['2014': '2015'], a.loc['2014': '2015'])


def test_coerce_loc_index():
    for t in [pd.Timestamp, np.datetime64]:
        assert isinstance(_coerce_loc_index([t('2014')], '2014'), t)


def test_nlargest_series():
    s = pd.Series([1, 3, 5, 2, 4, 6])
    ss = dd.from_pandas(s, npartitions=2)

    assert eq(ss.nlargest(2), s.nlargest(2))


def test_query():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)
    q = a.query('x**2 > y')
    with ignoring(ImportError):
        assert eq(q, df.query('x**2 > y'))


def test_deterministic_arithmetic_names():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    assert sorted((a.x + a.y ** 2).dask) == sorted((a.x + a.y ** 2).dask)
    assert sorted((a.x + a.y ** 2).dask) != sorted((a.x + a.y ** 3).dask)
    assert sorted((a.x + a.y ** 2).dask) != sorted((a.x - a.y ** 2).dask)


def test_deterministic_reduction_names():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    assert a.x.sum()._name == a.x.sum()._name
    assert a.x.mean()._name == a.x.mean()._name
    assert a.x.var()._name == a.x.var()._name
    assert a.x.min()._name == a.x.min()._name
    assert a.x.max()._name == a.x.max()._name
    assert a.x.count()._name == a.x.count()._name
    # Test reduction without token string
    assert (sorted(reduction(a.x, len, np.sum).dask) !=
            sorted(reduction(a.x, np.sum, np.sum).dask))
    assert (sorted(reduction(a.x, len, np.sum).dask) ==
            sorted(reduction(a.x, len, np.sum).dask))


def test_deterministic_apply_concat_apply_names():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    assert sorted(a.x.nlargest(2).dask) == sorted(a.x.nlargest(2).dask)
    assert sorted(a.x.nlargest(2).dask) != sorted(a.x.nlargest(3).dask)
    assert (sorted(a.x.drop_duplicates().dask) ==
            sorted(a.x.drop_duplicates().dask))
    assert (sorted(a.groupby('x').y.mean().dask) ==
            sorted(a.groupby('x').y.mean().dask))
    # Test aca without passing in token string
    f = lambda a: a.nlargest(5)
    f2 = lambda a: a.nlargest(3)
    assert (sorted(aca(a.x, f, f, a.x.name).dask) !=
            sorted(aca(a.x, f2, f2, a.x.name).dask))
    assert (sorted(aca(a.x, f, f, a.x.name).dask) ==
            sorted(aca(a.x, f, f, a.x.name).dask))


def test_gh_517():
    arr = np.random.randn(100, 2)
    df = pd.DataFrame(arr, columns=['a', 'b'])
    ddf = dd.from_pandas(df, 2)
    assert ddf.index.nunique().compute() == 100

    ddf2 = dd.from_pandas(pd.concat([df, df]), 5)
    assert ddf2.index.nunique().compute() == 100


def test_drop_axis_1():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    assert eq(a.drop('y', axis=1), df.drop('y', axis=1))


def test_gh580():
    df = pd.DataFrame({'x': np.arange(10, dtype=float)})
    ddf = dd.from_pandas(df, 2)
    assert eq(np.cos(df['x']), np.cos(ddf['x']))
    assert eq(np.cos(df['x']), np.cos(ddf['x']))


def test_rename_dict():
    renamer = {'a': 'A', 'b': 'B'}
    assert eq(d.rename(columns=renamer),
              full.rename(columns=renamer))


def test_rename_function():
    renamer = lambda x: x.upper()
    assert eq(d.rename(columns=renamer),
              full.rename(columns=renamer))


def test_rename_index():
    renamer = {0: 1}
    assert raises(ValueError, lambda: d.rename(index=renamer))


def test_to_frame():
    s = pd.Series([1, 2, 3], name='foo')
    a = dd.from_pandas(s, npartitions=2)

    assert a.to_frame()._known_dtype
    assert eq(s.to_frame(), a.to_frame())
    assert eq(s.to_frame('bar'), a.to_frame('bar'))


def test_apply():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
    ddf = dd.from_pandas(df, npartitions=2)

    func = lambda row: row['x'] + row['y']
    assert eq(ddf.x.apply(lambda x: x + 1),
              df.x.apply(lambda x: x + 1))

    # specify columns
    assert eq(ddf.apply(lambda xy: xy[0] + xy[1], axis=1, columns=None),
              df.apply(lambda xy: xy[0] + xy[1], axis=1))
    assert eq(ddf.apply(lambda xy: xy[0] + xy[1], axis='columns', columns=None),
              df.apply(lambda xy: xy[0] + xy[1], axis='columns'))

    # inferrence
    assert eq(ddf.apply(lambda xy: xy[0] + xy[1], axis=1),
              df.apply(lambda xy: xy[0] + xy[1], axis=1))
    assert eq(ddf.apply(lambda xy: xy, axis=1),
              df.apply(lambda xy: xy, axis=1))

    # result will be dataframe
    func = lambda x: pd.Series([x, x])
    assert eq(ddf.x.apply(func, name=[0, 1]), df.x.apply(func))
    # inference
    assert eq(ddf.x.apply(func), df.x.apply(func))

    # axis=0
    with tm.assertRaises(NotImplementedError):
        ddf.apply(lambda xy: xy, axis=0)

    with tm.assertRaises(NotImplementedError):
        ddf.apply(lambda xy: xy, axis='index')


def test_cov():
    df = pd.util.testing.makeMissingDataframe(0.3, 42)
    ddf = dd.from_pandas(df, npartitions=3)

    assert eq(ddf.cov(), df.cov())
    assert eq(ddf.cov(10), df.cov(10))
    assert ddf.cov()._name == ddf.cov()._name
    assert ddf.cov(10)._name != ddf.cov()._name

    a = df.A
    b = df.B
    da = dd.from_pandas(a, npartitions=3)
    db = dd.from_pandas(b, npartitions=4)
    assert eq(da.cov(db), a.cov(b))
    assert eq(da.cov(db, 10), a.cov(b, 10))
    assert da.cov(db)._name == da.cov(db)._name
    assert da.cov(db, 10)._name != da.cov(db)._name


def test_corr():
    df = pd.util.testing.makeMissingDataframe(0.3, 42)
    ddf = dd.from_pandas(df, npartitions=3)

    assert eq(ddf.corr(), df.corr())
    assert eq(ddf.corr(min_periods=10), df.corr(min_periods=10))
    assert ddf.corr()._name == ddf.corr()._name
    assert ddf.corr(min_periods=10)._name != ddf.corr()._name

    pytest.raises(NotImplementedError, lambda: ddf.corr(method='spearman'))

    a = df.A
    b = df.B
    da = dd.from_pandas(a, npartitions=3)
    db = dd.from_pandas(b, npartitions=4)
    assert eq(da.corr(db), a.corr(b))
    assert eq(da.corr(db, min_periods=10), a.corr(b, min_periods=10))
    assert da.corr(db)._name == da.corr(db)._name
    assert da.corr(db, min_periods=10)._name != da.corr(db)._name

    pytest.raises(NotImplementedError, lambda: da.corr(db, method='spearman'))
    pytest.raises(TypeError, lambda: da.corr(ddf))


def test_cov_corr_stable():
    df = pd.DataFrame(np.random.random((20000000, 2)) * 2 - 1, columns=['a', 'b'])
    ddf = dd.from_pandas(df, npartitions=50)
    assert eq(ddf.cov(), df.cov())
    assert eq(ddf.corr(), df.corr())


def test_apply_infer_columns():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
    ddf = dd.from_pandas(df, npartitions=2)

    def return_df(x):
        # will create new DataFrame which columns is ['sum', 'mean']
        return pd.Series([x.sum(), x.mean()], index=['sum', 'mean'])

    # DataFrame to completely different DataFrame
    result = ddf.apply(return_df, axis=1)
    assert isinstance(result, dd.DataFrame)
    tm.assert_index_equal(result.columns, pd.Index(['sum', 'mean']))
    assert eq(result, df.apply(return_df, axis=1))

    # DataFrame to Series
    result = ddf.apply(lambda x: 1, axis=1)
    assert isinstance(result, dd.Series)
    assert result.name is None
    assert eq(result, df.apply(lambda x: 1, axis=1))

    def return_df2(x):
        return pd.Series([x * 2, x * 3], index=['x2', 'x3'])

    # Series to completely different DataFrame
    result = ddf.x.apply(return_df2)
    assert isinstance(result, dd.DataFrame)
    tm.assert_index_equal(result.columns, pd.Index(['x2', 'x3']))
    assert eq(result, df.x.apply(return_df2))

    # Series to Series
    result = ddf.x.apply(lambda x: 1)
    assert isinstance(result, dd.Series)
    assert result.name == 'x'
    assert eq(result, df.x.apply(lambda x: 1))


def test_index_time_properties():
    i = tm.makeTimeSeries()
    a = dd.from_pandas(i, npartitions=3)

    assert (i.index.day == a.index.day.compute()).all()
    assert (i.index.month == a.index.month.compute()).all()


@pytest.mark.skipif(LooseVersion(pd.__version__) <= '0.16.2',
                    reason="nlargest not in pandas pre 0.16.2")
def test_nlargest():
    from string import ascii_lowercase
    df = pd.DataFrame({'a': np.random.permutation(10),
                       'b': list(ascii_lowercase[:10])})
    ddf = dd.from_pandas(df, npartitions=2)

    res = ddf.nlargest(5, 'a')
    exp = df.nlargest(5, 'a')
    eq(res, exp)


@pytest.mark.skipif(LooseVersion(pd.__version__) <= '0.16.2',
                    reason="nlargest not in pandas pre 0.16.2")
def test_nlargest_multiple_columns():
    from string import ascii_lowercase
    df = pd.DataFrame({'a': np.random.permutation(10),
                       'b': list(ascii_lowercase[:10]),
                       'c': np.random.permutation(10).astype('float64')})
    ddf = dd.from_pandas(df, npartitions=2)

    result = ddf.nlargest(5, ['a', 'b'])
    expected = df.nlargest(5, ['a', 'b'])
    eq(result, expected)


def test_reset_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
    ddf = dd.from_pandas(df, npartitions=2)

    res = ddf.reset_index()
    exp = df.reset_index()

    assert len(res.index.compute()) == len(exp.index)
    tm.assert_index_equal(res.columns, exp.columns)
    tm.assert_numpy_array_equal(res.compute().values, exp.values)


def test_dataframe_compute_forward_kwargs():
    x = dd.from_pandas(pd.DataFrame({'a': range(10)}), npartitions=2).a.sum()
    x.compute(bogus_keyword=10)


def test_series_iteritems():
    df = pd.DataFrame({'x': [1, 2, 3, 4]})
    ddf = dd.from_pandas(df, npartitions=2)
    for (a, b) in zip(df['x'].iteritems(), ddf['x'].iteritems()):
        assert a == b


def test_dataframe_iterrows():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
    ddf = dd.from_pandas(df, npartitions=2)

    for (a, b) in zip(df.iterrows(), ddf.iterrows()):
        tm.assert_series_equal(a[1], b[1])


def test_dataframe_itertuples():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
    ddf = dd.from_pandas(df, npartitions=2)

    for (a, b) in zip(df.itertuples(), ddf.itertuples()):
        assert a == b


def test_from_delayed():
    from dask import do
    dfs = [do(tm.makeTimeDataFrame)(i) for i in range(1, 5)]
    df = dd.from_delayed(dfs, columns=['A', 'B', 'C', 'D'])

    assert (df.compute().columns == df.columns).all()
    assert list(df.map_partitions(len).compute()) == [1, 2, 3, 4]

    ss = [df.A for df in dfs]
    s = dd.from_delayed(ss, columns='A')

    assert s.compute().name == s.name
    assert list(s.map_partitions(len).compute()) == [1, 2, 3, 4]

    df = dd.from_delayed(dfs, tm.makeTimeDataFrame(1))
    assert df._known_dtype
    assert list(df.columns) == ['A', 'B', 'C', 'D']


def test_to_delayed():
    from dask.delayed import Value
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
    ddf = dd.from_pandas(df, npartitions=2)
    a, b = ddf.to_delayed()
    assert isinstance(a, Value)
    assert isinstance(b, Value)

    assert eq(a.compute(), df.iloc[:2])


def test_astype():
    df = pd.DataFrame({'x': [1, 2, 3, None], 'y': [10, 20, 30, 40]},
                      index=[10, 20, 30, 40])
    a = dd.from_pandas(df, 2)

    assert eq(a.astype(float), df.astype(float))
    assert eq(a.x.astype(float), df.x.astype(float))


def test_groupby_callable():
    a = pd.DataFrame({'x': [1, 2, 3, None], 'y': [10, 20, 30, 40]},
                      index=[1, 2, 3, 4])
    b = dd.from_pandas(a, 2)

    def iseven(x):
        return x % 2 == 0

    assert eq(a.groupby(iseven).y.sum(),
              b.groupby(iseven).y.sum())
    assert eq(a.y.groupby(iseven).sum(),
              b.y.groupby(iseven).sum())


def test_set_index_sorted_true():
    df = pd.DataFrame({'x': [1, 2, 3, 4],
                       'y': [10, 20, 30, 40],
                       'z': [4, 3, 2, 1]})
    a = dd.from_pandas(df, 2, sort=False)
    assert not a.known_divisions

    b = a.set_index('x', sorted=True)
    assert b.known_divisions
    assert set(a.dask).issubset(set(b.dask))

    for drop in [True, False]:
        eq(a.set_index('x', drop=drop),
           df.set_index('x', drop=drop))
        eq(a.set_index(a.x, sorted=True, drop=drop),
           df.set_index(df.x, drop=drop))
        eq(a.set_index(a.x + 1, sorted=True, drop=drop),
           df.set_index(df.x + 1, drop=drop))

    with pytest.raises(ValueError):
        a.set_index(a.z, sorted=True)


def test_methods_tokenize_differently():
    df = pd.DataFrame({'x': [1, 2, 3, 4]})
    df = dd.from_pandas(df, npartitions=1)
    assert (df.x.map_partitions(pd.Series.min)._name !=
            df.x.map_partitions(pd.Series.max)._name)

import numpy as np
import pandas as pd
import pandas.util.testing as tm

from dask.utils import raises
import dask.dataframe as dd
from dask.dataframe.utils import eq, assert_dask_graph


def groupby_internal_repr():
    pdf = pd.DataFrame({'x': [1, 2, 3, 4, 6, 7, 8, 9, 10],
                        'y': list('abcbabbcda')})
    ddf = dd.from_pandas(pdf, 3)

    gp = pdf.groupby('y')
    dp = ddf.groupby('y')
    assert isinstance(dp, dd.groupby.DataFrameGroupBy)
    assert isinstance(dp._pd, pd.core.groupby.DataFrameGroupBy)
    assert isinstance(dp.obj, dd.DataFrame)
    assert eq(dp.obj, gp.obj)

    gp = pdf.groupby('y')['x']
    dp = ddf.groupby('y')['x']
    assert isinstance(dp, dd.groupby.SeriesGroupBy)
    assert isinstance(dp._pd, pd.core.groupby.SeriesGroupBy)
    # slicing should not affect to internal
    assert isinstance(dp.obj, dd.Series)
    assert eq(dp.obj, gp.obj)

    gp = pdf.groupby('y')[['x']]
    dp = ddf.groupby('y')[['x']]
    assert isinstance(dp, dd.groupby.DataFrameGroupBy)
    assert isinstance(dp._pd, pd.core.groupby.DataFrameGroupBy)
    # slicing should not affect to internal
    assert isinstance(dp.obj, dd.DataFrame)
    assert eq(dp.obj, gp.obj)

    gp = pdf.groupby(pdf.y)['x']
    dp = ddf.groupby(ddf.y)['x']
    assert isinstance(dp, dd.groupby.SeriesGroupBy)
    assert isinstance(dp._pd, pd.core.groupby.SeriesGroupBy)
    # slicing should not affect to internal
    assert isinstance(dp.obj, dd.Series)
    assert eq(dp.obj, gp.obj)

    gp = pdf.groupby(pdf.y)[['x']]
    dp = ddf.groupby(ddf.y)[['x']]
    assert isinstance(dp, dd.groupby.DataFrameGroupBy)
    assert isinstance(dp._pd, pd.core.groupby.DataFrameGroupBy)
    # slicing should not affect to internal
    assert isinstance(dp.obj, dd.DataFrame)
    assert eq(dp.obj, gp.obj)


def groupby_error():
    pdf = pd.DataFrame({'x': [1, 2, 3, 4, 6, 7, 8, 9, 10],
                        'y': list('abcbabbcda')})
    ddf = dd.from_pandas(pdf, 3)

    with tm.assertRaises(KeyError):
        ddf.groupby('A')

    with tm.assertRaises(KeyError):
        ddf.groupby(['x', 'A'])

    dp = ddf.groupby('y')

    msg = 'Column not found: '
    with tm.assertRaisesRegexp(KeyError, msg):
        dp['A']

    with tm.assertRaisesRegexp(KeyError, msg):
        dp[['x', 'A']]


def groupby_internal_head():
    pdf = pd.DataFrame({'A': [1, 2] * 10,
                        'B': np.random.randn(20),
                        'C': np.random.randn(20)})
    ddf = dd.from_pandas(pdf, 3)

    assert eq(ddf.groupby('A')._head().sum(),
              pdf.head().groupby('A').sum())

    assert eq(ddf.groupby(ddf['A'])._head().sum(),
              pdf.head().groupby(pdf['A']).sum())

    assert eq(ddf.groupby(ddf['A'] + 1)._head().sum(),
              pdf.head().groupby(pdf['A'] + 1).sum())


def test_full_groupby():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    full = d.compute()

    assert raises(Exception, lambda: d.groupby('does_not_exist'))
    assert raises(Exception, lambda: d.groupby('a').does_not_exist)
    assert 'b' in dir(d.groupby('a'))

    def func(df):
        df['b'] = df.b - df.b.mean()
        return df

    assert eq(d.groupby('a').apply(func), full.groupby('a').apply(func))


def test_groupby_on_index():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    full = d.compute()

    e = d.set_index('a')
    efull = full.set_index('a')
    assert eq(d.groupby('a').b.mean(), e.groupby(e.index).b.mean())

    def func(df):
        df.loc[:, 'b'] = df.b - df.b.mean()
        return df

    assert eq(d.groupby('a').apply(func).set_index('a'),
              e.groupby(e.index).apply(func))
    assert eq(d.groupby('a').apply(func), full.groupby('a').apply(func))
    assert eq(d.groupby('a').apply(func).set_index('a'),
              full.groupby('a').apply(func).set_index('a'))
    assert eq(efull.groupby(efull.index).apply(func),
              e.groupby(e.index).apply(func))


def test_groupby_multilevel_getitem():
    df = pd.DataFrame({'a': [1, 2, 3, 1, 2, 3],
                       'b': [1, 2, 1, 4, 2, 1],
                       'c': [1, 3, 2, 1, 1, 2],
                       'd': [1, 2, 1, 1, 2, 2]})
    ddf = dd.from_pandas(df, 2)

    cases = [(ddf.groupby('a')['b'], df.groupby('a')['b']),
             (ddf.groupby(['a', 'b']), df.groupby(['a', 'b'])),
             (ddf.groupby(['a', 'b'])['c'], df.groupby(['a', 'b'])['c']),
             (ddf.groupby('a')[['b', 'c']], df.groupby('a')[['b', 'c']]),
             (ddf.groupby('a')[['b']], df.groupby('a')[['b']]),
             (ddf.groupby(['a', 'b', 'c']), df.groupby(['a', 'b', 'c']))]

    for d, p in cases:
        assert isinstance(d, dd.groupby._GroupBy)
        assert isinstance(p, pd.core.groupby.GroupBy)
        assert eq(d.sum(), p.sum())
        assert eq(d.min(), p.min())
        assert eq(d.max(), p.max())
        assert eq(d.count(), p.count())
        assert eq(d.mean(), p.mean().astype(float))


def test_groupby_get_group():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 6], 'b': [4, 2, 7]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 2, 6], 'b': [3, 3, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [4, 3, 7], 'b': [1, 1, 3]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    full = d.compute()

    for ddkey, pdkey in [('b', 'b'), (d.b, full.b),
                         (d.b + 1, full.b + 1)]:
        ddgrouped = d.groupby(ddkey)
        pdgrouped = full.groupby(pdkey)
        # DataFrame
        assert eq(ddgrouped.get_group(2), pdgrouped.get_group(2))
        assert eq(ddgrouped.get_group(3), pdgrouped.get_group(3))
        # Series
        assert eq(ddgrouped.a.get_group(3), pdgrouped.a.get_group(3))
        assert eq(ddgrouped.a.get_group(2), pdgrouped.a.get_group(2))


def test_dataframe_groupby_nunique():
    strings = list('aaabbccccdddeee')
    data = np.random.randn(len(strings))
    ps = pd.DataFrame(dict(strings=strings, data=data))
    s = dd.from_pandas(ps, npartitions=3)
    expected = ps.groupby('strings')['data'].nunique()
    assert eq(s.groupby('strings')['data'].nunique(), expected)


def test_dataframe_groupby_nunique_across_group_same_value():
    strings = list('aaabbccccdddeee')
    data = list(map(int, '123111223323412'))
    ps = pd.DataFrame(dict(strings=strings, data=data))
    s = dd.from_pandas(ps, npartitions=3)
    expected = ps.groupby('strings')['data'].nunique()
    assert eq(s.groupby('strings')['data'].nunique(), expected)


def test_series_groupby_propagates_names():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    ddf = dd.from_pandas(df, 2)
    func = lambda df: df['y'].sum()

    result = ddf.groupby('x').apply(func, columns='y')

    expected = df.groupby('x').apply(func)
    expected.name = 'y'
    assert eq(result, expected)


def test_series_groupby():
    s = pd.Series([1, 2, 2, 1, 1])
    pd_group = s.groupby(s)

    ss = dd.from_pandas(s, npartitions=2)
    dask_group = ss.groupby(ss)

    pd_group2 = s.groupby(s + 1)
    dask_group2 = ss.groupby(ss + 1)

    for dg, pdg in [(dask_group, pd_group), (pd_group2, dask_group2)]:
        assert eq(dg.count(), pdg.count())
        assert eq(dg.sum(), pdg.sum())
        assert eq(dg.min(), pdg.min())
        assert eq(dg.max(), pdg.max())


def test_series_groupby_errors():
    s = pd.Series([1, 2, 2, 1, 1])

    ss = dd.from_pandas(s, npartitions=2)

    msg = "Grouper for '1' not 1-dimensional"
    with tm.assertRaisesRegexp(ValueError, msg):
        s.groupby([1, 2])  # pandas
    with tm.assertRaisesRegexp(ValueError, msg):
        ss.groupby([1, 2]) # dask should raise the same error
    msg = "Grouper for '2' not 1-dimensional"
    with tm.assertRaisesRegexp(ValueError, msg):
        s.groupby([2])  # pandas
    with tm.assertRaisesRegexp(ValueError, msg):
        ss.groupby([2]) # dask should raise the same error

    msg = "No group keys passed!"
    with tm.assertRaisesRegexp(ValueError, msg):
        s.groupby([])  # pandas
    with tm.assertRaisesRegexp(ValueError, msg):
        ss.groupby([]) # dask should raise the same error

    sss = dd.from_pandas(s, npartitions=3)
    assert raises(NotImplementedError, lambda: ss.groupby(sss))

    with tm.assertRaises(KeyError):
        s.groupby('x')  # pandas
    with tm.assertRaises(KeyError):
        ss.groupby('x') # dask should raise the same error


def test_groupby_index_array():
    df = tm.makeTimeDataFrame()
    ddf = dd.from_pandas(df, npartitions=2)

    eq(df.A.groupby(df.index.month).nunique(),
       ddf.A.groupby(ddf.index.month).nunique(), check_names=False)


def test_groupby_set_index():
    df = tm.makeTimeDataFrame()
    ddf = dd.from_pandas(df, npartitions=2)
    assert raises(NotImplementedError,
                  lambda: ddf.groupby(df.index.month, as_index=False))


def test_split_apply_combine_on_series():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 6], 'b': [4, 2, 7]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 4, 6], 'b': [3, 3, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [4, 3, 7], 'b': [1, 1, 3]},
                                  index=[9, 9, 9])}
    ddf1 = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    pdf1 = ddf1.compute()

    for ddkey, pdkey in [('b', 'b'), (ddf1.b, pdf1.b),
                         (ddf1.b + 1, pdf1.b + 1)]:
        assert eq(ddf1.groupby(ddkey).a.min(), pdf1.groupby(pdkey).a.min())
        assert eq(ddf1.groupby(ddkey).a.max(), pdf1.groupby(pdkey).a.max())
        assert eq(ddf1.groupby(ddkey).a.count(), pdf1.groupby(pdkey).a.count())
        assert eq(ddf1.groupby(ddkey).a.mean(), pdf1.groupby(pdkey).a.mean())
        assert eq(ddf1.groupby(ddkey).a.nunique(), pdf1.groupby(pdkey).a.nunique())

        assert eq(ddf1.groupby(ddkey).sum(), pdf1.groupby(pdkey).sum())
        assert eq(ddf1.groupby(ddkey).min(), pdf1.groupby(pdkey).min())
        assert eq(ddf1.groupby(ddkey).max(), pdf1.groupby(pdkey).max())
        assert eq(ddf1.groupby(ddkey).count(), pdf1.groupby(pdkey).count())
        assert eq(ddf1.groupby(ddkey).mean(), pdf1.groupby(pdkey).mean())

    for ddkey, pdkey in [(ddf1.b, pdf1.b), (ddf1.b + 1, pdf1.b + 1)]:
        assert eq(ddf1.a.groupby(ddkey).sum(), pdf1.a.groupby(pdkey).sum(), check_names=False)
        assert eq(ddf1.a.groupby(ddkey).max(), pdf1.a.groupby(pdkey).max(), check_names=False)
        assert eq(ddf1.a.groupby(ddkey).count(), pdf1.a.groupby(pdkey).count(), check_names=False)
        assert eq(ddf1.a.groupby(ddkey).mean(), pdf1.a.groupby(pdkey).mean(), check_names=False)
        assert eq(ddf1.a.groupby(ddkey).nunique(), pdf1.a.groupby(pdkey).nunique(), check_names=False)

    for i in range(8):
        assert eq(ddf1.groupby(ddf1.b > i).a.sum(), pdf1.groupby(pdf1.b > i).a.sum())
        assert eq(ddf1.groupby(ddf1.b > i).a.min(), pdf1.groupby(pdf1.b > i).a.min())
        assert eq(ddf1.groupby(ddf1.b > i).a.max(), pdf1.groupby(pdf1.b > i).a.max())
        assert eq(ddf1.groupby(ddf1.b > i).a.count(), pdf1.groupby(pdf1.b > i).a.count())
        assert eq(ddf1.groupby(ddf1.b > i).a.mean(), pdf1.groupby(pdf1.b > i).a.mean())
        assert eq(ddf1.groupby(ddf1.b > i).a.nunique(), pdf1.groupby(pdf1.b > i).a.nunique())

        assert eq(ddf1.groupby(ddf1.a > i).b.sum(), pdf1.groupby(pdf1.a > i).b.sum())
        assert eq(ddf1.groupby(ddf1.a > i).b.min(), pdf1.groupby(pdf1.a > i).b.min())
        assert eq(ddf1.groupby(ddf1.a > i).b.max(), pdf1.groupby(pdf1.a > i).b.max())
        assert eq(ddf1.groupby(ddf1.a > i).b.count(), pdf1.groupby(pdf1.a > i).b.count())
        assert eq(ddf1.groupby(ddf1.a > i).b.mean(), pdf1.groupby(pdf1.a > i).b.mean())
        assert eq(ddf1.groupby(ddf1.a > i).b.nunique(), pdf1.groupby(pdf1.a > i).b.nunique())

        assert eq(ddf1.groupby(ddf1.b > i).sum(), pdf1.groupby(pdf1.b > i).sum())
        assert eq(ddf1.groupby(ddf1.b > i).min(), pdf1.groupby(pdf1.b > i).min())
        assert eq(ddf1.groupby(ddf1.b > i).max(), pdf1.groupby(pdf1.b > i).max())
        assert eq(ddf1.groupby(ddf1.b > i).count(), pdf1.groupby(pdf1.b > i).count())
        assert eq(ddf1.groupby(ddf1.b > i).mean(), pdf1.groupby(pdf1.b > i).mean())

        assert eq(ddf1.groupby(ddf1.a > i).sum(), pdf1.groupby(pdf1.a > i).sum())
        assert eq(ddf1.groupby(ddf1.a > i).min(), pdf1.groupby(pdf1.a > i).min())
        assert eq(ddf1.groupby(ddf1.a > i).max(), pdf1.groupby(pdf1.a > i).max())
        assert eq(ddf1.groupby(ddf1.a > i).count(), pdf1.groupby(pdf1.a > i).count())
        assert eq(ddf1.groupby(ddf1.a > i).mean(), pdf1.groupby(pdf1.a > i).mean())

    for ddkey, pdkey in [('a', 'a'), (ddf1.a, pdf1.a),
                         (ddf1.a + 1, pdf1.a + 1), (ddf1.a > 3, pdf1.a > 3)]:
        assert eq(ddf1.groupby(ddkey).b.sum(), pdf1.groupby(pdkey).b.sum())
        assert eq(ddf1.groupby(ddkey).b.min(), pdf1.groupby(pdkey).b.min())
        assert eq(ddf1.groupby(ddkey).b.max(), pdf1.groupby(pdkey).b.max())
        assert eq(ddf1.groupby(ddkey).b.count(), pdf1.groupby(pdkey).b.count())
        assert eq(ddf1.groupby(ddkey).b.mean(), pdf1.groupby(pdkey).b.mean())
        assert eq(ddf1.groupby(ddkey).b.nunique(), pdf1.groupby(pdkey).b.nunique())

        assert eq(ddf1.groupby(ddkey).sum(), pdf1.groupby(pdkey).sum())
        assert eq(ddf1.groupby(ddkey).min(), pdf1.groupby(pdkey).min())
        assert eq(ddf1.groupby(ddkey).max(), pdf1.groupby(pdkey).max())
        assert eq(ddf1.groupby(ddkey).count(), pdf1.groupby(pdkey).count())
        assert eq(ddf1.groupby(ddkey).mean(), pdf1.groupby(pdkey).mean().astype(float))

    assert sorted(ddf1.groupby('b').a.sum().dask) == \
           sorted(ddf1.groupby('b').a.sum().dask)
    assert sorted(ddf1.groupby(ddf1.a > 3).b.mean().dask) == \
           sorted(ddf1.groupby(ddf1.a > 3).b.mean().dask)

    # test raises with incorrect key
    assert raises(KeyError, lambda: ddf1.groupby('x'))
    assert raises(KeyError, lambda: ddf1.groupby(['a', 'x']))
    assert raises(KeyError, lambda: ddf1.groupby('a')['x'])
    assert raises(KeyError, lambda: ddf1.groupby('a')['b', 'x'])
    assert raises(KeyError, lambda: ddf1.groupby('a')[['b', 'x']])

    # test graph node labels
    assert_dask_graph(ddf1.groupby('b').a.sum(), 'series-groupby-sum')
    assert_dask_graph(ddf1.groupby('b').a.min(), 'series-groupby-min')
    assert_dask_graph(ddf1.groupby('b').a.max(), 'series-groupby-max')
    assert_dask_graph(ddf1.groupby('b').a.count(), 'series-groupby-count')
    # mean consists from sum and count operations
    assert_dask_graph(ddf1.groupby('b').a.mean(), 'series-groupby-sum')
    assert_dask_graph(ddf1.groupby('b').a.mean(), 'series-groupby-count')
    assert_dask_graph(ddf1.groupby('b').a.nunique(), 'series-groupby-nunique')

    assert_dask_graph(ddf1.groupby('b').sum(), 'dataframe-groupby-sum')
    assert_dask_graph(ddf1.groupby('b').min(), 'dataframe-groupby-min')
    assert_dask_graph(ddf1.groupby('b').max(), 'dataframe-groupby-max')
    assert_dask_graph(ddf1.groupby('b').count(), 'dataframe-groupby-count')
    # mean consists from sum and count operations
    assert_dask_graph(ddf1.groupby('b').mean(), 'dataframe-groupby-sum')
    assert_dask_graph(ddf1.groupby('b').mean(), 'dataframe-groupby-count')


def test_apply_shuffle():
    pdf = pd.DataFrame({'A': [1, 2, 3, 4] * 5,
                        'B': np.random.randn(20),
                        'C': np.random.randn(20),
                        'D': np.random.randn(20)})
    ddf = dd.from_pandas(pdf, 3)

    assert eq(ddf.groupby('A').apply(lambda x: x.sum()),
              pdf.groupby('A').apply(lambda x: x.sum()))

    assert eq(ddf.groupby(ddf['A']).apply(lambda x: x.sum()),
              pdf.groupby(pdf['A']).apply(lambda x: x.sum()))

    assert eq(ddf.groupby(ddf['A'] + 1).apply(lambda x: x.sum()),
              pdf.groupby(pdf['A'] + 1).apply(lambda x: x.sum()))

    # SeriesGroupBy
    assert eq(ddf.groupby('A')['B'].apply(lambda x: x.sum()),
              pdf.groupby('A')['B'].apply(lambda x: x.sum()))

    assert eq(ddf.groupby(ddf['A'])['B'].apply(lambda x: x.sum()),
              pdf.groupby(pdf['A'])['B'].apply(lambda x: x.sum()))

    assert eq(ddf.groupby(ddf['A'] + 1)['B'].apply(lambda x: x.sum()),
              pdf.groupby(pdf['A'] + 1)['B'].apply(lambda x: x.sum()))

    # DataFrameGroupBy with column slice
    assert eq(ddf.groupby('A')[['B', 'C']].apply(lambda x: x.sum()),
              pdf.groupby('A')[['B', 'C']].apply(lambda x: x.sum()))

    assert eq(ddf.groupby(ddf['A'])[['B', 'C']].apply(lambda x: x.sum()),
              pdf.groupby(pdf['A'])[['B', 'C']].apply(lambda x: x.sum()))

    assert eq(ddf.groupby(ddf['A'] + 1)[['B', 'C']].apply(lambda x: x.sum()),
              pdf.groupby(pdf['A'] + 1)[['B', 'C']].apply(lambda x: x.sum()))

from datetime import datetime
from operator import getitem

import pandas as pd
import pandas.util.testing as tm
import numpy as np

import dask
from dask.async import get_sync
from dask.utils import raises
import dask.dataframe as dd
from dask.dataframe.core import (concat, repartition_divisions, _loc,
        _coerce_loc_index)
from dask.dataframe.core import (concat, repartition_divisions, _loc,
        _coerce_loc_index, _concat)


def check_dask(dsk, check_names=True):
    if hasattr(dsk, 'dask'):
        result = dsk.compute(get=get_sync)
        if isinstance(dsk, dd.Index):
            assert isinstance(result, pd.Index)
        elif isinstance(dsk, dd.Series):
            assert isinstance(result, pd.Series)
            assert isinstance(dsk.columns, tuple)
            assert len(dsk.columns) == 1
            if check_names:
                assert dsk.name == result.name, (dsk.name, result.name)
        elif isinstance(dsk, dd.DataFrame):
            assert isinstance(result, pd.DataFrame), type(result)
            assert isinstance(dsk.columns, tuple)
            if check_names:
                columns = pd.Index(dsk.columns)
                tm.assert_index_equal(columns, result.columns)
        elif isinstance(dsk, dd.core.Scalar):
            assert np.isscalar(result)
        else:
            msg = 'Unsupported dask instance {0} found'.format(type(dsk))
            raise AssertionError(msg)
        return result
    return dsk


def eq(a, b, check_names=True):
    a = check_dask(a, check_names=check_names)
    b = check_dask(b, check_names=check_names)
    if isinstance(a, pd.DataFrame):
        a = a.sort_index()
        b = b.sort_index()
        tm.assert_frame_equal(a, b)
    elif isinstance(a, pd.Series):
        tm.assert_series_equal(a, b)
    elif isinstance(a, pd.Index):
        tm.assert_index_equal(a, b)
    else:
        assert np.allclose(a, b)
    return True


def assert_dask_graph(dask, label):
    if hasattr(dask, 'dask'):
        dask = dask.dask
    assert isinstance(dask, dict)
    for k in dask:
        if isinstance(k, tuple):
            k = k[0]
        if k.startswith(label):
            return True
    else:
        msg = "given dask graph doesn't contan label: {0}"
        raise AssertionError(msg.format(label))


dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
full = d.compute()


def test_Dataframe():
    result = (d['a'] + 1).compute()
    expected = pd.Series([2, 3, 4, 5, 6, 7, 8, 9, 10],
                        index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
                        name='a')

    assert eq(result, expected)

    assert list(d.columns) == list(['a', 'b'])


    full = d.compute()
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
    assert sorted(d.head(2, compute=False).dask) == \
           sorted(d.head(2, compute=False).dask)
    assert sorted(d.head(2, compute=False).dask) != \
           sorted(d.head(3, compute=False).dask)

    assert eq(d.tail(2), full.tail(2))
    assert eq(d.tail(3), full.tail(3))
    assert eq(d.tail(2), dsk[('x', 2)].tail(2))
    assert eq(d['a'].tail(2), full['a'].tail(2))
    assert eq(d['a'].tail(3), full['a'].tail(3))
    assert eq(d['a'].tail(2), dsk[('x', 2)]['a'].tail(2))
    assert sorted(d.tail(2, compute=False).dask) == \
           sorted(d.tail(2, compute=False).dask)
    assert sorted(d.tail(2, compute=False).dask) != \
           sorted(d.tail(3, compute=False).dask)


def test_Series():
    assert isinstance(d.a, dd.Series)
    assert isinstance(d.a + 1, dd.Series)
    assert eq((d + 1), full + 1)
    assert repr(d.a).startswith('dd.Series')


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
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    full = d.compute()

    d2 = d.set_index('b', npartitions=3)
    assert d2.npartitions == 3
    # assert eq(d2, full.set_index('b').sort())
    assert str(d2.compute().sort(['a'])) == str(full.set_index('b').sort(['a']))

    d3 = d.set_index(d.b, npartitions=3)
    assert d3.npartitions == 3
    # assert eq(d3, full.set_index(full.b).sort())
    assert str(d3.compute().sort(['a'])) == str(full.set_index(full.b).sort(['a']))

    d4 = d.set_index('b')
    assert str(d4.compute().sort(['a'])) == str(full.set_index('b').sort(['a']))


def test_split_apply_combine_on_series():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 6], 'b': [4, 2., 7]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 2, 6], 'b': [3, 3, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [4, 3, 7], 'b': [1, 1, 3]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    full = d.compute()

    for ddkey, pdkey in [('b', 'b'), (d.b, full.b),
                         (d.b + 1, full.b + 1)]:
        assert eq(d.groupby(ddkey).a.sum(), full.groupby(pdkey).a.sum())
        assert eq(d.groupby(ddkey).a.min(), full.groupby(pdkey).a.min())
        assert eq(d.groupby(ddkey).a.max(), full.groupby(pdkey).a.max())
        assert eq(d.groupby(ddkey).a.count(), full.groupby(pdkey).a.count())
        assert eq(d.groupby(ddkey).a.mean(), full.groupby(pdkey).a.mean())
        assert eq(d.groupby(ddkey).a.nunique(), full.groupby(pdkey).a.nunique())

        assert eq(d.groupby(ddkey).sum(), full.groupby(pdkey).sum())
        assert eq(d.groupby(ddkey).min(), full.groupby(pdkey).min())
        assert eq(d.groupby(ddkey).max(), full.groupby(pdkey).max())
        assert eq(d.groupby(ddkey).count(), full.groupby(pdkey).count())
        assert eq(d.groupby(ddkey).mean(), full.groupby(pdkey).mean())

    for i in range(8):
        assert eq(d.groupby(d.b > i).a.sum(), full.groupby(full.b > i).a.sum())
        assert eq(d.groupby(d.b > i).a.min(), full.groupby(full.b > i).a.min())
        assert eq(d.groupby(d.b > i).a.max(), full.groupby(full.b > i).a.max())
        assert eq(d.groupby(d.b > i).a.count(), full.groupby(full.b > i).a.count())
        assert eq(d.groupby(d.b > i).a.mean(), full.groupby(full.b > i).a.mean())
        assert eq(d.groupby(d.b > i).a.nunique(), full.groupby(full.b > i).a.nunique())

        assert eq(d.groupby(d.a > i).b.sum(), full.groupby(full.a > i).b.sum())
        assert eq(d.groupby(d.a > i).b.min(), full.groupby(full.a > i).b.min())
        assert eq(d.groupby(d.a > i).b.max(), full.groupby(full.a > i).b.max())
        assert eq(d.groupby(d.a > i).b.count(), full.groupby(full.a > i).b.count())
        assert eq(d.groupby(d.a > i).b.mean(), full.groupby(full.a > i).b.mean())
        assert eq(d.groupby(d.a > i).b.nunique(), full.groupby(full.a > i).b.nunique())

        assert eq(d.groupby(d.b > i).sum(), full.groupby(full.b > i).sum())
        assert eq(d.groupby(d.b > i).min(), full.groupby(full.b > i).min())
        assert eq(d.groupby(d.b > i).max(), full.groupby(full.b > i).max())
        assert eq(d.groupby(d.b > i).count(), full.groupby(full.b > i).count())
        assert eq(d.groupby(d.b > i).mean(), full.groupby(full.b > i).mean())

        assert eq(d.groupby(d.a > i).sum(), full.groupby(full.a > i).sum())
        assert eq(d.groupby(d.a > i).min(), full.groupby(full.a > i).min())
        assert eq(d.groupby(d.a > i).max(), full.groupby(full.a > i).max())
        assert eq(d.groupby(d.a > i).count(), full.groupby(full.a > i).count())
        assert eq(d.groupby(d.a > i).mean(), full.groupby(full.a > i).mean())

    for ddkey, pdkey in [('a', 'a'), (d.a, full.a),
                         (d.a + 1, full.a + 1), (d.a > 3, full.a > 3)]:
        assert eq(d.groupby(ddkey).b.sum(), full.groupby(pdkey).b.sum())
        assert eq(d.groupby(ddkey).b.min(), full.groupby(pdkey).b.min())
        assert eq(d.groupby(ddkey).b.max(), full.groupby(pdkey).b.max())
        assert eq(d.groupby(ddkey).b.count(), full.groupby(pdkey).b.count())
        assert eq(d.groupby(ddkey).b.mean(), full.groupby(pdkey).b.mean())
        assert eq(d.groupby(ddkey).b.nunique(), full.groupby(pdkey).b.nunique())

        assert eq(d.groupby(ddkey).sum(), full.groupby(pdkey).sum())
        assert eq(d.groupby(ddkey).min(), full.groupby(pdkey).min())
        assert eq(d.groupby(ddkey).max(), full.groupby(pdkey).max())
        assert eq(d.groupby(ddkey).count(), full.groupby(pdkey).count())
        assert eq(d.groupby(ddkey).mean(), full.groupby(pdkey).mean().astype(float))

    assert sorted(d.groupby('b').a.sum().dask) == \
           sorted(d.groupby('b').a.sum().dask)
    assert sorted(d.groupby(d.a > 3).b.mean().dask) == \
           sorted(d.groupby(d.a > 3).b.mean().dask)

    assert_dask_graph(d.groupby('b').a.sum(), 'series-groupby-sum')
    assert_dask_graph(d.groupby('b').a.min(), 'series-groupby-min')
    assert_dask_graph(d.groupby('b').a.max(), 'series-groupby-max')
    assert_dask_graph(d.groupby('b').a.count(), 'series-groupby-count')
    # mean consists from sum and count operations
    assert_dask_graph(d.groupby('b').a.mean(), 'series-groupby-sum')
    assert_dask_graph(d.groupby('b').a.mean(), 'series-groupby-count')
    assert_dask_graph(d.groupby('b').a.nunique(), 'series-groupby-nunique')

    assert_dask_graph(d.groupby('b').sum(), 'dataframe-groupby-sum')
    assert_dask_graph(d.groupby('b').min(), 'dataframe-groupby-min')
    assert_dask_graph(d.groupby('b').max(), 'dataframe-groupby-max')
    assert_dask_graph(d.groupby('b').count(), 'dataframe-groupby-count')
    # mean consists from sum and count operations
    assert_dask_graph(d.groupby('b').mean(), 'dataframe-groupby-sum')
    assert_dask_graph(d.groupby('b').mean(), 'dataframe-groupby-count')


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


def test_arithmetic_frame():
    d2 = dd.from_pandas(pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]}), 3)
    d3 = dd.from_pandas(pd.DataFrame({'x': [5, 6, 7, 8], 'y': [2, 4, 5, 3]}), 2)
    full2 = d2.compute()
    full3 = d3.compute()

    dsk4 = {('y', 0): pd.DataFrame({'a': [3, 2, 1], 'b': [7, 8, 9]},
                                   index=[0, 1, 3]),
            ('y', 1): pd.DataFrame({'a': [5, 2, 8], 'b': [4, 2, 3]},
                                   index=[5, 6, 8]),
            ('y', 2): pd.DataFrame({'a': [1, 4, 10], 'b': [1, 0, 5]},
                                   index=[9, 9, 9])}

    d4 = dd.DataFrame(dsk4, 'y', ['a', 'b'], [0, 4, 9, 9])
    full4 = d4.compute()

    cases = [(d, d, full, full),
             (d2, d3, full2, full3),
             (d, d4, full, full4)]
    for (l, r, el, er) in cases:

        assert eq(l + r, el + er)
        assert eq(l * r, el * er)
        assert eq(l - r, el - er)
        assert eq(l / r, el / er)
        assert eq(l & r, el & er)
        assert eq(l | r, el | er)
        assert eq(l ^ r, el ^ er)
        assert eq(l // r, el // er)
        assert eq(l ** r, el ** er)
        assert eq(l % r, el % er)
        assert eq(l > r, el > er)
        assert eq(l < r, el < er)
        assert eq(l >= r, el >= er)
        assert eq(l <= r, el <= er)
        assert eq(l == r, el == er)
        assert eq(l != r, el != er)

        assert eq(l + 2, el + 2)
        assert eq(l * 2, el * 2)
        assert eq(l - 2, el - 2)
        assert eq(l / 2, el / 2)
        assert eq(l & True, el & True)
        assert eq(l | True, el | True)
        assert eq(l ^ True, el ^ True)
        assert eq(l // 2, el // 2)
        assert eq(l ** 2, el ** 2)
        assert eq(l % 2, el % 2)
        assert eq(l > 2, el > 2)
        assert eq(l < 2, el < 2)
        assert eq(l >= 2, el >= 2)
        assert eq(l <= 2, el <= 2)
        assert eq(l == 2, el == 2)
        assert eq(l != 2, el != 2)

        assert eq(2 + l, 2 + el)
        assert eq(2 * l, 2 * el)
        assert eq(2 - l, 2 - el)
        assert eq(2 / l, 2 / el)
        assert eq(True & l, True & el)
        assert eq(True | l, True | el)
        assert eq(True ^ l, True ^ el)
        assert eq(2 // l, 2 // el)
        assert eq(2 ** l, 2 ** el)
        assert eq(2 % l, 2 % el)
        assert eq(2 > l, 2 > el)
        assert eq(2 < l, 2 < el)
        assert eq(2 >= l, 2 >= el)
        assert eq(2 <= l, 2 <= el)
        assert eq(2 == l, 2 == el)
        assert eq(2 != l, 2 != el)

        assert eq(-l, -el)
        assert eq(abs(l), abs(el))
        assert eq(~(l == r), ~(el == er))


    dsk5 = {('z', 0): pd.DataFrame({'a': [3, 2, 1], 'b': [7, 8, 9]},
                                   index=[0, 1, 3]),
            ('z', 1): pd.DataFrame({'a': [5, 2, 8], 'b': [4, 2, 3]},
                                   index=[5, 6, 8]),
            ('z', 2): pd.DataFrame({'a': [1, 4, 10], 'b': [1, 0, 5]},
                                   index=[9, 9, 9])}
    d5 = dd.DataFrame(dsk4, 'z', ['a', 'b'], [0, 3, 6, 9])
    assert raises(ValueError, lambda: (d + d5).compute())


def test_reductions():
    for dds, pds in [(d.b, full.b), (d.a, full.a),
                   (d['a'], full['a']), (d['b'], full['b'])]:
        assert eq(dds.sum(), pds.sum())
        assert eq(dds.min(), pds.min())
        assert eq(dds.max(), pds.max())
        assert eq(dds.count(), pds.count())
        assert eq(dds.std(), pds.std())
        assert eq(dds.var(), pds.var())
        assert eq(dds.std(ddof=0), pds.std(ddof=0))
        assert eq(dds.var(ddof=0), pds.var(ddof=0))
        assert eq(dds.mean(), pds.mean())
        assert eq(dds.nunique(), pds.nunique())

    assert_dask_graph(d.b.sum(), 'series-sum')
    assert_dask_graph(d.b.min(), 'series-min')
    assert_dask_graph(d.b.max(), 'series-max')
    assert_dask_graph(d.b.count(), 'series-count')
    assert_dask_graph(d.b.std(), 'series-std(ddof=1)')
    assert_dask_graph(d.b.var(), 'series-var(ddof=1)')
    assert_dask_graph(d.b.std(ddof=0), 'series-std(ddof=0)')
    assert_dask_graph(d.b.var(ddof=0), 'series-var(ddof=0)')
    assert_dask_graph(d.b.mean(), 'series-mean')
    # nunique is performed using drop-duplicates
    assert_dask_graph(d.b.nunique(), 'drop-duplicates')


def test_reductions_frame():
    assert eq(d.sum(), full.sum())
    assert eq(d.min(), full.min())
    assert eq(d.max(), full.max())
    assert eq(d.count(), full.count())
    assert eq(d.std(), full.std())
    assert eq(d.var(), full.var())
    assert eq(d.std(ddof=0), full.std(ddof=0))
    assert eq(d.var(ddof=0), full.var(ddof=0))
    assert eq(d.mean(), full.mean())

    for axis in [0, 1, 'index', 'columns']:
        assert eq(d.sum(axis=axis), full.sum(axis=axis))
        assert eq(d.min(axis=axis), full.min(axis=axis))
        assert eq(d.max(axis=axis), full.max(axis=axis))
        assert eq(d.count(axis=axis), full.count(axis=axis))
        assert eq(d.std(axis=axis), full.std(axis=axis))
        assert eq(d.var(axis=axis), full.var(axis=axis))
        assert eq(d.std(axis=axis, ddof=0), full.std(axis=axis, ddof=0))
        assert eq(d.var(axis=axis, ddof=0), full.var(axis=axis, ddof=0))
        assert eq(d.mean(axis=axis), full.mean(axis=axis))

    assert raises(ValueError, lambda: d.sum(axis='incorrect').compute())

    assert_dask_graph(d.sum(), 'dataframe-sum')
    assert_dask_graph(d.min(), 'dataframe-min')
    assert_dask_graph(d.max(), 'dataframe-max')
    assert_dask_graph(d.count(), 'dataframe-count')
    # std, var, mean consists from sum and count operations
    assert_dask_graph(d.std(), 'dataframe-sum')
    assert_dask_graph(d.std(), 'dataframe-count')
    assert_dask_graph(d.var(), 'dataframe-sum')
    assert_dask_graph(d.var(), 'dataframe-count')
    assert_dask_graph(d.mean(), 'dataframe-sum')
    assert_dask_graph(d.mean(), 'dataframe-count')

def test_reductions_frame_dtypes():
    df = pd.DataFrame({'int': [1, 2, 3, 4, 5, 6, 7, 8],
                       'float': [1., 2., 3., 4., np.nan, 6., 7., 8.],
                       'dt': [pd.NaT] + [datetime(2011, i, 1) for i in range(1, 8)],
                       'str': list('abcdefgh')})
    ddf = dd.from_pandas(df, 3)
    assert eq(df.sum(), ddf.sum())
    assert eq(df.min(), ddf.min())
    assert eq(df.max(), ddf.max())
    assert eq(df.count(), ddf.count())
    assert eq(df.std(), ddf.std())
    assert eq(df.var(), ddf.var())
    assert eq(df.std(ddof=0), ddf.std(ddof=0))
    assert eq(df.var(ddof=0), ddf.var(ddof=0))
    assert eq(df.mean(), ddf.mean())

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


def test_map_partitions_multi_argument():
    assert eq(dd.map_partitions(lambda a, b: a + b, None, d.a, d.b),
              full.a + full.b)
    assert eq(dd.map_partitions(lambda a, b, c: a + b + c, None, d.a, d.b, 1),
              full.a + full.b + 1)


def test_map_partitions():
    assert eq(d.map_partitions(lambda df: df, columns=d.columns), full)
    assert eq(d.map_partitions(lambda df: df), full)
    result = d.map_partitions(lambda df: df.sum(axis=1), None)
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
    assert b.columns == a.columns
    assert eq(df, b)

    b = dd.map_partitions(lambda x: x, a.x.name, a.x)
    assert b.name == a.x.name
    assert eq(df.x, b)

    b = dd.map_partitions(lambda x: x, a.x.name, a.x)
    assert b.name == a.x.name
    assert eq(df.x, b)

    b = dd.map_partitions(lambda df: df.x + df.y, None, a)
    assert b.name == None
    assert isinstance(b, dd.Series)

    b = dd.map_partitions(lambda df: df.x + 1, 'x', a)
    assert isinstance(b, dd.Series)
    assert b.name == 'x'


def test_map_partitions_method_names():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    b = a.map_partitions(lambda x: x)
    assert isinstance(b, dd.DataFrame)
    assert b.columns == a.columns

    b = a.map_partitions(lambda df: df.x + 1, columns=None)
    assert isinstance(b, dd.Series)
    assert b.name == None

    b = a.map_partitions(lambda df: df.x + 1, columns='x')
    assert isinstance(b, dd.Series)
    assert b.name == 'x'


def test_drop_duplicates():
    assert eq(d.a.drop_duplicates(), full.a.drop_duplicates())
    assert eq(d.drop_duplicates(), full.drop_duplicates())
    assert eq(d.index.drop_duplicates(), full.index.drop_duplicates())


def test_full_groupby():
    assert raises(Exception, lambda: d.groupby('does_not_exist'))
    assert raises(Exception, lambda: d.groupby('a').does_not_exist)
    assert 'b' in dir(d.groupby('a'))
    def func(df):
        df['b'] = df.b - df.b.mean()
        return df

    assert eq(d.groupby('a').apply(func), full.groupby('a').apply(func))

    assert sorted(d.groupby('a').apply(func).dask) == \
           sorted(d.groupby('a').apply(func).dask)


def test_groupby_on_index():
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


def test_set_partition():
    d2 = d.set_partition('b', [0, 2, 9])
    assert d2.divisions == (0, 2, 9)
    expected = full.set_index('b').sort(ascending=True)
    assert eq(d2.compute().sort(ascending=True), expected)


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


def test_categorize():
    dsk = {('x', 0): pd.DataFrame({'a': ['Alice', 'Bob', 'Alice'],
                                   'b': ['C', 'D', 'E']},
                                   index=[0, 1, 2]),
           ('x', 1): pd.DataFrame({'a': ['Bob', 'Charlie', 'Charlie'],
                                   'b': ['A', 'A', 'B']},
                                   index=[3, 4, 5])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 3, 5])
    full = d.compute()

    c = d.categorize('a')
    cfull = c.compute()
    assert cfull.dtypes['a'] == 'category'
    assert cfull.dtypes['b'] == 'O'

    assert list(cfull.a.astype('O')) == list(full.a)

    assert (d._get(c.dask, c._keys()[:1])[0].dtypes == cfull.dtypes).all()

    assert (d.categorize().compute().dtypes == 'category').all()

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


def test_isin():
    assert eq(d.a.isin([0, 1, 2]), full.a.isin([0, 1, 2]))


def test_len():
    assert len(d) == len(full)
    assert len(d.a) == len(full.a)

def test_quantile():
    result = d.b.quantile([.3, .7]).compute()
    assert len(result) == 2
    assert result[0] == 0
    assert 3 < result[1] < 7


def test_empty_quantile():
    assert d.b.quantile([]).compute().tolist() == []

def test_quantiles_raises():
    assert raises(NotImplementedError, lambda: d.b.quantiles([30]))

def test_index():
    assert eq(d.index, full.index)


def test_loc():
    assert d.loc[3:8].divisions[0] == 3
    assert d.loc[3:8].divisions[-1] == 8

    assert d.loc[5].divisions == (5, 5)

    assert eq(d.loc[5], full.loc[5])
    assert eq(d.loc[3:8], full.loc[3:8])
    assert eq(d.loc[:8], full.loc[:8])
    assert eq(d.loc[3:], full.loc[3:])

    assert eq(d.a.loc[5], full.a.loc[5])
    assert eq(d.a.loc[3:8], full.a.loc[3:8])
    assert eq(d.a.loc[:8], full.a.loc[:8])
    assert eq(d.a.loc[3:], full.a.loc[3:])

    assert raises(KeyError, lambda: d.loc[1000])
    assert eq(d.loc[1000:], full.loc[1000:])
    assert eq(d.loc[-2000:-1000], full.loc[-2000:-1000])

    assert sorted(d.loc[5].dask) == sorted(d.loc[5].dask)
    assert sorted(d.loc[5].dask) != sorted(d.loc[6].dask)


def test_loc_with_text_dates():
    A = tm.makeTimeSeries(10).iloc[:5]
    B = tm.makeTimeSeries(10).iloc[5:]
    s = dd.Series({('df', 0): A, ('df', 1): B}, 'df', None,
                  [A.index.min(), A.index.max(), B.index.max()])

    assert s.loc['2000': '2010'].divisions == s.divisions
    assert eq(s.loc['2000': '2010'], s)
    assert len(s.loc['2000-01-03': '2000-01-05'].compute()) == 3


def test_loc_with_series():
    assert eq(d.loc[d.a % 2 == 0], full.loc[full.a % 2 == 0])

    assert sorted(d.loc[d.a % 2].dask) == sorted(d.loc[d.a % 2].dask)
    assert sorted(d.loc[d.a % 2].dask) != sorted(d.loc[d.a % 3].dask)


def test_iloc_raises():
    assert raises(AttributeError, lambda: d.iloc[:5])


def test_assign():
    assert eq(d.assign(c=d.a + 1, e=d.a + d.b),
              full.assign(c=full.a + 1, e=full.a + full.b))


def test_map():
    assert eq(d.a.map(lambda x: x + 1), full.a.map(lambda x: x + 1))


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

    assert raises(ValueError, lambda: d.loc[3])


def test_concat2():
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

    assert dd.concat([a, b]).dask == dd.concat([a, b]).dask


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


def test_series_nunique():
    ps = pd.Series(list('aaabbccccdddeee'), name='a')
    s = dd.from_pandas(ps, npartitions=3)
    assert eq(s.nunique(), ps.nunique())


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


def test_set_partition_2():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')})
    ddf = dd.from_pandas(df, 2)

    result = ddf.set_partition('y', ['a', 'c', 'd'])
    assert result.divisions == ('a', 'c', 'd')

    assert list(result.compute(get=get_sync).index[-2:]) == ['d', 'd']


def test_repartition():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                      index=[10, 20, 30, 40, 50, 60])
    a = dd.from_pandas(df, 2)

    b = a.repartition(divisions=[10, 20, 50, 60])
    assert b.divisions == (10, 20, 50, 60)
    assert eq(a, b)
    assert eq(a._get(b.dask, (b._name, 0)), df.iloc[:1])


def test_repartition_divisions():
    result = repartition_divisions([1, 3, 7], [1, 4, 6, 7], 'a', 'b', 'c')  # doctest: +SKIP
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


def test_datetime_accessor():
    df = pd.DataFrame({'x': [1, 2, 3, 4]})
    df['x'] = df.x.astype('M8[us]')

    a = dd.from_pandas(df, 2)

    assert 'date' in dir(a.x.dt)

    # pandas loses Series.name via datetime accessor
    # see https://github.com/pydata/pandas/issues/10712
    assert eq(a.x.dt.date, df.x.dt.date, check_names=False)
    assert (a.x.dt.to_pydatetime().compute() == df.x.dt.to_pydatetime()).all()


def test_str_accessor():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'D']})

    a = dd.from_pandas(df, 2)

    assert 'upper' in dir(a.x.str)

    assert eq(a.x.str.upper(), df.x.str.upper())


def test_empty_max():
    df = pd.DataFrame({'x': [1, 2, 3]})
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


def test_categorical_set_index():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': ['a', 'b', 'b', 'c']})
    df['y'] = df.y.astype('category')
    a = dd.from_pandas(df, npartitions=2)

    with dask.set_options(get=get_sync):
        b = a.set_index('y')
        df2 = df.set_index('y')
        assert list(b.index.compute()), list(df2.index)

        b = a.set_index(a.y)
        df2 = df.set_index(df.y)
        assert list(b.index.compute()), list(df2.index)


def test_query():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)
    assert eq(a.query('x**2 > y'), df.query('x**2 > y'))


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


def test_deterministic_apply_concat_apply_names():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    assert sorted(a.x.nlargest(2).dask) == sorted(a.x.nlargest(2).dask)
    assert sorted(a.x.nlargest(2).dask) != sorted(a.x.nlargest(3).dask)
    assert sorted(a.x.drop_duplicates().dask) == \
           sorted(a.x.drop_duplicates().dask)
    assert sorted(a.groupby('x').y.mean().dask) == \
           sorted(a.groupby('x').y.mean().dask)


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

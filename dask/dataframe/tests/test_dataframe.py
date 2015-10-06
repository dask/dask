from itertools import product
from datetime import datetime
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
            assert (np.isscalar(result) or
                    isinstance(result, (pd.Timestamp, pd.Timedelta)))
        else:
            msg = 'Unsupported dask instance {0} found'.format(type(dsk))
            raise AssertionError(msg)
        return result
    return dsk


def _maybe_sort(a):
    # sort by value, then index
    try:
        if isinstance(a, pd.DataFrame):
            # ToDo: after v0.17, we can use consistent method
            # https://github.com/pydata/pandas/pull/10726
            a = a.sort(columns=a.columns.tolist())
        else:
            a = a.order()
    except (TypeError, IndexError, ValueError):
        pass
    return a.sort_index()


def eq(a, b, check_names=True):
    a = check_dask(a, check_names=check_names)
    b = check_dask(b, check_names=check_names)
    if isinstance(a, pd.DataFrame):
        a = _maybe_sort(a)
        b = _maybe_sort(b)
        tm.assert_frame_equal(a, b)
    elif isinstance(a, pd.Series):
        a = _maybe_sort(a)
        b = _maybe_sort(b)
        tm.assert_series_equal(a, b, check_names=check_names)
    elif isinstance(a, pd.Index):
        tm.assert_index_equal(a, b)
    else:
        if a == b:
            return True
        else:
            if np.isnan(a):
                assert np.isnan(b)
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


def test_set_index_raises_error_on_bad_input():
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                        'b': [7, 6, 5, 4, 3, 2, 1]})
    ddf = dd.from_pandas(df, 2)

    assert raises(NotImplementedError, lambda: ddf.set_index(['a', 'b']))


def test_split_apply_combine_on_series():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 6], 'b': [4, 2, 7]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 4, 6], 'b': [3, 3, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [4, 3, 7], 'b': [1, 1, 3]},
                                  index=[9, 9, 9])}
    d = dd.DataFrame(dsk, 'x', ['a', 'b'], [0, 4, 9, 9])
    full = d.compute()

    for ddkey, pdkey in [('b', 'b'), (d.b, full.b),
                         (d.b + 1, full.b + 1)]:
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

    for ddkey, pdkey in [(d.b, full.b), (d.b + 1, full.b + 1)]:
        assert eq(d.a.groupby(ddkey).sum(), full.a.groupby(pdkey).sum(), check_names=False)
        assert eq(d.a.groupby(ddkey).max(), full.a.groupby(pdkey).max(), check_names=False)
        assert eq(d.a.groupby(ddkey).count(), full.a.groupby(pdkey).count(), check_names=False)
        assert eq(d.a.groupby(ddkey).mean(), full.a.groupby(pdkey).mean(), check_names=False)
        assert eq(d.a.groupby(ddkey).nunique(), full.a.groupby(pdkey).nunique(), check_names=False)

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

    # test raises with incorrect key
    assert raises(KeyError, lambda: d.groupby('x'))
    assert raises(KeyError, lambda: d.groupby(['a', 'x']))
    assert raises(KeyError, lambda: d.groupby('a')['x'])
    assert raises(KeyError, lambda: d.groupby('a')['b', 'x'])
    assert raises(KeyError, lambda: d.groupby('a')[['b', 'x']])

    # test graph node labels
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
             (ddf.groupby('a')[['b']], df.groupby('a')[['b']])]

    for d, p in cases:
        assert isinstance(d, dd.core._GroupBy)
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

def test_arithmetics():
    pdf2 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4]})
    pdf3 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                         'b': [2, 4, 5, 3, 4, 2, 1, 0]})
    ddf2 = dd.from_pandas(pdf2, 3)
    ddf3 = dd.from_pandas(pdf3, 2)

    dsk4 = {('y', 0): pd.DataFrame({'a': [3, 2, 1], 'b': [7, 8, 9]},
                                   index=[0, 1, 3]),
            ('y', 1): pd.DataFrame({'a': [5, 2, 8], 'b': [4, 2, 3]},
                                   index=[5, 6, 8]),
            ('y', 2): pd.DataFrame({'a': [1, 4, 10], 'b': [1, 0, 5]},
                                   index=[9, 9, 9])}
    ddf4 = dd.DataFrame(dsk4, 'y', ['a', 'b'], [0, 4, 9, 9])
    pdf4 =ddf4.compute()

    # Arithmetics
    cases = [(d, d, full, full),
             (d, d.repartition([0, 1, 3, 6, 9]), full, full),
             (ddf2, ddf3, pdf2, pdf3),
             (ddf2.repartition([0, 3, 6, 7]), ddf3.repartition([0, 7]),
              pdf2, pdf3),
             (ddf2.repartition([0, 7]), ddf3.repartition([0, 2, 4, 5, 7]),
              pdf2, pdf3),
             (d, ddf4, full, pdf4),
             (d, ddf4.repartition([0, 9]), full, pdf4),
             (d.repartition([0, 3, 9]), ddf4.repartition([0, 5, 9]),
              full, pdf4),
             # dask + pandas
             (d, pdf4, full, pdf4), (ddf2, pdf3, pdf2, pdf3)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b)
        check_frame_arithmetics(l, r, el, er)

    # different index, pandas raises ValueError in comparison ops

    pdf5 = pd.DataFrame({'a': [3, 2, 1, 5, 2, 8, 1, 4, 10],
                         'b': [7, 8, 9, 4, 2, 3, 1, 0, 5]},
                        index=[0, 1, 3, 5, 6, 8, 9, 9, 9])
    ddf5 = dd.from_pandas(pdf5, 2)

    pdf6 = pd.DataFrame({'a': [3, 2, 1, 5, 2 ,8, 1, 4, 10],
                         'b': [7, 8, 9, 5, 7, 8, 4, 2, 5]},
                        index=[0, 1, 2, 3, 4, 5, 6, 7, 9])
    ddf6 = dd.from_pandas(pdf6, 4)

    pdf7 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4]},
                        index=list('aaabcdeh'))
    pdf8 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                         'b': [2, 4, 5, 3, 4, 2, 1, 0]},
                        index=list('abcdefgh'))
    ddf7 = dd.from_pandas(pdf7, 3)
    ddf8 = dd.from_pandas(pdf8, 4)

    pdf9 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4],
                         'c': [5, 6, 7, 8, 1, 2, 3, 4]},
                        index=list('aaabcdeh'))
    pdf10 = pd.DataFrame({'b': [5, 6, 7, 8, 4, 3, 2, 1],
                          'c': [2, 4, 5, 3, 4, 2, 1, 0],
                          'd': [2, 4, 5, 3, 4, 2, 1, 0]},
                        index=list('abcdefgh'))
    ddf9 = dd.from_pandas(pdf9, 3)
    ddf10 = dd.from_pandas(pdf10, 4)

    # Arithmetics with different index
    cases = [(ddf5, ddf6, pdf5, pdf6),
             (ddf5.repartition([0, 9]), ddf6, pdf5, pdf6),
             (ddf5.repartition([0, 5, 9]), ddf6.repartition([0, 7, 9]),
              pdf5, pdf6),
             (ddf7, ddf8, pdf7, pdf8),
             (ddf7.repartition(['a', 'c', 'h']), ddf8.repartition(['a', 'h']),
              pdf7, pdf8),
             (ddf7.repartition(['a', 'b', 'e', 'h']),
              ddf8.repartition(['a', 'e', 'h']), pdf7, pdf8),
             (ddf9, ddf10, pdf9, pdf10),
             (ddf9.repartition(['a', 'c', 'h']), ddf10.repartition(['a', 'h']),
              pdf9, pdf10),
             # dask + pandas
             (ddf5, pdf6, pdf5, pdf6), (ddf7, pdf8, pdf7, pdf8),
             (ddf9, pdf10, pdf9, pdf10)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b,
                                 allow_comparison_ops=False)
        check_frame_arithmetics(l, r, el, er,
                                allow_comparison_ops=False)

def test_arithmetics_different_index():

    # index are different, but overwraps
    pdf1 = pd.DataFrame({'a': [1, 2, 3, 4, 5], 'b': [3, 5, 2, 5, 7]},
                        index=[1, 2, 3, 4, 5])
    ddf1 = dd.from_pandas(pdf1, 2)
    pdf2 = pd.DataFrame({'a': [3, 2, 6, 7, 8], 'b': [9, 4, 2, 6, 2]},
                        index=[3, 4, 5, 6, 7])
    ddf2 = dd.from_pandas(pdf2, 2)

    # index are not overwrapped
    pdf3 = pd.DataFrame({'a': [1, 2, 3, 4, 5], 'b': [3, 5, 2, 5, 7]},
                        index=[1, 2, 3, 4, 5])
    ddf3 = dd.from_pandas(pdf3, 2)
    pdf4 = pd.DataFrame({'a': [3, 2, 6, 7, 8], 'b': [9, 4, 2, 6, 2]},
                        index=[10, 11, 12, 13, 14])
    ddf4 = dd.from_pandas(pdf4, 2)

    # index is included in another
    pdf5 = pd.DataFrame({'a': [1, 2, 3, 4, 5], 'b': [3, 5, 2, 5, 7]},
                        index=[1, 3, 5, 7, 9])
    ddf5 = dd.from_pandas(pdf5, 2)
    pdf6 = pd.DataFrame({'a': [3, 2, 6, 7, 8], 'b': [9, 4, 2, 6, 2]},
                        index=[2, 3, 4, 5, 6])
    ddf6 = dd.from_pandas(pdf6, 2)

    cases = [(ddf1, ddf2, pdf1, pdf2),
             (ddf2, ddf1, pdf2, pdf1),
             (ddf1.repartition([1, 3, 5]), ddf2.repartition([3, 4, 7]),
              pdf1, pdf2),
             (ddf2.repartition([3, 4, 5, 7]), ddf1.repartition([1, 2, 4, 5]),
              pdf2, pdf1),
             (ddf3, ddf4, pdf3, pdf4),
             (ddf4, ddf3, pdf4, pdf3),
             (ddf3.repartition([1, 2, 3, 4, 5]),
              ddf4.repartition([10, 11, 12, 13, 14]), pdf3, pdf4),
             (ddf4.repartition([10, 14]), ddf3.repartition([1, 3, 4, 5]),
              pdf4, pdf3),
             (ddf5, ddf6, pdf5, pdf6),
             (ddf6, ddf5, pdf6, pdf5),
             (ddf5.repartition([1, 7, 8, 9]), ddf6.repartition([2, 3, 4, 6]),
              pdf5, pdf6),
             (ddf6.repartition([2, 6]), ddf5.repartition([1, 3, 7, 9]),
              pdf6, pdf5),
             # dask + pandas
             (ddf1, pdf2, pdf1, pdf2), (ddf2, pdf1, pdf2, pdf1),
             (ddf3, pdf4, pdf3, pdf4), (ddf4, pdf3, pdf4, pdf3),
             (ddf5, pdf6, pdf5, pdf6), (ddf6, pdf5, pdf6, pdf5)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b,
                                 allow_comparison_ops=False)
        check_frame_arithmetics(l, r, el, er,
                                allow_comparison_ops=False)

    pdf7 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                          'b': [5, 6, 7, 8, 1, 2, 3, 4]},
                         index=[0, 2, 4, 8, 9, 10, 11, 13])
    pdf8 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                          'b': [2, 4, 5, 3, 4, 2, 1, 0]},
                         index=[1, 3, 4, 8, 9, 11, 12, 13])
    ddf7 = dd.from_pandas(pdf7, 3)
    ddf8 = dd.from_pandas(pdf8, 2)

    pdf9 = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8],
                         'b': [5, 6, 7, 8, 1, 2, 3, 4]},
                        index=[0, 2, 4, 8, 9, 10, 11, 13])
    pdf10 = pd.DataFrame({'a': [5, 6, 7, 8, 4, 3, 2, 1],
                          'b': [2, 4, 5, 3, 4, 2, 1, 0]},
                         index=[0, 3, 4, 8, 9, 11, 12, 13])
    ddf9 = dd.from_pandas(pdf9, 3)
    ddf10 = dd.from_pandas(pdf10, 2)

    cases = [(ddf7, ddf8, pdf7, pdf8),
             (ddf8, ddf7, pdf8, pdf7),
             (ddf7.repartition([0, 13]),
              ddf8.repartition([0, 4, 11, 14], force=True),
              pdf7, pdf8),
             (ddf8.repartition([-5, 10, 15], force=True),
              ddf7.repartition([-1, 4, 11, 14], force=True), pdf8, pdf7),
             (ddf7.repartition([0, 8, 12, 13]),
              ddf8.repartition([0, 2, 8, 12, 13], force=True), pdf7, pdf8),
             (ddf8.repartition([-5, 0, 10, 20], force=True),
              ddf7.repartition([-1, 4, 11, 13], force=True), pdf8, pdf7),
             (ddf9, ddf10, pdf9, pdf10),
             (ddf10, ddf9, pdf10, pdf9),
             # dask + pandas
             (ddf7, pdf8, pdf7, pdf8), (ddf8, pdf7, pdf8, pdf7),
             (ddf9, pdf10, pdf9, pdf10), (ddf10, pdf9, pdf10, pdf9)]

    for (l, r, el, er) in cases:
        check_series_arithmetics(l.a, r.b, el.a, er.b,
                                 allow_comparison_ops=False)
        check_frame_arithmetics(l, r, el, er,
                                allow_comparison_ops=False)


def check_series_arithmetics(l, r, el, er, allow_comparison_ops=True):
    assert isinstance(l, dd.Series)
    assert isinstance(r, (dd.Series, pd.Series))
    assert isinstance(el, pd.Series)
    assert isinstance(er, pd.Series)

    # l, r may be repartitioned, test whether repartition keeps original data
    assert eq(l, el)
    assert eq(r, er)

    assert eq(l + r, el + er)
    assert eq(l * r, el * er)
    assert eq(l - r, el - er)
    assert eq(l / r, el / er)
    assert eq(l // r, el // er)
    assert eq(l ** r, el ** er)
    assert eq(l % r, el % er)

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(l & r, el & er)
        assert eq(l | r, el | er)
        assert eq(l ^ r, el ^ er)
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

    assert eq(2 + r, 2 + er)
    assert eq(2 * r, 2 * er)
    assert eq(2 - r, 2 - er)
    assert eq(2 / r, 2 / er)
    assert eq(True & r, True & er)
    assert eq(True | r, True | er)
    assert eq(True ^ r, True ^ er)
    assert eq(2 // r, 2 // er)
    assert eq(2 ** r, 2 ** er)
    assert eq(2 % r, 2 % er)
    assert eq(2 > r, 2 > er)
    assert eq(2 < r, 2 < er)
    assert eq(2 >= r, 2 >= er)
    assert eq(2 <= r, 2 <= er)
    assert eq(2 == r, 2 == er)
    assert eq(2 != r, 2 != er)

    assert eq(-l, -el)
    assert eq(abs(l), abs(el))

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(~(l == r), ~(el == er))


def check_frame_arithmetics(l, r, el, er, allow_comparison_ops=True):
    assert isinstance(l, dd.DataFrame)
    assert isinstance(r, (dd.DataFrame, pd.DataFrame))
    assert isinstance(el, pd.DataFrame)
    assert isinstance(er, pd.DataFrame)
    # l, r may be repartitioned, test whether repartition keeps original data
    assert eq(l, el)
    assert eq(r, er)

    assert eq(l + r, el + er)
    assert eq(l * r, el * er)
    assert eq(l - r, el - er)
    assert eq(l / r, el / er)
    assert eq(l // r, el // er)
    assert eq(l ** r, el ** er)
    assert eq(l % r, el % er)

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(l & r, el & er)
        assert eq(l | r, el | er)
        assert eq(l ^ r, el ^ er)
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

    if allow_comparison_ops:
        # comparison is allowed if data have same index
        assert eq(~(l == r), ~(el == er))


def test_scalar_arithmetics():
    l = dd.core.Scalar({('l', 0): 10}, 'l')
    r = dd.core.Scalar({('r', 0): 4}, 'r')
    el = 10
    er = 4

    assert isinstance(l, dd.core.Scalar)
    assert isinstance(r, dd.core.Scalar)

    # l, r may be repartitioned, test whether repartition keeps original data
    assert eq(l, el)
    assert eq(r, er)

    assert eq(l + r, el + er)
    assert eq(l * r, el * er)
    assert eq(l - r, el - er)
    assert eq(l / r, el / er)
    assert eq(l // r, el // er)
    assert eq(l ** r, el ** er)
    assert eq(l % r, el % er)

    assert eq(l & r, el & er)
    assert eq(l | r, el | er)
    assert eq(l ^ r, el ^ er)
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

    assert eq(2 + r, 2 + er)
    assert eq(2 * r, 2 * er)
    assert eq(2 - r, 2 - er)
    assert eq(2 / r, 2 / er)
    assert eq(True & r, True & er)
    assert eq(True | r, True | er)
    assert eq(True ^ r, True ^ er)
    assert eq(2 // r, 2 // er)
    assert eq(2 ** r, 2 ** er)
    assert eq(2 % r, 2 % er)
    assert eq(2 > r, 2 > er)
    assert eq(2 < r, 2 < er)
    assert eq(2 >= r, 2 >= er)
    assert eq(2 <= r, 2 <= er)
    assert eq(2 == r, 2 == er)
    assert eq(2 != r, 2 != er)

    assert eq(-l, -el)
    assert eq(abs(l), abs(el))

    assert eq(~(l == r), ~(el == er))


def test_scalar_arithmetics_with_dask_instances():
    s = dd.core.Scalar({('s', 0): 10}, 's')
    e = 10

    pds = pd.Series([1, 2, 3, 4, 5, 6, 7])
    dds = dd.from_pandas(pds, 2)

    pdf = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7],
                        'b': [7, 6, 5, 4, 3, 2, 1]})
    ddf = dd.from_pandas(pdf, 2)

    # pandas Series
    result = pds + s   # this result pd.Series (automatically computed)
    assert isinstance(result, pd.Series)
    assert eq(result, pds + e)

    result = s + pds   # this result dd.Series
    assert isinstance(result, dd.Series)
    assert eq(result, pds + e)

    # dask Series
    result = dds + s   # this result dd.Series
    assert isinstance(result, dd.Series)
    assert eq(result, pds + e)

    result = s + dds   # this result dd.Series
    assert isinstance(result, dd.Series)
    assert eq(result, pds + e)


    # pandas DataFrame
    result = pdf + s   # this result pd.DataFrame (automatically computed)
    assert isinstance(result, pd.DataFrame)
    assert eq(result, pdf + e)

    result = s + pdf   # this result dd.DataFrame
    assert isinstance(result, dd.DataFrame)
    assert eq(result, pdf + e)

    # dask DataFrame
    result = ddf + s   # this result dd.DataFrame
    assert isinstance(result, dd.DataFrame)
    assert eq(result, pdf + e)

    result = s + ddf   # this result dd.DataFrame
    assert isinstance(result, dd.DataFrame)
    assert eq(result, pdf + e)


def test_frame_series_arithmetic_methods():

    pdf1 = pd.DataFrame({'A': np.arange(10),
                         'B': [np.nan, 1, 2, 3, 4] * 2,
                         'C': [np.nan] * 10,
                         'D': np.arange(10)},
                        index=list('abcdefghij'), columns=list('ABCD'))
    pdf2 = pd.DataFrame(np.random.randn(10, 4),
                        index=list('abcdefghjk'), columns=list('ABCX'))
    ps1 = pdf1.A
    ps2 = pdf2.A

    ddf1 = dd.from_pandas(pdf1, 2)
    ddf2 = dd.from_pandas(pdf2, 2)
    ds1 = ddf1.A
    ds2 = ddf2.A

    s = dd.core.Scalar({('s', 0): 4}, 's')

    for l, r, el, er in [(ddf1, ddf2, pdf1, pdf2), (ds1, ds2, ps1, ps2),
                         (ddf1.repartition(['a', 'f', 'j']), ddf2, pdf1, pdf2),
                         (ds1.repartition(['a', 'b', 'f', 'j']), ds2, ps1, ps2),
                         (ddf1, ddf2.repartition(['a', 'k']), pdf1, pdf2),
                         (ds1, ds2.repartition(['a', 'b', 'd', 'h', 'k']), ps1, ps2),
                         (ddf1, 3, pdf1, 3), (ds1, 3, ps1, 3),
                         (ddf1, s, pdf1, 4), (ds1, s, ps1, 4)]:
        # l, r may be repartitioned, test whether repartition keeps original data
        assert eq(l, el)
        assert eq(r, er)

        assert eq(l.add(r, fill_value=0), el.add(er, fill_value=0))
        assert eq(l.sub(r, fill_value=0), el.sub(er, fill_value=0))
        assert eq(l.mul(r, fill_value=0), el.mul(er, fill_value=0))
        assert eq(l.div(r, fill_value=0), el.div(er, fill_value=0))
        assert eq(l.truediv(r, fill_value=0), el.truediv(er, fill_value=0))
        assert eq(l.floordiv(r, fill_value=1), el.floordiv(er, fill_value=1))
        assert eq(l.mod(r, fill_value=0), el.mod(er, fill_value=0))
        assert eq(l.pow(r, fill_value=0), el.pow(er, fill_value=0))

        assert eq(l.radd(r, fill_value=0), el.radd(er, fill_value=0))
        assert eq(l.rsub(r, fill_value=0), el.rsub(er, fill_value=0))
        assert eq(l.rmul(r, fill_value=0), el.rmul(er, fill_value=0))
        assert eq(l.rdiv(r, fill_value=0), el.rdiv(er, fill_value=0))
        assert eq(l.rtruediv(r, fill_value=0), el.rtruediv(er, fill_value=0))
        assert eq(l.rfloordiv(r, fill_value=1), el.rfloordiv(er, fill_value=1))
        assert eq(l.rmod(r, fill_value=0), el.rmod(er, fill_value=0))
        assert eq(l.rpow(r, fill_value=0), el.rpow(er, fill_value=0))

    for l, r, el, er in [(ddf1, ds2, pdf1, ps2), (ddf1, ddf2.X, pdf1, pdf2.X)]:
        assert eq(l, el)
        assert eq(r, er)

        # must specify axis=0 to add Series to each column
        # axis=1 is not supported (add to each row)
        assert eq(l.add(r, axis=0), el.add(er, axis=0))
        assert eq(l.sub(r, axis=0), el.sub(er, axis=0))
        assert eq(l.mul(r, axis=0), el.mul(er, axis=0))
        assert eq(l.div(r, axis=0), el.div(er, axis=0))
        assert eq(l.truediv(r, axis=0), el.truediv(er, axis=0))
        assert eq(l.floordiv(r, axis=0), el.floordiv(er, axis=0))
        assert eq(l.mod(r, axis=0), el.mod(er, axis=0))
        assert eq(l.pow(r, axis=0), el.pow(er, axis=0))

        assert eq(l.radd(r, axis=0), el.radd(er, axis=0))
        assert eq(l.rsub(r, axis=0), el.rsub(er, axis=0))
        assert eq(l.rmul(r, axis=0), el.rmul(er, axis=0))
        assert eq(l.rdiv(r, axis=0), el.rdiv(er, axis=0))
        assert eq(l.rtruediv(r, axis=0), el.rtruediv(er, axis=0))
        assert eq(l.rfloordiv(r, axis=0), el.rfloordiv(er, axis=0))
        assert eq(l.rmod(r, axis=0), el.rmod(er, axis=0))
        assert eq(l.rpow(r, axis=0), el.rpow(er, axis=0))

        assert raises(ValueError, lambda: l.add(r, axis=1))

    for l, r, el, er in [(ddf1, pdf2, pdf1, pdf2), (ddf1, ps2, pdf1, ps2)]:
        assert eq(l, el)
        assert eq(r, er)

        for axis in [0, 1, 'index', 'columns']:
            assert eq(l.add(r, axis=axis), el.add(er, axis=axis))
            assert eq(l.sub(r, axis=axis), el.sub(er, axis=axis))
            assert eq(l.mul(r, axis=axis), el.mul(er, axis=axis))
            assert eq(l.div(r, axis=axis), el.div(er, axis=axis))
            assert eq(l.truediv(r, axis=axis), el.truediv(er, axis=axis))
            assert eq(l.floordiv(r, axis=axis), el.floordiv(er, axis=axis))
            assert eq(l.mod(r, axis=axis), el.mod(er, axis=axis))
            assert eq(l.pow(r, axis=axis), el.pow(er, axis=axis))

            assert eq(l.radd(r, axis=axis), el.radd(er, axis=axis))
            assert eq(l.rsub(r, axis=axis), el.rsub(er, axis=axis))
            assert eq(l.rmul(r, axis=axis), el.rmul(er, axis=axis))
            assert eq(l.rdiv(r, axis=axis), el.rdiv(er, axis=axis))
            assert eq(l.rtruediv(r, axis=axis), el.rtruediv(er, axis=axis))
            assert eq(l.rfloordiv(r, axis=axis), el.rfloordiv(er, axis=axis))
            assert eq(l.rmod(r, axis=axis), el.rmod(er, axis=axis))
            assert eq(l.rpow(r, axis=axis), el.rpow(er, axis=axis))


def test_reductions():
    nans1 = pd.Series([1] + [np.nan] * 4 + [2] + [np.nan] * 3)
    nands1 = dd.from_pandas(nans1, 2)
    nans2 = pd.Series([1] + [np.nan] * 8)
    nands2 = dd.from_pandas(nans2, 2)
    nans3 = pd.Series([np.nan] * 9)
    nands3 = dd.from_pandas(nans3, 2)

    bools = pd.Series([True, False, True, False, True], dtype=bool)
    boolds = dd.from_pandas(bools, 2)

    for dds, pds in [(d.b, full.b), (d.a, full.a),
                     (d['a'], full['a']), (d['b'], full['b']),
                     (nands1, nans1), (nands2, nans2), (nands3, nans3),
                     (boolds, bools)]:
        assert isinstance(dds, dd.Series)
        assert isinstance(pds, pd.Series)
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
        assert eq(dds.nbytes, pds.nbytes)

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


def test_reduction_series_invalid_axis():
    for axis in [1, 'columns']:
        for s in [d.a, full.a]: # both must behave the same
            assert raises(ValueError, lambda: s.sum(axis=axis))
            assert raises(ValueError, lambda: s.min(axis=axis))
            assert raises(ValueError, lambda: s.max(axis=axis))
            # only count doesn't have axis keyword
            assert raises(TypeError, lambda: s.count(axis=axis))
            assert raises(ValueError, lambda: s.std(axis=axis))
            assert raises(ValueError, lambda: s.var(axis=axis))
            assert raises(ValueError, lambda: s.mean(axis=axis))


def test_reductions_non_numeric_dtypes():
    # test non-numric blocks

    def check_raises(d, p, func):
        assert raises((TypeError, ValueError),
                      lambda: getattr(d, func)().compute())
        assert raises((TypeError, ValueError),
                      lambda: getattr(p, func)())

    pds = pd.Series(['a', 'b', 'c', 'd', 'e'])
    dds = dd.from_pandas(pds, 2)
    assert eq(dds.sum(), pds.sum())
    assert eq(dds.min(), pds.min())
    assert eq(dds.max(), pds.max())
    assert eq(dds.count(), pds.count())
    check_raises(dds, pds, 'std')
    check_raises(dds, pds, 'var')
    check_raises(dds, pds, 'mean')
    assert eq(dds.nunique(), pds.nunique())

    for pds in [pd.Series(pd.Categorical([1, 2, 3, 4, 5], ordered=True)),
                pd.Series(pd.Categorical(list('abcde'), ordered=True)),
                pd.Series(pd.date_range('2011-01-01', freq='D', periods=5))]:
        dds = dd.from_pandas(pds, 2)

        check_raises(dds, pds, 'sum')
        assert eq(dds.min(), pds.min())
        assert eq(dds.max(), pds.max())
        assert eq(dds.count(), pds.count())
        check_raises(dds, pds, 'std')
        check_raises(dds, pds, 'var')
        check_raises(dds, pds, 'mean')
        assert eq(dds.nunique(), pds.nunique())


    pds= pd.Series(pd.timedelta_range('1 days', freq='D', periods=5))
    dds = dd.from_pandas(pds, 2)
    assert eq(dds.sum(), pds.sum())
    assert eq(dds.min(), pds.min())
    assert eq(dds.max(), pds.max())
    assert eq(dds.count(), pds.count())

    # ToDo: pandas supports timedelta std, otherwise dask raises:
    # incompatible type for a datetime/timedelta operation [__pow__]
    # assert eq(dds.std(), pds.std())
    # assert eq(dds.var(), pds.var())

    # ToDo: pandas supports timedelta std, otherwise dask raises:
    # TypeError: unsupported operand type(s) for *: 'float' and 'Timedelta'
    # assert eq(dds.mean(), pds.mean())

    assert eq(dds.nunique(), pds.nunique())


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

    # axis=0
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

    # axis=1
    assert_dask_graph(d.sum(axis=1), 'dataframe-sum(axis=1)')
    assert_dask_graph(d.min(axis=1), 'dataframe-min(axis=1)')
    assert_dask_graph(d.max(axis=1), 'dataframe-max(axis=1)')
    assert_dask_graph(d.count(axis=1), 'dataframe-count(axis=1)')
    assert_dask_graph(d.std(axis=1), 'dataframe-std(axis=1, ddof=1)')
    assert_dask_graph(d.var(axis=1), 'dataframe-var(axis=1, ddof=1)')
    assert_dask_graph(d.mean(axis=1), 'dataframe-mean(axis=1)')

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

    assert eq(df._get_numeric_data(), ddf._get_numeric_data())

    numerics = ddf[['int', 'float']]
    assert numerics._get_numeric_data().dask == numerics.dask

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


def test_get_division():
    pdf = pd.DataFrame(np.random.randn(10, 5), columns=list('abcde'))
    ddf = dd.from_pandas(pdf, 3)
    assert ddf.divisions == (0, 4, 8, 9)

    # DataFrame
    div1 = ddf.get_division(0)
    assert isinstance(div1, dd.DataFrame)
    eq(div1, pdf.loc[0:3])
    div2 = ddf.get_division(1)
    eq(div2, pdf.loc[4:7])
    div3 = ddf.get_division(2)
    eq(div3, pdf.loc[8:9])
    assert len(div1) + len(div2) + len(div3) == len(pdf)

    # Series
    div1 = ddf.a.get_division(0)
    assert isinstance(div1, dd.Series)
    eq(div1, pdf.a.loc[0:3])
    div2 = ddf.a.get_division(1)
    eq(div2, pdf.a.loc[4:7])
    div3 = ddf.a.get_division(2)
    eq(div3, pdf.a.loc[8:9])
    assert len(div1) + len(div2) + len(div3) == len(pdf.a)

    assert raises(ValueError, lambda: ddf.get_division(-1))
    assert raises(ValueError, lambda: ddf.get_division(3))

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
    # series / multiple
    result = d.b.quantile([.3, .7])
    exp = full.b.quantile([.3, .7]) # result may different
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
    exp = full.b.quantile(.5) # result may different
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
    assert raises(NotImplementedError, lambda: d.iloc[:5])


def test_getitem():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                       'B': [9, 8, 7, 6, 5, 4, 3, 2, 1],
                       'C': [True, False, True] * 3},
                      columns=list('ABC'))
    ddf = dd.from_pandas(df, 2)
    assert eq(ddf['A'], df['A'])
    assert eq(ddf[['A', 'B']], df[['A', 'B']])
    assert eq(ddf[ddf.C], df[df.C])
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

    s = pd.Series([7, 8], name=6, index=['a', 'b'])
    assert eq(ddf.append(s), df.append(s))



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


@pytest.mark.parametrize(['npartitions', 'freq', 'closed', 'label'],
                         list(product([2, 5], ['30T', 'h', 'd', 'w', 'M'],
                                      ['right', 'left'], ['right', 'left'])))
def test_series_resample(npartitions, freq, closed, label):
    index = pd.date_range('1-1-2000', '2-15-2000', freq='h')
    index = index.union(pd.date_range('4-15-2000', '5-15-2000', freq='h'))
    df = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(df, npartitions=npartitions)
    # Series output
    result = ds.resample(freq, how='mean', closed=closed, label=label).compute()
    expected = df.resample(freq, how='mean', closed=closed, label=label)
    tm.assert_series_equal(result, expected, check_dtype=False)
    # Frame output
    resampled = ds.resample(freq, how='ohlc', closed=closed, label=label)
    divisions = resampled.divisions
    result = resampled.compute()
    expected = df.resample(freq, how='ohlc', closed=closed, label=label)
    tm.assert_frame_equal(result, expected, check_dtype=False)
    assert expected.index[0] == divisions[0]
    assert expected.index[-1] == divisions[-1]


def test_series_resample_not_implemented():
    index = pd.date_range(start='20120102', periods=100, freq='T')
    s = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(s, npartitions=5)
    # Frequency doesn't evenly divide day
    assert raises(NotImplementedError, lambda: ds.resample('57T'))
    # Kwargs not implemented
    kwargs = {'fill_method': 'bfill', 'limit': 2, 'loffset': 2, 'base': 2,
              'convention': 'end', 'kind': 'period'}
    for k, v in kwargs.items():
        assert raises(NotImplementedError, lambda: ds.resample('6h', **{k: v}))


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

    assert a.x.dt.date.dask == a.x.dt.date.dask
    assert a.x.dt.to_pydatetime().dask == a.x.dt.to_pydatetime().dask


def test_str_accessor():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'D']})

    a = dd.from_pandas(df, 2)

    assert 'upper' in dir(a.x.str)

    assert eq(a.x.str.upper(), df.x.str.upper())

    assert a.x.str.upper().dask == a.x.str.upper().dask


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
    assert sorted(reduction(a.x, len, np.sum).dask) !=\
           sorted(reduction(a.x, np.sum, np.sum).dask)
    assert sorted(reduction(a.x, len, np.sum).dask) ==\
           sorted(reduction(a.x, len, np.sum).dask)


def test_deterministic_apply_concat_apply_names():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 6, 7, 8]})
    a = dd.from_pandas(df, npartitions=2)

    assert sorted(a.x.nlargest(2).dask) == sorted(a.x.nlargest(2).dask)
    assert sorted(a.x.nlargest(2).dask) != sorted(a.x.nlargest(3).dask)
    assert sorted(a.x.drop_duplicates().dask) == \
           sorted(a.x.drop_duplicates().dask)
    assert sorted(a.groupby('x').y.mean().dask) == \
           sorted(a.groupby('x').y.mean().dask)
    # Test aca without passing in token string
    f = lambda a: a.nlargest(5)
    f2 = lambda a: a.nlargest(3)
    assert sorted(aca(a.x, f, f, a.x.name).dask) !=\
           sorted(aca(a.x, f2, f2, a.x.name).dask)
    assert sorted(aca(a.x, f, f, a.x.name).dask) ==\
           sorted(aca(a.x, f, f, a.x.name).dask)


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

    assert eq(s.to_frame(), a.to_frame())
    assert eq(s.to_frame('bar'), a.to_frame('bar'))


def test_series_groupby_propagates_names():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    ddf = dd.from_pandas(df, 2)
    func = lambda df: df['y'].sum()

    result = ddf.groupby('x').apply(func, columns='y')

    expected = df.groupby('x').apply(func)
    expected.name = 'y'

    tm.assert_series_equal(result.compute(), expected)


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

    assert raises(TypeError, lambda: ss.groupby([1, 2]))
    sss = dd.from_pandas(s, npartitions=3)
    assert raises(NotImplementedError, lambda: ss.groupby(sss))


def test_apply():
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
    a = dd.from_pandas(df, npartitions=2)

    func = lambda row: row['x'] + row['y']
    eq(a.x.apply(lambda x: x + 1), df.x.apply(lambda x: x + 1))

    eq(a.apply(lambda xy: xy[0] + xy[1], axis=1, columns=None),
       df.apply(lambda xy: xy[0] + xy[1], axis=1))

    assert raises(NotImplementedError, lambda: a.apply(lambda xy: xy, axis=0))
    assert raises(ValueError, lambda: a.apply(lambda xy: xy, axis=1))

    func = lambda x: pd.Series([x, x])
    eq(a.x.apply(func, name=[0, 1]), df.x.apply(func))


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


def test_groupby_index_array():
    df = tm.makeTimeDataFrame()
    ddf = dd.from_pandas(df, npartitions=2)

    eq(df.A.groupby(df.index.month).nunique(),
       ddf.A.groupby(ddf.index.month).nunique(), check_names=False)

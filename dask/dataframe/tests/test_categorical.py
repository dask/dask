import operator
import warnings

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest

import dask
from dask.async import get_sync
import dask.dataframe as dd
from dask.dataframe.utils import make_meta, assert_eq, is_categorical_dtype


@pytest.fixture(params=[True, False])
def cat_series(request):
    ordered = request.param
    return pd.Series(pd.Categorical(list('bacbac'), ordered=ordered))


def test_is_categorical_dtype():
    df = pd.DataFrame({'cat': pd.Categorical([1, 2, 3, 4]),
                       'x': [1, 2, 3, 4]})

    assert is_categorical_dtype(df['cat'])
    assert not is_categorical_dtype(df['x'])

    ddf = dd.from_pandas(df, 2)

    assert is_categorical_dtype(ddf['cat'])
    assert not is_categorical_dtype(ddf['x'])


def test_categorize():
    dsk = {('x', 0): pd.DataFrame({'a': ['Alice', 'Bob', 'Alice'],
                                   'b': ['C', 'D', 'E']},
                                  index=[0, 1, 2]),
           ('x', 1): pd.DataFrame({'a': ['Bob', 'Charlie', 'Charlie'],
                                   'b': ['A', 'A', 'B']},
                                  index=[3, 4, 5])}
    meta = make_meta({'a': 'O', 'b': 'O'}, index=pd.Index([], 'i8'))
    d = dd.DataFrame(dsk, 'x', meta, [0, 3, 5])
    full = d.compute()

    c = d.categorize('a')
    cfull = c.compute()
    assert cfull.dtypes['a'] == 'category'
    assert cfull.dtypes['b'] == 'O'

    assert list(cfull.a.astype('O')) == list(full.a)
    assert (d._get(c.dask, c._keys()[:1])[0].dtypes == cfull.dtypes).all()
    assert (d.categorize().compute().dtypes == 'category').all()


@pytest.mark.parametrize('shuffle', ['disk', 'tasks'])
def test_categorical_set_index(shuffle):
    df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': ['a', 'b', 'b', 'c']})
    df['y'] = df.y.astype('category', ordered=True)
    a = dd.from_pandas(df, npartitions=2)

    with dask.set_options(get=get_sync, shuffle=shuffle):
        b = a.set_index('y')
        d1, d2 = b.get_partition(0), b.get_partition(1)
        assert list(d1.index.compute()) == ['a']
        assert list(sorted(d2.index.compute())) == ['b', 'b', 'c']

        b = a.set_index(a.y)
        d1, d2 = b.get_partition(0), b.get_partition(1)
        assert list(d1.index.compute()) == ['a']
        assert list(sorted(d2.index.compute())) == ['b', 'b', 'c']

        b = a.set_index('y', divisions=['a', 'b', 'c'])
        d1, d2 = b.get_partition(0), b.get_partition(1)
        assert list(d1.index.compute()) == ['a']
        assert list(sorted(d2.index.compute())) == ['b', 'b', 'c']


def test_dataframe_categoricals():
    df = pd.DataFrame({'x': list('a' * 5 + 'b' * 5 + 'c' * 5),
                       'y': range(15)})
    df.x = df.x.astype('category')
    ddf = dd.from_pandas(df, npartitions=2)
    assert (ddf.x.cat.categories == pd.Index(['a', 'b', 'c'])).all()
    assert 'cat' in dir(ddf.x)
    assert not hasattr(df.y, 'cat')
    assert 'cat' not in dir(ddf.y)


def test_categorize_nan():
    df = dd.from_pandas(pd.DataFrame({"A": ['a', 'b', 'a', float('nan')]}),
                        npartitions=2)
    with warnings.catch_warnings(record=True) as record:
        df.categorize().compute()
    assert len(record) == 0


class TestCategoricalAccessor:

    @pytest.mark.parametrize('prop, compare', [
        ('categories', tm.assert_index_equal),
        ('ordered', assert_eq),
        ('codes', assert_eq),
    ])
    def test_properties(self, cat_series, prop, compare):
        a = dd.from_pandas(cat_series, npartitions=2)
        expected = getattr(cat_series.cat, prop)
        result = getattr(a.cat, prop)
        compare(result, expected)

    @pytest.mark.parametrize('method, kwargs', [
        ('add_categories', dict(new_categories=['d', 'e'])),
        ('as_ordered', {}),
        ('as_unordered', {}),
        ('as_ordered', {}),
        ('remove_categories', dict(removals=['a'])),
        ('rename_categories', dict(new_categories=['d', 'e', 'f'])),
        ('reorder_categories', dict(new_categories=['a', 'b', 'c'])),
        ('set_categories', dict(new_categories=['a', 'e', 'b'])),
        ('remove_unused_categories', {}),
    ])
    def test_callable(self, cat_series, method, kwargs):
        a = dd.from_pandas(cat_series, npartitions=2)
        op = operator.methodcaller(method, **kwargs)
        expected = op(cat_series.cat)
        result = op(a.cat)
        assert_eq(result, expected)
        assert_eq(result._meta.cat.categories, expected.cat.categories)
        assert_eq(result._meta.cat.ordered, expected.cat.ordered)

    def test_categorical_empty(self):
        # GH 1705

        def make_empty():
            return pd.DataFrame({"A": pd.Categorical([np.nan, np.nan])})

        def make_full():
            return pd.DataFrame({"A": pd.Categorical(['a', 'a'])})

        a = dd.from_delayed([dask.delayed(make_empty)(),
                             dask.delayed(make_full)()])
        # Used to raise an IndexError
        a.A.cat.categories

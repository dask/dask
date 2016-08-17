import pandas as pd
import pandas.util.testing as tm
import pytest

import dask
from dask.async import get_sync
import dask.dataframe as dd
from dask.dataframe.utils import make_meta


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

        b = a.set_partition('y', ['a', 'b', 'c'])
        d1, d2 = b.get_partition(0), b.get_partition(1)
        assert list(d1.index.compute()) == ['a']
        assert list(sorted(d2.index.compute())) == ['b', 'b', 'c']


def test_dataframe_categoricals():
    df = pd.DataFrame({'x': list('a'*5 + 'b'*5 + 'c'*5),
                       'y': range(15)})
    df.x = df.x.astype('category')
    ddf = dd.from_pandas(df, npartitions=2)
    assert (ddf.x.cat.categories == pd.Index(['a', 'b', 'c'])).all()
    assert 'cat' in dir(ddf.x)
    assert not hasattr(df.y, 'cat')
    assert 'cat' not in dir(ddf.y)


def test_categories():
    df = pd.DataFrame({'x': [1, 2, 3, 4],
                       'y': pd.Categorical(['a', 'b', 'a', 'c'])},
                      index=pd.CategoricalIndex(['x', 'x', 'y', 'y']))

    categories = dd.categorical.get_categories(df)
    assert set(categories.keys()) == set(['y', '.index'])
    assert list(categories['y']) == ['a', 'b', 'c']
    assert list(categories['.index']) == ['x', 'y']

    df2 = dd.categorical.strip_categories(df)
    assert not dd.categorical.get_categories(df2)

    df3 = dd.categorical._categorize(categories, df2)
    tm.assert_frame_equal(df, df3)

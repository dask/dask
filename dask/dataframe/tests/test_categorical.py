import pandas as pd
import pandas.util.testing as tm

import dask
from dask.async import get_sync
import dask.dataframe as dd


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


def test_dataframe_categoricals():
    df = pd.DataFrame({'x': list('a'*5 + 'b'*5 + 'c'*5),
                       'y': range(15)})
    df.x = df.x.astype('category')
    ddf = dd.from_pandas(df, npartitions=2)
    assert (ddf.x.cat.categories == pd.Index(['a', 'b', 'c'])).all()
    assert not hasattr(df.y, 'cat')


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

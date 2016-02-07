from __future__ import absolute_import, division, print_function

import pandas as pd
from toolz import partial

from dask.base import compute


def _categorize_block(df, categories):
    """ Categorize a dataframe with given categories

    df: DataFrame
    categories: dict mapping column name to iterable of categories
    """
    df = df.copy()
    for col, vals in categories.items():
        df[col] = pd.Categorical(df[col], categories=vals, ordered=False)
    return df


def categorize(df, columns=None, **kwargs):
    """
    Convert columns of dataframe to category dtype

    This aids performance, both in-memory and in spilling to disk
    """
    if columns is None:
        dtypes = df.dtypes
        columns = [name for name, dt in zip(dtypes.index, dtypes.values)
                    if dt == 'O']
    if not isinstance(columns, (list, tuple)):
        columns = [columns]

    distincts = [df[col].drop_duplicates() for col in columns]
    values = compute(*distincts, **kwargs)

    func = partial(_categorize_block, categories=dict(zip(columns, values)))
    return df.map_partitions(func, columns=df.columns)


def _categorize(categories, df):
    """ Categorize columns in dataframe

    >>> df = pd.DataFrame({'x': [1, 2, 3], 'y': [0, 2, 0]})
    >>> categories = {'y': ['A', 'B', 'c']}
    >>> _categorize(categories, df)
       x  y
    0  1  A
    1  2  c
    2  3  A

    >>> _categorize(categories, df.y)
    0    A
    1    c
    2    A
    dtype: category
    Categories (3, object): [A, B, c]
    """
    if '.index' in categories:
        index = pd.CategoricalIndex(
                  pd.Categorical.from_codes(df.index.values, categories['.index']))
    else:
        index = df.index
    if isinstance(df, pd.Series):
        if df.name in categories:
            cat = pd.Categorical.from_codes(df.values, categories[df.name])
            return pd.Series(cat, index=index)
        else:
            return df

    else:
        return pd.DataFrame(
                dict((col, pd.Categorical.from_codes(df[col].values, categories[col])
                           if col in categories
                           else df[col].values)
                    for col in df.columns),
                columns=df.columns,
                index=index)


def strip_categories(df):
    """ Strip categories from dataframe

    >>> df = pd.DataFrame({'x': [1, 2, 3], 'y': ['A', 'B', 'A']})
    >>> df['y'] = df.y.astype('category')
    >>> strip_categories(df)
       x  y
    0  1  0
    1  2  1
    2  3  0
    """
    return pd.DataFrame(dict((col, df[col].cat.codes.values
                                   if iscategorical(df.dtypes[col])
                                   else df[col].values)
                              for col in df.columns),
                        columns=df.columns,
                        index=df.index.codes
                              if iscategorical(df.index.dtype)
                              else df.index)

def iscategorical(dt):
    return isinstance(dt, pd.core.common.CategoricalDtype)


def get_categories(df):
    """
    Get Categories of dataframe

    >>> df = pd.DataFrame({'x': [1, 2, 3], 'y': ['A', 'B', 'A']})
    >>> df['y'] = df.y.astype('category')
    >>> get_categories(df)
    {'y': Index([u'A', u'B'], dtype='object')}
    """
    result = dict((col, df[col].cat.categories) for col in df.columns
                  if iscategorical(df.dtypes[col]))
    if iscategorical(df.index.dtype):
        result['.index'] = df.index.categories
    return result


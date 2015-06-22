import pandas as pd
import numpy as np
from collections import Iterator
import toolz


def shard_df_on_index(df, divisions):
    """ Shard a DataFrame by ranges on its index

    Example
    -------

    >>> df = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]})
    >>> df
        a  b
    0   0  5
    1  10  4
    2  20  3
    3  30  2
    4  40  1

    >>> shards = list(shard_df_on_index(df, [2, 4]))
    >>> shards[0]
        a  b
    0   0  5
    1  10  4

    >>> shards[1]
        a  b
    2  20  3
    3  30  2

    >>> shards[2]
        a  b
    4  40  1
    """
    if isinstance(divisions, Iterator):
        divisions = list(divisions)
    if not len(divisions):
        yield df
    else:
        divisions = np.array(divisions)
        df = df.sort()
        indices = df.index.searchsorted(divisions, side='right')
        yield df.iloc[:indices[0]]
        for i in range(len(indices) - 1):
            yield df.iloc[indices[i]: indices[i+1]]
        yield df.iloc[indices[-1]:]


def unique(divisions):
    """ Polymorphic unique function

    >>> list(unique([1, 2, 3, 1, 2, 3]))
    [1, 2, 3]

    >>> unique(np.array([1, 2, 3, 1, 2, 3]))
    array([1, 2, 3])

    >>> unique(pd.Categorical(['Alice', 'Bob', 'Alice'], ordered=False))
    [Alice, Bob]
    Categories (2, object): [Alice, Bob]
    """
    if isinstance(divisions, np.ndarray):
        return np.unique(divisions)
    if isinstance(divisions, pd.Categorical):
        return pd.Categorical.from_codes(np.unique(divisions.codes),
                divisions.categories, divisions.ordered)
    if isinstance(divisions, (tuple, list, Iterator)):
        return tuple(toolz.unique(divisions))
    raise NotImplementedError()


def _categorize(categories, df):
    """ Categorize columns in dataframe

    >>> df = pd.DataFrame({'x': [1, 2, 3], 'y': [0, 2, 0]})
    >>> categories = {'y': ['A', 'B', 'c']}
    >>> _categorize(categories, df)
       x  y
    0  1  A
    1  2  c
    2  3  A
    """
    if isinstance(df, pd.Series):
        if df.name in categories:
            cat = pd.Categorical.from_codes(df.values, categories[df.name])
            return pd.Series(cat, index=df.index)
        else:
            return df

    else:
        return pd.DataFrame(
                dict((col, pd.Categorical.from_codes(df[col], categories[col])
                           if col in categories
                           else df[col])
                    for col in df.columns),
                columns=df.columns,
                index=df.index)


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
    return pd.DataFrame(dict((col, df[col].cat.codes
                                   if iscategorical(df.dtypes[col])
                                   else df[col])
                              for col in df.columns),
                        columns=df.columns,
                        index=df.index)

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
    return dict((col, df[col].cat.categories) for col in df.columns
                if iscategorical(df.dtypes[col]))



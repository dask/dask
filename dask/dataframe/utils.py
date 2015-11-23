from __future__ import absolute_import, division, print_function

from collections import Iterator

import numpy as np
import pandas as pd
import toolz

from dask.async import get_sync


def shard_df_on_index(df, divisions):
    """ Shard a DataFrame by ranges on its index

    Examples
    --------

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

    >>> list(shard_df_on_index(df, []))[0]  # empty case
        a  b
    0   0  5
    1  10  4
    2  20  3
    3  30  2
    4  40  1
    """
    if isinstance(divisions, Iterator):
        divisions = list(divisions)
    if not len(divisions):
        yield df
    else:
        divisions = np.array(divisions)
        df = df.sort_index()
        index = df.index
        if iscategorical(index.dtype):
            index = index.as_ordered()
        indices = index.searchsorted(divisions)
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


###############################################################
# Testing
###############################################################

import pandas.util.testing as tm
from distutils.version import LooseVersion
PANDAS_VERSION = LooseVersion(pd.__version__)

if PANDAS_VERSION >= LooseVersion('0.17.0'):
    PANDAS_0170 = True
else:
    PANDAS_0170 = False


def _check_dask(dsk, check_names=True):
    import dask.dataframe as dd
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
        if PANDAS_0170:
            if isinstance(a, pd.DataFrame):
                a = a.sort_values(by=a.columns.tolist())
            else:
                a = a.sort_values()
        else:
            if isinstance(a, pd.DataFrame):
                a = a.sort(columns=a.columns.tolist())
            else:
                a = a.order()
    except (TypeError, IndexError, ValueError):
        pass
    return a.sort_index()


def eq(a, b, check_names=True, ignore_index=False):
    a = _check_dask(a, check_names=check_names)
    b = _check_dask(b, check_names=check_names)
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
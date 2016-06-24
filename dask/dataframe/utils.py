from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion

from collections import Iterator
import sys

import numpy as np
import pandas as pd
import pandas.util.testing as tm
from pandas.core.common import is_datetime64tz_dtype
import toolz

from dask.async import get_sync

PANDAS_VERSION = LooseVersion(pd.__version__)

# Pandas <0.18.1 doesn't have the `is_extension_type` function
if PANDAS_VERSION > '0.18.0':
    is_extension_type = pd.core.common.is_extension_type
else:
    def is_extension_type(x):
        return False


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
    from dask.dataframe.categorical import iscategorical

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


_simple_fake_mapping = {
    'b': True,   # boolean
    'i': -1,   # signed int
    'u': 1,   # unsigned int
    'f': 1.0,   # float
    'c': complex(1, 0),   # complex
    'S': b'foo',   # bytestring
    'U': u'foo',   # unicode string
    'V': np.array([b' '], dtype=np.void)[0],  # void
    'M': np.datetime64('1970-01-01'),
    'm': np.timedelta64(1, 'D'),
}


def nonempty_sample_df(empty):
    """ Create a dataframe from the given empty dataframe that contains one
    row of fake data (generated from the empty dataframe's dtypes).
    """
    nonempty = {}
    idx = pd.RangeIndex(start=0, stop=1, step=1)
    for key, dtype in empty.dtypes.iteritems():
        if is_datetime64tz_dtype(dtype):
            entry = pd.Timestamp('1970-01-01', tz='America/New_York')
        elif pd.core.common.is_categorical_dtype(dtype):
            accessor = empty[key].cat
            example = accessor.categories[0]
            cat = pd.Categorical([example], categories=accessor.categories,
                                 ordered=accessor.ordered)
            entry = pd.Series(cat, name=key)
        elif dtype.kind in _simple_fake_mapping:
            entry = _simple_fake_mapping[dtype.kind]
        elif is_extension_type(dtype):
            raise TypeError("Can't handle extension dtype: {}".format(dtype))
        elif dtype.name == 'object':
            entry = 'foo'
        else:
            raise TypeError("Can't handle dtype: {}".format(dtype))

        if not isinstance(entry, pd.Series):
            entry = pd.Series([entry], name=key, index=idx)

        nonempty[key] = entry

    df = pd.DataFrame(nonempty)
    return df


###############################################################
# Testing
###############################################################


def _check_dask(dsk, check_names=True):
    import dask.dataframe as dd
    if hasattr(dsk, 'dask'):
        result = dsk.compute(get=get_sync)
        if isinstance(dsk, dd.Index):
            assert isinstance(result, pd.Index), type(result)
            if check_names:
                assert dsk.name == result.name
            # cache
            assert isinstance(dsk._pd, pd.Index), type(dsk._pd)
            if check_names:
                assert dsk._pd.name == result.name
        elif isinstance(dsk, dd.Series):
            assert isinstance(result, pd.Series), type(result)
            if check_names:
                assert dsk.name == result.name, (dsk.name, result.name)
            # cache
            assert isinstance(dsk._pd, pd.Series), type(dsk._pd)
            if check_names:
                assert dsk._pd.name == result.name
        elif isinstance(dsk, dd.DataFrame):
            assert isinstance(result, pd.DataFrame), type(result)
            assert isinstance(dsk.columns, pd.Index), type(dsk.columns)
            if check_names:
                tm.assert_index_equal(dsk.columns, result.columns)
            # cache
            assert isinstance(dsk._pd, pd.DataFrame), type(dsk._pd)
            if check_names:
                tm.assert_index_equal(dsk._pd.columns, result.columns)
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
            a = a.sort_values(by=a.columns.tolist())
        else:
            a = a.sort_values()
    except (TypeError, IndexError, ValueError):
        pass
    return a.sort_index()


def eq(a, b, check_names=True, **kwargs):
    assert_divisions(a)
    assert_divisions(b)
    assert_sane_keynames(a)
    assert_sane_keynames(b)
    a = _check_dask(a, check_names=check_names)
    b = _check_dask(b, check_names=check_names)
    if isinstance(a, pd.DataFrame):
        a = _maybe_sort(a)
        b = _maybe_sort(b)
        tm.assert_frame_equal(a, b, **kwargs)
    elif isinstance(a, pd.Series):
        a = _maybe_sort(a)
        b = _maybe_sort(b)
        tm.assert_series_equal(a, b, check_names=check_names, **kwargs)
    elif isinstance(a, pd.Index):
        tm.assert_index_equal(a, b, **kwargs)
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


def assert_divisions(ddf):
    if not hasattr(ddf, 'divisions'):
        return
    if not hasattr(ddf, 'index'):
        return
    if not ddf.known_divisions:
        return

    results = get_sync(ddf.dask, ddf._keys())
    for i, df in enumerate(results[:-1]):
        if len(df):
            assert df.index.min() >= ddf.divisions[i]
            assert df.index.max() < ddf.divisions[i + 1]

    if len(results[-1]):
        assert results[-1].index.min() >= ddf.divisions[-2]
        assert results[-1].index.max() <= ddf.divisions[-1]


def assert_sane_keynames(ddf):
    if not hasattr(ddf, 'dask'):
        return
    for k in ddf.dask.keys():
        while isinstance(k, tuple):
            k = k[0]
        assert isinstance(k, (str, bytes))
        assert len(k) < 100
        assert ' ' not in k
        if sys.version_info[0] >= 3:
            assert k.split('-')[0].isidentifier()

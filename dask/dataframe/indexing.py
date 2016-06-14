from __future__ import absolute_import, division, print_function

from datetime import datetime

import bisect
import numpy as np
import pandas as pd


def _partition_of_index_value(divisions, val):
    """ In which partition does this value lie?

    >>> _partition_of_index_value([0, 5, 10], 3)
    0
    >>> _partition_of_index_value([0, 5, 10], 8)
    1
    >>> _partition_of_index_value([0, 5, 10], 100)
    1
    >>> _partition_of_index_value([0, 5, 10], 5)  # left-inclusive divisions
    1
    """
    if divisions[0] is None:
        raise ValueError(
            "Can not use loc on DataFrame without known divisions")
    val = _coerce_loc_index(divisions, val)
    i = bisect.bisect_right(divisions, val)
    return min(len(divisions) - 2, max(0, i - 1))


def _loc(df, start, stop, include_right_boundary=True):
    """

    >>> df = pd.DataFrame({'x': [10, 20, 30, 40, 50]}, index=[1, 2, 2, 3, 4])
    >>> _loc(df, 2, None)
        x
    2  20
    2  30
    3  40
    4  50
    >>> _loc(df, 1, 3)
        x
    1  10
    2  20
    2  30
    3  40
    >>> _loc(df, 1, 3, include_right_boundary=False)
        x
    1  10
    2  20
    2  30
    """
    result = df.loc[start:stop]
    if not include_right_boundary:
        right_index = result.index.get_slice_bound(stop, 'left', 'loc')
        result = result.iloc[:right_index]
    return result


def _try_loc(df, ind):
    try:
        return df.loc[ind]
    except KeyError:
        return df.head(0)


def _coerce_loc_index(divisions, o):
    """ Transform values to be comparable against divisions

    This is particularly valuable to use with pandas datetimes
    """
    if divisions and isinstance(divisions[0], datetime):
        return pd.Timestamp(o)
    if divisions and isinstance(divisions[0], np.datetime64):
        return np.datetime64(o).astype(divisions[0].dtype)
    return o


def _maybe_partial_time_string(index, indexer, kind):
    """
    Convert indexer for partial string selection
    if data has DatetimeIndex/PeriodIndex
    """

    # do not pass dd.Index
    assert isinstance(index, pd.Index)

    if not isinstance(index, (pd.DatetimeIndex, pd.PeriodIndex)):
        return indexer

    if isinstance(indexer, slice):
        if isinstance(indexer.start, pd.compat.string_types):
            start = index._maybe_cast_slice_bound(indexer.start, 'left', kind)
        else:
            start = indexer.start

        if isinstance(indexer.stop, pd.compat.string_types):
            stop = index._maybe_cast_slice_bound(indexer.stop, 'right', kind)
        else:
            stop = indexer.stop
        return slice(start, stop)

    elif isinstance(indexer, pd.compat.string_types):
        start = index._maybe_cast_slice_bound(indexer, 'left', 'loc')
        stop = index._maybe_cast_slice_bound(indexer, 'right', 'loc')
        return slice(start, stop)
    return indexer

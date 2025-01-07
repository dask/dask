from __future__ import annotations

import datetime
from numbers import Integral

from dask.dataframe import methods

CombinedOutput = type("CombinedOutput", (tuple,), {})


def _combined_parts(prev_part, current_part, next_part, before, after):
    msg = (
        "Partition size is less than overlapping "
        "window size. Try using ``df.repartition`` "
        "to increase the partition size."
    )

    if prev_part is not None and isinstance(before, Integral):
        if prev_part.shape[0] != before:
            raise NotImplementedError(msg)

    if next_part is not None and isinstance(after, Integral):
        if next_part.shape[0] != after:
            raise NotImplementedError(msg)

    parts = [p for p in (prev_part, current_part, next_part) if p is not None]
    combined = methods.concat(parts)

    return CombinedOutput(
        (
            combined,
            len(prev_part) if prev_part is not None else None,
            len(next_part) if next_part is not None else None,
        )
    )


def overlap_chunk(func, before, after, *args, **kwargs):
    dfs = [df for df in args if isinstance(df, CombinedOutput)]
    combined, prev_part_length, next_part_length = dfs[0]

    args = [arg[0] if isinstance(arg, CombinedOutput) else arg for arg in args]

    out = func(*args, **kwargs)

    if prev_part_length is None:
        before = None
    if isinstance(before, datetime.timedelta):
        before = prev_part_length

    expansion = None
    if combined.shape[0] != 0:
        expansion = out.shape[0] // combined.shape[0]
    if before and expansion:
        before *= expansion
    if next_part_length is None:
        return out.iloc[before:]
    if isinstance(after, datetime.timedelta):
        after = next_part_length
    if after and expansion:
        after *= expansion
    return out.iloc[before:-after]


def _head_timedelta(current, next_, after):
    """Return rows of ``next_`` whose index is before the last
    observation in ``current`` + ``after``.

    Parameters
    ----------
    current : DataFrame
    next_ : DataFrame
    after : timedelta

    Returns
    -------
    overlapped : DataFrame
    """
    return next_[next_.index < (current.index.max() + after)]


def _tail_timedelta(prevs, current, before):
    """Return the concatenated rows of each dataframe in ``prevs`` whose
    index is after the first observation in ``current`` - ``before``.

    Parameters
    ----------
    current : DataFrame
    prevs : list of DataFrame objects
    before : timedelta

    Returns
    -------
    overlapped : DataFrame
    """
    selected = methods.concat(
        [prev[prev.index > (current.index.min() - before)] for prev in prevs]
    )
    return selected

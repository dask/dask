import datetime
import inspect
from numbers import Integral

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from pandas.core.window import Rolling as pd_Rolling

from dask.base import tokenize
from dask.dataframe import methods
from dask.dataframe.core import _Frame, no_default
from dask.highlevelgraph import HighLevelGraph
from dask.utils import M, derived_from, funcname, has_keyword


def overlap_chunk(
    func, prev_part, current_part, next_part, before, after, args, kwargs
):

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
    out = func(combined, *args, **kwargs)
    if prev_part is None:
        before = None
    if isinstance(before, datetime.timedelta):
        before = len(prev_part)

    expansion = None
    if combined.shape[0] != 0:
        expansion = out.shape[0] // combined.shape[0]
    if before and expansion:
        before *= expansion
    if next_part is None:
        return out.iloc[before:]
    if isinstance(after, datetime.timedelta):
        after = len(next_part)
    if after and expansion:
        after *= expansion
    return out.iloc[before:-after]


def map_overlap(
    func,
    before,
    after,
    *args,
    meta=no_default,
    enforce_metadata=True,
    transform_divisions=True,
    align_dataframes=True,
    **kwargs,
):
    """Apply a function to each partition, sharing rows with adjacent partitions.

    Parameters
    ----------
    func : function
        Function applied to each partition.
    df : dd.DataFrame, dd.Series
    before : int or timedelta
        The rows to prepend to partition ``i`` from the end of
        partition ``i - 1``.
    after : int or timedelta
        The rows to append to partition ``i`` from the beginning
        of partition ``i + 1``.
    args, kwargs :
        Arguments and keywords to pass to the function. The partition will
        be the first argument, and these will be passed *after*.

    See Also
    --------
    dd.DataFrame.map_overlap
    """
    dfs = [df for df in args if isinstance(df, _Frame)]

    if isinstance(before, datetime.timedelta) or isinstance(after, datetime.timedelta):
        if not is_datetime64_any_dtype(dfs[0].index._meta_nonempty.inferred_type):
            raise TypeError(
                "Must have a `DatetimeIndex` when using string offset "
                "for `before` and `after`"
            )
    else:
        if not (
            isinstance(before, Integral)
            and before >= 0
            and isinstance(after, Integral)
            and after >= 0
        ):
            raise ValueError("before and after must be positive integers")

    name = kwargs.pop("token", None)
    parent_meta = kwargs.pop("parent_meta", None)

    assert callable(func)
    if name is not None:
        token = tokenize(meta, *args, **kwargs)
    else:
        name = funcname(func)
        token = tokenize(func, meta, *args, **kwargs)
    name = f"{name}-{token}"

    from dask.dataframe.core import _get_meta, _maybe_from_pandas
    from dask.dataframe.multi import _maybe_align_partitions

    if align_dataframes:
        args = _maybe_from_pandas(args)
        args = _maybe_align_partitions(args)

    meta = _get_meta(args, dfs, func, kwargs, meta, parent_meta)

    dsk = {}

    prevs = _get_previous_partitions(dfs[0], before, dsk)
    nexts = _get_nexts_partitions(dfs[0], after, dsk)

    # Pop the first dataframe for the moment
    args = args[1:]

    for i, (prev, current, next) in enumerate(
        zip(prevs, dfs[0].__dask_keys__(), nexts)
    ):
        dsk[(name, i)] = (
            overlap_chunk,
            func,
            prev,
            current,
            next,
            before,
            after,
            args,
            kwargs,
        )

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=dfs)
    return dfs[0]._constructor(graph, name, meta, dfs[0].divisions)


def _get_nexts_partitions(df, after, dsk):
    df_name = df._name

    timedelta_partition_message = (
        "Partition size is less than specified window. "
        "Try using ``df.repartition`` to increase the partition size"
    )

    name_b = "overlap-append-" + tokenize(df, after)
    if after and isinstance(after, Integral):
        nexts = []
        for i in range(1, df.npartitions):
            key = (name_b, i)
            dsk[key] = (M.head, (df_name, i), after)
            nexts.append(key)
        nexts.append(None)
    elif isinstance(after, datetime.timedelta):
        # TODO: Do we have a use-case for this? Pandas doesn't allow negative rolling windows
        deltas = pd.Series(df.divisions).diff().iloc[1:-1]
        if (after > deltas).any():
            raise ValueError(timedelta_partition_message)

        nexts = []
        for i in range(1, df.npartitions):
            key = (name_b, i)
            dsk[key] = (_head_timedelta, (df_name, i - 0), (df_name, i), after)
            nexts.append(key)
        nexts.append(None)
    else:
        nexts = [None] * df.npartitions
    return nexts


def _get_previous_partitions(df, before, dsk):
    df_name = df._name

    name_a = "overlap-prepend-" + tokenize(df, before)
    if before and isinstance(before, Integral):

        prevs = [None]
        for i in range(df.npartitions - 1):
            key = (name_a, i)
            dsk[key] = (M.tail, (df_name, i), before)
            prevs.append(key)

    elif isinstance(before, datetime.timedelta):
        # Assumes monotonic (increasing?) index
        divs = pd.Series(df.divisions)
        deltas = divs.diff().iloc[1:-1]

        # In the first case window-size is larger than at least one partition, thus it is
        # necessary to calculate how many partitions must be used for each rolling task.
        # Otherwise, these calculations can be skipped (faster)

        if (before > deltas).any():
            pt_z = divs[0]
            prevs = [None]
            for i in range(df.npartitions - 1):
                # Select all indexes of relevant partitions between the current partition and
                # the partition with the highest division outside the rolling window (before)
                pt_i = divs[i + 1]

                # lower-bound the search to the first division
                lb = max(pt_i - before, pt_z)

                first, j = divs[i], i
                while first > lb and j > 0:
                    first = first - deltas[j]
                    j = j - 1

                key = (name_a, i)
                dsk[key] = (
                    _tail_timedelta,
                    [(df_name, k) for k in range(j, i + 1)],
                    (df_name, i + 1),
                    before,
                )
                prevs.append(key)

        else:
            prevs = [None]
            for i in range(df.npartitions - 1):
                key = (name_a, i)
                dsk[key] = (
                    _tail_timedelta,
                    [(df_name, i)],
                    (df_name, i + 1),
                    before,
                )
                prevs.append(key)
    else:
        prevs = [None] * df.npartitions
    return prevs


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


class Rolling:
    """Provides rolling window calculations."""

    def __init__(
        self, obj, window=None, min_periods=None, center=False, win_type=None, axis=0
    ):
        self.obj = obj  # dataframe or series
        self.window = window
        self.min_periods = min_periods
        self.center = center
        self.axis = axis
        self.win_type = win_type
        # Allow pandas to raise if appropriate
        obj._meta.rolling(**self._rolling_kwargs())
        # Using .rolling(window='2s'), pandas will convert the
        # offset str to a window in nanoseconds. But pandas doesn't
        # accept the integer window with win_type='freq', so we store
        # that information here.
        # See https://github.com/pandas-dev/pandas/issues/15969
        self._win_type = None if isinstance(self.window, int) else "freq"

    def _rolling_kwargs(self):
        return {
            "window": self.window,
            "min_periods": self.min_periods,
            "center": self.center,
            "win_type": self.win_type,
            "axis": self.axis,
        }

    @property
    def _has_single_partition(self):
        """
        Indicator for whether the object has a single partition (True)
        or multiple (False).
        """
        return (
            self.axis in (1, "columns")
            or (isinstance(self.window, Integral) and self.window <= 1)
            or self.obj.npartitions == 1
        )

    @staticmethod
    def pandas_rolling_method(df, rolling_kwargs, name, *args, **kwargs):
        rolling = df.rolling(**rolling_kwargs)
        return getattr(rolling, name)(*args, **kwargs)

    def _call_method(self, method_name, *args, **kwargs):
        rolling_kwargs = self._rolling_kwargs()
        meta = self.pandas_rolling_method(
            self.obj._meta_nonempty, rolling_kwargs, method_name, *args, **kwargs
        )

        if self._has_single_partition:
            # There's no overlap just use map_partitions
            return self.obj.map_partitions(
                self.pandas_rolling_method,
                rolling_kwargs,
                method_name,
                *args,
                token=method_name,
                meta=meta,
                **kwargs,
            )
        # Convert window to overlap
        if self.center:
            before = self.window // 2
            after = self.window - before - 1
        elif self._win_type == "freq":
            before = pd.Timedelta(self.window)
            after = 0
        else:
            before = self.window - 1
            after = 0
        return map_overlap(
            self.pandas_rolling_method,
            before,
            after,
            self.obj,
            rolling_kwargs,
            method_name,
            *args,
            token=method_name,
            meta=meta,
            **kwargs,
        )

    @derived_from(pd_Rolling)
    def count(self):
        return self._call_method("count")

    @derived_from(pd_Rolling)
    def cov(self):
        return self._call_method("cov")

    @derived_from(pd_Rolling)
    def sum(self):
        return self._call_method("sum")

    @derived_from(pd_Rolling)
    def mean(self):
        return self._call_method("mean")

    @derived_from(pd_Rolling)
    def median(self):
        return self._call_method("median")

    @derived_from(pd_Rolling)
    def min(self):
        return self._call_method("min")

    @derived_from(pd_Rolling)
    def max(self):
        return self._call_method("max")

    @derived_from(pd_Rolling)
    def std(self, ddof=1):
        return self._call_method("std", ddof=1)

    @derived_from(pd_Rolling)
    def var(self, ddof=1):
        return self._call_method("var", ddof=1)

    @derived_from(pd_Rolling)
    def skew(self):
        return self._call_method("skew")

    @derived_from(pd_Rolling)
    def kurt(self):
        return self._call_method("kurt")

    @derived_from(pd_Rolling)
    def quantile(self, quantile):
        return self._call_method("quantile", quantile)

    @derived_from(pd_Rolling)
    def apply(
        self,
        func,
        raw=None,
        engine="cython",
        engine_kwargs=None,
        args=None,
        kwargs=None,
    ):
        compat_kwargs = {}
        kwargs = kwargs or {}
        args = args or ()
        meta = self.obj._meta.rolling(0)
        if has_keyword(meta.apply, "engine"):
            # PANDAS_GT_100
            compat_kwargs = dict(engine=engine, engine_kwargs=engine_kwargs)
        if raw is None:
            # PANDAS_GT_100: The default changed from None to False
            raw = inspect.signature(meta.apply).parameters["raw"]

        return self._call_method(
            "apply", func, raw=raw, args=args, kwargs=kwargs, **compat_kwargs
        )

    @derived_from(pd_Rolling)
    def aggregate(self, func, args=(), kwargs={}, **kwds):
        return self._call_method("agg", func, args=args, kwargs=kwargs, **kwds)

    agg = aggregate

    def __repr__(self):
        def order(item):
            k, v = item
            _order = {
                "window": 0,
                "min_periods": 1,
                "center": 2,
                "win_type": 3,
                "axis": 4,
            }
            return _order[k]

        rolling_kwargs = self._rolling_kwargs()
        rolling_kwargs["window"] = self.window
        rolling_kwargs["win_type"] = self._win_type
        return "Rolling [{}]".format(
            ",".join(
                f"{k}={v}"
                for k, v in sorted(rolling_kwargs.items(), key=order)
                if v is not None
            )
        )


class RollingGroupby(Rolling):
    def __init__(
        self,
        groupby,
        window=None,
        min_periods=None,
        center=False,
        win_type=None,
        axis=0,
    ):
        self._groupby_kwargs = groupby._groupby_kwargs
        self._groupby_slice = groupby._slice

        obj = groupby.obj
        if self._groupby_slice is not None:
            if isinstance(self._groupby_slice, str):
                sliced_plus = [self._groupby_slice]
            else:
                sliced_plus = list(self._groupby_slice)
            if isinstance(groupby.by, str):
                sliced_plus.append(groupby.by)
            else:
                sliced_plus.extend(groupby.by)
            obj = obj[sliced_plus]

        super().__init__(
            obj,
            window=window,
            min_periods=min_periods,
            center=center,
            win_type=win_type,
            axis=axis,
        )

    @staticmethod
    def pandas_rolling_method(
        df,
        rolling_kwargs,
        name,
        *args,
        groupby_kwargs=None,
        groupby_slice=None,
        **kwargs,
    ):
        groupby = df.groupby(**groupby_kwargs)
        if groupby_slice:
            groupby = groupby[groupby_slice]
        rolling = groupby.rolling(**rolling_kwargs)
        return getattr(rolling, name)(*args, **kwargs).sort_index(level=-1)

    def _call_method(self, method_name, *args, **kwargs):
        return super()._call_method(
            method_name,
            *args,
            groupby_kwargs=self._groupby_kwargs,
            groupby_slice=self._groupby_slice,
            **kwargs,
        )

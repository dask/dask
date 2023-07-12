import functools

import numpy as np
from dask import is_dask_collection
from dask.dataframe.core import _concat, is_dataframe_like, is_series_like
from dask.dataframe.groupby import (
    _agg_finalize,
    _apply_chunk,
    _build_agg_args,
    _determine_levels,
    _groupby_aggregate,
    _groupby_apply_funcs,
    _normalize_spec,
    _value_counts,
    _value_counts_aggregate,
    _var_agg,
    _var_chunk,
    _var_combine,
)
from dask.utils import M, is_index_like

from dask_expr._collection import DataFrame, Series, new_collection
from dask_expr._expr import MapPartitions, Projection
from dask_expr._reductions import ApplyConcatApply, Reduction


def _as_dict(key, value):
    # Utility to convert a single kwarg to a dict.
    # The dict will be empty if the value is None
    return {} if value is None else {key: value}


###
### Groupby-aggregation expressions
###


class SingleAggregation(ApplyConcatApply):
    """Single groupby aggregation

    This is an abstract class. Sub-classes must implement
    the following methods:

    -   `groupby_chunk`: Applied to each group within
        the `chunk` method of `ApplyConcatApply`
    -   `groupby_aggregate`: Applied to each group within
        the `aggregate` method of `ApplyConcatApply`

    Parameters
    ----------
    frame: Expr
        Dataframe- or series-like expression to group.
    by: str, list or Series
        The key for grouping
    observed:
        Passed through to dataframe backend.
    dropna:
        Whether rows with NA values should be dropped.
    chunk_kwargs:
        Key-word arguments to pass to `groupby_chunk`.
    aggregate_kwargs:
        Key-word arguments to pass to `aggregate_chunk`.
    """

    _parameters = [
        "frame",
        "by",
        "observed",
        "dropna",
        "chunk_kwargs",
        "aggregate_kwargs",
        "_slice",
    ]
    _defaults = {
        "observed": None,
        "dropna": None,
        "chunk_kwargs": None,
        "aggregate_kwargs": None,
        "_slice": None,
    }

    groupby_chunk = None
    groupby_aggregate = None

    @classmethod
    def chunk(cls, df, by=None, **kwargs):
        return _apply_chunk(df, *by, **kwargs)

    @classmethod
    def aggregate(cls, inputs, **kwargs):
        return _groupby_aggregate(_concat(inputs), **kwargs)

    @property
    def chunk_kwargs(self) -> dict:
        chunk_kwargs = self.operand("chunk_kwargs") or {}
        columns = self._slice
        return {
            "chunk": self.groupby_chunk,
            "columns": columns,
            "by": self.by,
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
            **chunk_kwargs,
        }

    @property
    def aggregate_kwargs(self) -> dict:
        aggregate_kwargs = self.operand("aggregate_kwargs") or {}
        groupby_aggregate = self.groupby_aggregate or self.groupby_chunk
        return {
            "aggfunc": groupby_aggregate,
            "levels": _determine_levels(self.by),
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
            **aggregate_kwargs,
        }

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            columns = sorted(set(parent.columns + self.by))
            if columns == self.frame.columns:
                return
            return type(parent)(
                type(self)(self.frame[columns], *self.operands[1:]),
                *parent.operands[1:],
            )


class GroupbyAggregation(ApplyConcatApply):
    """General groupby aggregation

    This class can be used directly to perform a general
    groupby aggregation by passing in a `str`, `list` or
    `dict`-based specification using the `arg` operand.

    Parameters
    ----------
    frame: Expr
        Dataframe- or series-like expression to group.
    by: str, list or Series
        The key for grouping
    arg: str, list or dict
        Aggregation spec defining the specific aggregations
        to perform.
    observed:
        Passed through to dataframe backend.
    dropna:
        Whether rows with NA values should be dropped.
    chunk_kwargs:
        Key-word arguments to pass to `groupby_chunk`.
    aggregate_kwargs:
        Key-word arguments to pass to `aggregate_chunk`.
    """

    _parameters = [
        "frame",
        "by",
        "arg",
        "observed",
        "dropna",
        "split_every",
    ]
    _defaults = {
        "observed": None,
        "dropna": None,
        "split_every": 8,
    }

    @functools.cached_property
    def spec(self):
        # Converts the `arg` operand into specific
        # chunk, aggregate, and finalizer functions
        group_columns = set(self.by)
        non_group_columns = [
            col for col in self.frame.columns if col not in group_columns
        ]
        spec = _normalize_spec(self.arg, non_group_columns)

        # Median not supported yet
        has_median = any(s[1] in ("median", np.median) for s in spec)
        if has_median:
            raise NotImplementedError("median not yet supported")

        keys = ["chunk_funcs", "aggregate_funcs", "finalizers"]
        return dict(zip(keys, _build_agg_args(spec)))

    @classmethod
    def chunk(cls, df, by=None, **kwargs):
        return _groupby_apply_funcs(df, *by, **kwargs)

    @classmethod
    def combine(cls, inputs, **kwargs):
        return _groupby_apply_funcs(_concat(inputs), **kwargs)

    @classmethod
    def aggregate(cls, inputs, **kwargs):
        return _agg_finalize(_concat(inputs), **kwargs)

    @property
    def chunk_kwargs(self) -> dict:
        return {
            "funcs": self.spec["chunk_funcs"],
            "sort": False,
            "by": self.by,
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
        }

    @property
    def combine_kwargs(self) -> dict:
        return {
            "funcs": self.spec["aggregate_funcs"],
            "level": _determine_levels(self.by),
            "sort": False,
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
        }

    @property
    def aggregate_kwargs(self) -> dict:
        return {
            "aggregate_funcs": self.spec["aggregate_funcs"],
            "finalize_funcs": self.spec["finalizers"],
            "level": _determine_levels(self.by),
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
        }

    def _simplify_down(self):
        # Use agg-spec information to add column projection
        column_projection = None
        if isinstance(self.arg, dict):
            column_projection = (
                set(self.by).union(self.arg.keys()).intersection(self.frame.columns)
            )
        if column_projection and column_projection < set(self.frame.columns):
            return type(self)(self.frame[list(column_projection)], *self.operands[1:])


class Sum(SingleAggregation):
    groupby_chunk = M.sum


class Prod(SingleAggregation):
    groupby_chunk = M.prod


class Min(SingleAggregation):
    groupby_chunk = M.min


class Max(SingleAggregation):
    groupby_chunk = M.max


class First(SingleAggregation):
    groupby_chunk = M.first


class Last(SingleAggregation):
    groupby_chunk = M.last


class Count(SingleAggregation):
    groupby_chunk = M.count
    groupby_aggregate = M.sum


class Size(SingleAggregation):
    groupby_chunk = M.size
    groupby_aggregate = M.sum


class ValueCounts(SingleAggregation):
    groupby_chunk = staticmethod(_value_counts)
    groupby_aggregate = staticmethod(_value_counts_aggregate)


class Var(Reduction):
    _parameters = ["frame", "by", "ddof", "numeric_only"]
    reduction_aggregate = _var_agg
    reduction_combine = _var_combine

    def chunk(self, frame, **kwargs):
        return _var_chunk(frame, *self.by, **kwargs)

    @functools.cached_property
    def levels(self):
        return _determine_levels(self.by)

    @functools.cached_property
    def aggregate_kwargs(self):
        return {
            "ddof": self.ddof,
            "levels": self.levels,
            "numeric_only": self.numeric_only,
        }

    @functools.cached_property
    def chunk_kwargs(self):
        return {"numeric_only": self.numeric_only}

    @functools.cached_property
    def combine_kwargs(self):
        return {"levels": self.levels}

    def _divisions(self):
        return (None, None)

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            columns = sorted(set(parent.columns + self.by))
            if columns == self.frame.columns:
                return
            return type(parent)(
                type(self)(self.frame[columns], *self.operands[1:]),
                *parent.operands[1:],
            )


class Std(SingleAggregation):
    _parameters = ["frame", "by", "ddof", "numeric_only"]

    @functools.cached_property
    def _meta(self):
        return self._lower()._meta

    def _lower(self):
        v = Var(*self.operands)
        return MapPartitions(
            v,
            func=np.sqrt,
            meta=v._meta,
            enforce_metadata=True,
            transform_divisions=True,
            clear_divisions=True,
        )


class Mean(SingleAggregation):
    @functools.cached_property
    def _meta(self):
        return self._lower()._meta

    def _lower(self):
        s = Sum(*self.operands)
        # Drop chunk/aggregate_kwargs for count
        c = Count(
            *[
                self.operand(param)
                if param not in ("chunk_kwargs", "aggregate_kwargs")
                else {}
                for param in self._parameters
            ]
        )
        if is_dataframe_like(s._meta):
            c = c[s.columns]
        return s / c


###
### Groupby Collection API
###


class GroupBy:
    """Collection container for groupby aggregations

    The purpose of this class is to expose an API similar
    to Pandas' `Groupby` for dask-expr collections.

    See Also
    --------
    SingleAggregation
    """

    def __init__(
        self,
        obj,
        by,
        sort=None,
        observed=None,
        dropna=None,
        slice=None,
    ):
        if (
            isinstance(by, Series)
            and by.name in obj.columns
            and by._name == obj[by.name]._name
        ):
            by = by.name
        elif isinstance(by, Series):
            # TODO: Implement this
            raise ValueError("by must be in the DataFrames columns.")

        by_ = by if isinstance(by, (tuple, list)) else [by]
        self._slice = slice
        # Check if we can project columns
        projection = None
        if (
            np.isscalar(slice)
            or isinstance(slice, (str, list, tuple))
            or (
                (is_index_like(slice) or is_series_like(slice))
                and not is_dask_collection(slice)
            )
        ):
            projection = set(by_).union(
                {slice} if (np.isscalar(slice) or isinstance(slice, str)) else slice
            )
            projection = [c for c in obj.columns if c in projection]

        self.by = [by] if np.isscalar(by) else list(by)
        self.obj = obj[projection] if projection is not None else obj
        self.sort = sort
        self.observed = observed
        self.dropna = dropna

        if not isinstance(self.obj, DataFrame):
            raise NotImplementedError(
                "groupby only supports DataFrame collections for now."
            )

        for key in self.by:
            if not (np.isscalar(key) and key in self.obj.columns):
                raise NotImplementedError("Can only group on column names (for now).")

        if self.sort:
            raise NotImplementedError("sort=True not yet supported.")

    def _numeric_only_kwargs(self, numeric_only):
        kwargs = {"numeric_only": numeric_only}
        return {"chunk_kwargs": kwargs, "aggregate_kwargs": kwargs}

    def _single_agg(
        self, expr_cls, split_out=1, chunk_kwargs=None, aggregate_kwargs=None
    ):
        if split_out > 1:
            raise NotImplementedError("split_out>1 not yet supported")
        return new_collection(
            expr_cls(
                self.obj.expr,
                self.by,
                self.observed,
                self.dropna,
                chunk_kwargs=chunk_kwargs,
                aggregate_kwargs=aggregate_kwargs,
                _slice=self._slice,
            )
        )

    def _aca_agg(self, expr_cls, split_out=1, **kwargs):
        if split_out > 1:
            raise NotImplementedError("split_out>1 not yet supported")
        x = new_collection(
            expr_cls(
                self.obj.expr,
                by=self.by,
                **kwargs,
                # TODO: Add observed and dropna when supported in dask/dask
            )
        )
        return x

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e) from e

    def __getitem__(self, key):
        g = GroupBy(
            self.obj,
            by=self.by,
            slice=key,
            sort=self.sort,
            dropna=self.dropna,
            observed=self.observed,
        )
        return g

    def count(self, **kwargs):
        return self._single_agg(Count, **kwargs)

    def sum(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Sum, **kwargs, **numeric_kwargs)

    def prod(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Prod, **kwargs, **numeric_kwargs)

    def mean(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Mean, **kwargs, **numeric_kwargs)

    def min(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Min, **kwargs, **numeric_kwargs)

    def max(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Max, **kwargs, **numeric_kwargs)

    def first(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(First, **kwargs, **numeric_kwargs)

    def last(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Last, **kwargs, **numeric_kwargs)

    def size(self, **kwargs):
        return self._single_agg(Size, **kwargs)

    def value_counts(self, **kwargs):
        return self._single_agg(ValueCounts, **kwargs)

    def var(self, ddof=1, numeric_only=True):
        if not numeric_only:
            raise NotImplementedError("numeric_only=False is not implemented")
        return self._aca_agg(Var, ddof=ddof, numeric_only=numeric_only)

    def std(self, ddof=1, numeric_only=True):
        if not numeric_only:
            raise NotImplementedError("numeric_only=False is not implemented")
        return self._aca_agg(Std, ddof=ddof, numeric_only=numeric_only)

    def aggregate(self, arg=None, split_every=8, split_out=1):
        if arg is None:
            raise NotImplementedError("arg=None not supported")

        if split_out > 1:
            raise NotImplementedError("split_out>1 not yet supported")

        return new_collection(
            GroupbyAggregation(
                self.obj.expr,
                self.by,
                arg,
                self.observed,
                self.dropna,
                split_every,
            )
        )

    def agg(self, *args, **kwargs):
        return self.aggregate(*args, **kwargs)

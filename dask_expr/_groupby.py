import functools
import math
import warnings

import numpy as np
from dask import is_dask_collection
from dask.core import flatten
from dask.dataframe.core import (
    _concat,
    apply_and_enforce,
    is_dataframe_like,
    is_series_like,
)
from dask.dataframe.dispatch import concat, make_meta, meta_nonempty
from dask.dataframe.groupby import (
    _agg_finalize,
    _apply_chunk,
    _build_agg_args,
    _cov_agg,
    _cov_chunk,
    _cov_combine,
    _determine_levels,
    _groupby_aggregate,
    _groupby_apply_funcs,
    _groupby_get_group,
    _groupby_slice_apply,
    _groupby_slice_shift,
    _groupby_slice_transform,
    _head_aggregate,
    _head_chunk,
    _normalize_spec,
    _nunique_df_chunk,
    _nunique_df_combine,
    _tail_aggregate,
    _tail_chunk,
    _value_counts,
    _value_counts_aggregate,
    _var_agg,
    _var_chunk,
)
from dask.utils import M, apply, is_index_like

from dask_expr._collection import FrameBase, Index, Series, new_collection
from dask_expr._expr import (
    Assign,
    Blockwise,
    Expr,
    MapPartitions,
    Projection,
    RenameFrame,
    RenameSeries,
    ToFrame,
    are_co_aligned,
    determine_column_projection,
    no_default,
)
from dask_expr._reductions import ApplyConcatApply, Chunk, Reduction
from dask_expr._shuffle import Shuffle
from dask_expr._util import is_scalar


def _as_dict(key, value):
    # Utility to convert a single kwarg to a dict.
    # The dict will be empty if the value is None
    return {} if value is None else {key: value}


def _adjust_split_out_for_group_keys(npartitions, by):
    return math.ceil(npartitions / (100 / (len(by) - 1)))


###
### Groupby-aggregation expressions
###


class GroupByBase:
    @functools.cached_property
    def _by_meta(self):
        return [meta_nonempty(x._meta) if isinstance(x, Expr) else x for x in self.by]

    @functools.cached_property
    def _by_columns(self):
        return [x for x in self.by if not isinstance(x, Expr)]

    @property
    def split_by(self):
        return list(
            flatten(
                [[x] if not isinstance(x, Expr) else x.columns for x in self.by],
                container=list,
            )
        )

    @functools.cached_property
    def by(self):
        return self.operands[len(self._parameters) :]

    @functools.cached_property
    def levels(self):
        return _determine_levels(self.by)

    @property
    def shuffle_by_index(self):
        return True


class GroupByChunk(Chunk, GroupByBase):
    @functools.cached_property
    def _args(self) -> list:
        return [self.frame] + self.by

    @functools.cached_property
    def _meta(self):
        args = [
            meta_nonempty(op._meta) if isinstance(op, Expr) else op for op in self._args
        ]
        return make_meta(self.operation(*args, **self._kwargs))


class GroupByApplyConcatApply(ApplyConcatApply, GroupByBase):
    _chunk_cls = GroupByChunk

    @functools.cached_property
    def _meta_chunk(self):
        meta = meta_nonempty(self.frame._meta)
        return self.chunk(meta, *self._by_meta, **self.chunk_kwargs)

    @property
    def _chunk_cls_args(self):
        return self.by

    @property
    def split_out(self):
        if self.operand("split_out") is None:
            return 1
        return super().split_out

    @property
    def _projection_columns(self):
        return self.frame.columns

    def _tune_down(self):
        if len(self.by) > 1 and self.operand("split_out") is None:
            return self.substitute_parameters(
                {
                    "split_out": functools.partial(
                        _adjust_split_out_for_group_keys, by=self.by
                    )
                }
            )


class SingleAggregation(GroupByApplyConcatApply, GroupByBase):
    """Single groupby aggregation

    This is an abstract class. Sub-classes must implement
    the following methods:

    -   `groupby_chunk`: Applied to each group within
        the `chunk` method of `GroupByApplyConcatApply`
    -   `groupby_aggregate`: Applied to each group within
        the `aggregate` method of `GroupByApplyConcatApply`

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
        "observed",
        "dropna",
        "chunk_kwargs",
        "aggregate_kwargs",
        "_slice",
        "split_every",
        "split_out",
        "sort",
    ]
    _defaults = {
        "observed": None,
        "dropna": None,
        "chunk_kwargs": None,
        "aggregate_kwargs": None,
        "_slice": None,
        "split_every": 8,
        "split_out": None,
        "sort": None,
    }

    groupby_chunk = None
    groupby_aggregate = None

    @classmethod
    def chunk(cls, df, *by, **kwargs):
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
            "levels": self.levels,
            "sort": self.sort,
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
            **aggregate_kwargs,
        }

    def _simplify_up(self, parent, dependents):
        return groupby_projection(self, parent, dependents)


class GroupbyAggregation(GroupByApplyConcatApply, GroupByBase):
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
        "arg",
        "observed",
        "dropna",
        "split_every",
        "split_out",
        "sort",
        "_slice",
    ]
    _defaults = {
        "observed": None,
        "dropna": None,
        "split_every": 8,
        "split_out": 1,
        "sort": None,
        "_slice": None,
    }
    chunk = staticmethod(_groupby_apply_funcs)

    @functools.cached_property
    def spec(self):
        # Converts the `arg` operand into specific
        # chunk, aggregate, and finalizer functions
        if is_dataframe_like(self.frame._meta):
            group_columns = self._by_columns
            if self._slice:
                non_group_columns = self._slice
                if is_scalar(non_group_columns):
                    non_group_columns = [non_group_columns]
            else:
                non_group_columns = [
                    col for col in self.frame.columns if col not in group_columns
                ]
            spec = _normalize_spec(self.arg, non_group_columns)
        elif is_series_like(self.frame._meta):
            if isinstance(self.arg, (list, tuple, dict)):
                spec = _normalize_spec({None: self.arg}, [])
                spec = [
                    (result_column, func, input_column)
                    for ((_, result_column), func, input_column) in spec
                ]

            else:
                spec = _normalize_spec({None: self.arg}, [])
                spec = [
                    (self.frame.columns[0], func, input_column)
                    for (_, func, input_column) in spec
                ]
        else:
            raise ValueError(f"aggregate on unknown object {self.frame._meta}")

        # Median not supported yet
        has_median = any(s[1] in ("median", np.median) for s in spec)
        if has_median:
            raise NotImplementedError("median not yet supported")

        keys = ["chunk_funcs", "aggregate_funcs", "finalizers"]
        return dict(zip(keys, _build_agg_args(spec)))

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
            "sort": self.sort,
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
        }

    @property
    def combine_kwargs(self) -> dict:
        return {
            "funcs": self.spec["aggregate_funcs"],
            "level": self.levels,
            "sort": self.sort,
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
        }

    @property
    def aggregate_kwargs(self) -> dict:
        return {
            "aggregate_funcs": self.spec["aggregate_funcs"],
            "finalize_funcs": self.spec["finalizers"],
            "level": self.levels,
            "sort": self.sort,
            **_as_dict("observed", self.observed),
            **_as_dict("dropna", self.dropna),
        }

    def _simplify_down(self):
        # Use agg-spec information to add column projection
        column_projection = None
        if isinstance(self.arg, dict):
            column_projection = (
                set(self._by_columns)
                .union(self.arg.keys())
                .intersection(self.frame.columns)
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


class IdxMin(SingleAggregation):
    groupby_chunk = M.idxmin
    groupby_aggregate = M.first


class IdxMax(IdxMin):
    groupby_chunk = M.idxmax
    groupby_aggregate = M.first


class ValueCounts(SingleAggregation):
    groupby_chunk = staticmethod(_value_counts)
    groupby_aggregate = staticmethod(_value_counts_aggregate)


class Cov(SingleAggregation):
    chunk = staticmethod(_cov_chunk)
    combine = staticmethod(_cov_combine)
    std = False

    @classmethod
    def aggregate(cls, inputs, **kwargs):
        return _cov_agg(inputs[0], **kwargs)

    @property
    def chunk_kwargs(self) -> dict:
        return self.operand("chunk_kwargs")

    @property
    def aggregate_kwargs(self) -> dict:
        kwargs = self.operand("aggregate_kwargs").copy()
        kwargs["sort"] = self.sort
        kwargs["std"] = self.std
        kwargs["levels"] = self.levels
        return kwargs

    @property
    def combine_kwargs(self) -> dict:
        return {"levels": self.levels}


class Corr(Cov):
    std = True


class GroupByReduction(Reduction, GroupByBase):
    _chunk_cls = GroupByChunk

    @property
    def _chunk_cls_args(self):
        return self.by

    @functools.cached_property
    def _meta_chunk(self):
        meta = meta_nonempty(self.frame._meta)
        return self.chunk(meta, *self._by_meta, **self.chunk_kwargs)


def _var_combine(g, levels, sort=False, observed=False, dropna=True):
    return g.groupby(level=levels, sort=sort, observed=observed, dropna=dropna).sum()


class Var(GroupByReduction):
    _parameters = [
        "frame",
        "ddof",
        "numeric_only",
        "split_out",
        "split_every",
        "sort",
        "dropna",
        "observed",
    ]
    _defaults = {
        "split_out": 1,
        "sort": None,
        "observed": None,
        "dropna": None,
        "split_every": None,
    }
    reduction_aggregate = _var_agg
    reduction_combine = _var_combine

    @staticmethod
    def chunk(frame, *by, **kwargs):
        return _var_chunk(frame, *by, **kwargs)

    @functools.cached_property
    def aggregate_kwargs(self):
        return {
            "ddof": self.ddof,
            "levels": self.levels,
            "numeric_only": self.numeric_only,
            "sort": self.sort,
            "observed": self.observed,
            "dropna": self.dropna,
        }

    @functools.cached_property
    def chunk_kwargs(self):
        return {
            "numeric_only": self.numeric_only,
            "observed": self.observed,
            "dropna": self.dropna,
        }

    @functools.cached_property
    def combine_kwargs(self):
        return {"levels": self.levels, "observed": self.observed, "dropna": self.dropna}

    def _divisions(self):
        if self.sort:
            return (None, None)
        return (None,) * (self.split_out + 1)

    def _simplify_up(self, parent, dependents):
        return groupby_projection(self, parent, dependents)


class Std(SingleAggregation):
    _parameters = [
        "frame",
        "ddof",
        "numeric_only",
        "split_out",
        "split_every",
        "sort",
        "dropna",
        "observed",
    ]
    _defaults = {"split_out": 1, "sort": None, "split_every": None}

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
            ],
            *self.by,
        )
        if is_dataframe_like(s._meta):
            c = c[s.columns]
        return s / c


def nunique_df_combine(dfs, *args, **kwargs):
    return _nunique_df_combine(concat(dfs), *args, **kwargs)


def nunique_df_aggregate(dfs, levels, name, sort=False):
    df = concat(dfs)
    if df.ndim == 1:
        # split out reduces to a Series
        return df.groupby(level=levels, sort=sort, observed=True).nunique()
    else:
        return df.groupby(level=levels, sort=sort, observed=True)[name].nunique()


class NUnique(SingleAggregation):
    aggregate = staticmethod(nunique_df_aggregate)
    combine = staticmethod(nunique_df_combine)

    @staticmethod
    def chunk(df, *by, **kwargs):
        if df.ndim == 1:
            df = df.to_frame()
            kwargs = dict(name=df.columns[0], levels=_determine_levels(by))
        return _nunique_df_chunk(df, *by, **kwargs)

    @functools.cached_property
    def chunk_kwargs(self) -> dict:
        kwargs = super().chunk_kwargs
        kwargs["name"] = self._slice
        return kwargs

    @functools.cached_property
    def aggregate_kwargs(self) -> dict:
        return {"levels": self.levels, "name": self._slice}

    @functools.cached_property
    def combine_kwargs(self):
        return {"levels": self.levels}


class Head(SingleAggregation):
    groupby_chunk = staticmethod(_head_chunk)
    groupby_aggregate = staticmethod(_head_aggregate)

    @classmethod
    def combine(cls, inputs, **kwargs):
        return _concat(inputs)


class Tail(Head):
    groupby_chunk = staticmethod(_tail_chunk)
    groupby_aggregate = staticmethod(_tail_aggregate)


class GroupByApply(Expr, GroupByBase):
    _parameters = [
        "frame",
        "observed",
        "dropna",
        "_slice",
        "group_keys",
        "func",
        "meta",
        "args",
        "kwargs",
    ]
    _defaults = {"observed": None, "dropna": None, "_slice": None, "group_keys": True}

    @functools.cached_property
    def grp_func(self):
        return functools.partial(_groupby_slice_apply, func=self.func)

    @functools.cached_property
    def _meta(self):
        if self.operand("meta") is not no_default:
            return make_meta(self.operand("meta"), parent_meta=self.frame._meta)
        return _meta_apply_transform(self, self.grp_func)

    def _divisions(self):
        if self.need_to_shuffle:
            return (None,) * (self.frame.npartitions + 1)
        return self.frame.divisions

    def _shuffle_grp_func(self, shuffled=False):
        return self.grp_func

    @property
    def need_to_shuffle(self):
        return any(div is None for div in self.frame.divisions) or not any(
            _contains_index_name(self.frame._meta.index.name, b) for b in self.by
        )

    def _lower(self):
        df = self.frame
        by = self.by

        if self.need_to_shuffle:

            def get_map_columns(df):
                map_columns = {col: str(col) for col in df.columns if col != str(col)}
                unmap_columns = {v: k for k, v in map_columns.items()}
                return map_columns, unmap_columns

            # Map Tuple[str] column names to str before the shuffle

            if any(isinstance(b, Expr) for b in self.by):
                # TODO: Simplify after multi column assign
                is_series = df.ndim == 1
                if is_series:
                    df = ToFrame(df)
                cols = []
                for i, b in enumerate(self.by):
                    if isinstance(b, Expr):
                        df = Assign(df, f"_by_{i}", b)
                        cols.append(f"_by_{i}")

                map_columns, unmap_columns = get_map_columns(df)
                if map_columns:
                    df = RenameFrame(df, map_columns)

                df = Shuffle(df, [map_columns.get(c, c) for c in cols], df.npartitions)

                if unmap_columns:
                    df = RenameFrame(df, unmap_columns)

                by = [
                    b
                    if not isinstance(b, Expr)
                    else RenameSeries(
                        Projection(df, f"_by_{i}"), index=self.by[i].columns[0]
                    )
                    for i, b in enumerate(self.by)
                ]
                cols = [col for col in df.columns if col not in cols]
                if is_series:
                    cols = cols[0]
                df = Projection(df, cols)
            else:
                map_columns, unmap_columns = get_map_columns(df)
                if map_columns:
                    df = RenameFrame(df, map_columns)
                df = Shuffle(
                    df, map_columns.get(self.by[0], self.by[0]), df.npartitions
                )

                if unmap_columns:
                    df = RenameFrame(df, unmap_columns)

            grp_func = self._shuffle_grp_func(True)
        else:
            grp_func = self._shuffle_grp_func(False)

        return GroupByUDFBlockwise(
            df,
            self._slice,
            self.group_keys,
            self.observed,
            self.dropna,
            self.operand("args"),
            self.operand("kwargs"),
            grp_func,
            self.operand("meta"),
            *by,
        )


class GroupByTransform(GroupByApply):
    @functools.cached_property
    def grp_func(self):
        return functools.partial(_groupby_slice_transform, func=self.func)


def _fillna(group, *, what, **kwargs):
    return getattr(group, what)(**kwargs)


class GroupByBFill(GroupByTransform):
    func = staticmethod(functools.partial(_fillna, what="bfill"))

    def _simplify_up(self, parent, dependents):
        if isinstance(parent, Projection):
            return groupby_projection(self, parent, dependents)


class GroupByFFill(GroupByBFill):
    func = staticmethod(functools.partial(_fillna, what="ffill"))


class GroupByShift(GroupByApply):
    _defaults = {
        "observed": None,
        "dropna": None,
        "_slice": None,
        "func": None,
        "group_keys": True,
    }

    @functools.cached_property
    def grp_func(self):
        return functools.partial(_groupby_slice_shift, shuffled=False)

    def _shuffle_grp_func(self, shuffled=False):
        return functools.partial(_groupby_slice_shift, shuffled=shuffled)


class Median(GroupByShift):
    @functools.cached_property
    def grp_func(self):
        return functools.partial(_median_groupby_aggregate)

    def _shuffle_grp_func(self, shuffled=False):
        return self.grp_func

    def _simplify_up(self, parent, dependents):
        if isinstance(parent, Projection):
            return groupby_projection(self, parent, dependents)


class GetGroup(Blockwise, GroupByBase):
    operation = staticmethod(_groupby_get_group)
    _parameters = ["frame", "get_key", "columns"]
    _keyword_only = ["get_key", "columns"]

    @property
    def _args(self) -> list:
        return [self.frame] + self.by

    @property
    def _kwargs(self) -> dict:
        cols = self.operand("columns")
        return {
            "get_key": self.get_key,
            "columns": cols if cols is not None else self.frame.columns,
        }


def _median_groupby_aggregate(
    df,
    by=None,
    key=None,
    group_keys=True,  # not used
    dropna=None,
    observed=None,
    numeric_only=False,
    *args,
    **kwargs,
):
    dropna = {"dropna": dropna} if dropna is not None else {}
    observed = {"observed": observed} if observed is not None else {}

    g = df.groupby(by=by, **observed, **dropna)
    if key is not None:
        g = g[key]
    return g.median(numeric_only=numeric_only)


class GroupByUDFBlockwise(Blockwise, GroupByBase):
    _parameters = [
        "frame",
        "_slice",
        "group_keys",
        "observed",
        "dropna",
        "args",
        "kwargs",
        "dask_func",
        "meta",
    ]
    _defaults = {"observed": None, "dropna": None}
    _keyword_only = [
        "_slice",
        "group_keys",
        "observed",
        "dropna",
        "args",
        "kwargs",
        "dask_func",
    ]

    @property
    def _args(self) -> list:
        return [self.frame] + self.by

    @functools.cached_property
    def _meta(self):
        if self.operand("meta") is not no_default:
            return make_meta(self.operand("meta"), parent_meta=self.frame._meta)
        return _meta_apply_transform(self, self.dask_func)

    def _task(self, index: int):
        args = [self._blockwise_arg(op, index) for op in self._args]
        kwargs = self._kwargs.copy()
        kwargs.update(
            {
                "_func": self.operation,
                "_meta": self._meta,
            }
        )
        return (apply, apply_and_enforce, args, kwargs)

    @staticmethod
    def operation(
        frame,
        *by,
        _slice,
        group_keys=None,
        observed=None,
        dropna=None,
        args=None,
        kwargs=None,
        dask_func=None,
    ):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        return dask_func(
            frame,
            list(by),
            key=_slice,
            group_keys=group_keys,
            *args,
            **_as_dict("observed", observed),
            **_as_dict("dropna", dropna),
            **kwargs,
        )


def _contains_index_name(index_name, by):
    if index_name is None:
        return False

    if isinstance(by, Expr):
        return False

    if not is_scalar(by):
        return False

    return index_name == by


def _meta_apply_transform(obj, grp_func):
    kwargs = obj.operand("kwargs")
    by_meta = obj._by_meta
    by_meta = [x if is_scalar(x) else meta_nonempty(x) for x in by_meta]
    meta_args, meta_kwargs = _extract_meta((obj.operand("args"), kwargs), nonempty=True)
    return make_meta(
        grp_func(
            meta_nonempty(obj.frame._meta),
            by_meta,
            key=obj._slice,
            *meta_args,
            **_as_dict("observed", obj.observed),
            **_as_dict("dropna", obj.dropna),
            **_as_dict("group_keys", obj.group_keys),
            **meta_kwargs,
        )
    )


def _extract_meta(x, nonempty=False):
    """
    Extract internal cache data (``_meta``) from dd.DataFrame / dd.Series
    """
    if isinstance(x, Expr):
        return meta_nonempty(x._meta) if nonempty else x._meta
    elif isinstance(x, list):
        return [_extract_meta(_x, nonempty) for _x in x]
    elif isinstance(x, tuple):
        return tuple(_extract_meta(_x, nonempty) for _x in x)
    elif isinstance(x, dict):
        res = {}
        for k in x:
            res[k] = _extract_meta(x[k], nonempty)
        return res
    else:
        return x


def groupby_projection(expr, parent, dependents):
    if isinstance(parent, Projection):
        columns = determine_column_projection(
            expr, parent, dependents, additional_columns=expr._by_columns
        )
        if columns == expr.frame.columns:
            return
        columns = [col for col in expr.frame.columns if col in columns]
        return type(parent)(
            type(expr)(expr.frame[columns], *expr.operands[1:]),
            *parent.operands[1:],
        )
    return


###
### Groupby Collection API
###


def _clean_by_expr(obj, by):
    if (
        isinstance(by, Series)
        and by.name in obj.columns
        and by._name == obj[by.name]._name
    ):
        return by.name
    elif isinstance(by, Index) and by._name == obj.index._name:
        return by.expr
    elif isinstance(by, Series):
        if not are_co_aligned(obj.expr, by.expr):
            raise NotImplementedError("by must be in the DataFrames columns.")
        return by.expr

    # By is a column name, e.g. str or int
    return by


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
        group_keys=True,
        sort=None,
        observed=None,
        dropna=None,
        slice=None,
    ):
        if isinstance(by, (tuple, list)):
            by = [_clean_by_expr(obj, x) for x in by]
        else:
            by = _clean_by_expr(obj, by)

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

        self.obj = obj[projection] if projection is not None else obj
        self.sort = sort
        self.observed = observed if observed is not None else False
        self.dropna = dropna
        self.group_keys = group_keys
        self.by = [by] if np.isscalar(by) or isinstance(by, Expr) else list(by)
        # surface pandas errors
        self._meta = self.obj._meta.groupby(
            by,
            group_keys=group_keys,
            sort=sort,
            **_as_dict("observed", observed),
            **_as_dict("dropna", dropna),
        )
        if slice is not None:
            if isinstance(slice, tuple):
                slice = list(slice)
            self._meta = self._meta[slice]

    def _numeric_only_kwargs(self, numeric_only):
        kwargs = {"numeric_only": numeric_only}
        return {"chunk_kwargs": kwargs.copy(), "aggregate_kwargs": kwargs.copy()}

    def _single_agg(
        self,
        expr_cls,
        split_every=None,
        split_out=None,
        chunk_kwargs=None,
        aggregate_kwargs=None,
    ):
        if split_every is None:
            split_every = 8
        return new_collection(
            expr_cls(
                self.obj.expr,
                self.observed,
                self.dropna,
                chunk_kwargs,
                aggregate_kwargs,
                self._slice,
                split_every,
                split_out,
                self.sort,
                *self.by,
            )
        )

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e) from e

    def __dir__(self):
        return sorted(
            set(
                dir(type(self))
                + list(self.__dict__)
                + list(filter(M.isidentifier, self.obj.columns))
            )
        )

    def compute(self, **kwargs):
        raise NotImplementedError(
            "DataFrameGroupBy does not allow compute method."
            "Please chain it with an aggregation method (like ``.mean()``) or get a "
            "specific group using ``.get_group()`` before calling ``compute()``"
        )

    def __getitem__(self, key):
        if is_scalar(key):
            return SeriesGroupBy(
                self.obj,
                by=self.by,
                slice=key,
                sort=self.sort,
                dropna=self.dropna,
                observed=self.observed,
            )
        g = GroupBy(
            self.obj,
            by=self.by,
            slice=key,
            sort=self.sort,
            dropna=self.dropna,
            observed=self.observed,
            group_keys=self.group_keys,
        )
        return g

    def get_group(self, key):
        return new_collection(GetGroup(self.obj.expr, key, self._slice, *self.by))

    def count(self, **kwargs):
        return self._single_agg(Count, **kwargs)

    def sum(self, numeric_only=False, min_count=None, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        result = self._single_agg(Sum, **kwargs, **numeric_kwargs)
        if min_count:
            return result.where(self.count() >= min_count, other=np.nan)
        return result

    def prod(self, numeric_only=False, min_count=None, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        result = self._single_agg(Prod, **kwargs, **numeric_kwargs)
        if min_count:
            return result.where(self.count() >= min_count, other=np.nan)
        return result

    def _all_numeric(self):
        """Are all columns that we're not grouping on numeric?"""
        numerics = self.obj._meta._get_numeric_data()
        # This computes a groupby but only on the empty meta
        post_group_columns = self._meta.count().columns
        return len(set(post_group_columns) - set(numerics.columns)) == 0

    def mean(self, numeric_only=False, **kwargs):
        if not numeric_only and not self._all_numeric():
            raise NotImplementedError(
                "'numeric_only=False' is not implemented in Dask."
            )
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Mean, **kwargs, **numeric_kwargs)

    def min(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Min, **kwargs, **numeric_kwargs)

    def max(self, numeric_only=False, **kwargs):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Max, **kwargs, **numeric_kwargs)

    def first(self, numeric_only=False, sort=None, **kwargs):
        if sort:
            raise NotImplementedError()
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(First, **kwargs, **numeric_kwargs)

    def cov(self, ddof=1, split_every=None, split_out=1, numeric_only=False):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(
            Cov,
            split_every,
            split_out,
            chunk_kwargs=numeric_kwargs["chunk_kwargs"],
            aggregate_kwargs={"ddof": ddof},
        )

    def corr(self, split_every=None, split_out=1, numeric_only=False):
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(
            Corr,
            split_every,
            split_out,
            chunk_kwargs=numeric_kwargs["chunk_kwargs"],
            aggregate_kwargs={"ddof": 1},
        )

    def last(self, numeric_only=False, sort=None, **kwargs):
        if sort:
            raise NotImplementedError()
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        return self._single_agg(Last, **kwargs, **numeric_kwargs)

    def ffill(self, limit=None):
        return self._transform_like_op(GroupByFFill, None, limit=limit)

    def bfill(self, limit=None):
        return self._transform_like_op(GroupByBFill, None, limit=limit)

    def size(self, **kwargs):
        return self._single_agg(Size, **kwargs)

    def value_counts(self, **kwargs):
        return self._single_agg(ValueCounts, **kwargs)

    def idxmin(
        self, split_every=None, split_out=1, skipna=True, numeric_only=False, **kwargs
    ):
        # TODO: Add shuffle and remove kwargs
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        numeric_kwargs["chunk_kwargs"]["skipna"] = skipna
        if "axis" in kwargs:
            raise NotImplementedError("axis is not supported")
        return self._single_agg(
            IdxMin, split_every=split_every, split_out=split_out, **numeric_kwargs
        )

    def idxmax(
        self, split_every=None, split_out=1, skipna=True, numeric_only=False, **kwargs
    ):
        # TODO: Add shuffle and remove kwargs
        numeric_kwargs = self._numeric_only_kwargs(numeric_only)
        numeric_kwargs["chunk_kwargs"]["skipna"] = skipna
        if "axis" in kwargs:
            raise NotImplementedError("axis is not supported")
        return self._single_agg(
            IdxMax, split_every=split_every, split_out=split_out, **numeric_kwargs
        )

    def head(self, n=5, split_every=None, split_out=1):
        chunk_kwargs = {"n": n}
        aggregate_kwargs = {
            "n": n,
            "index_levels": len(self.by) if not isinstance(self.by, Expr) else 1,
        }
        return self._single_agg(
            Head,
            split_every=split_every,
            split_out=split_out,
            chunk_kwargs=chunk_kwargs,
            aggregate_kwargs=aggregate_kwargs,
        )

    def tail(self, n=5, split_every=None, split_out=1):
        chunk_kwargs = {"n": n}
        aggregate_kwargs = {
            "n": n,
            "index_levels": len(self.by) if not isinstance(self.by, Expr) else 1,
        }
        return self._single_agg(
            Tail,
            split_every=split_every,
            split_out=split_out,
            chunk_kwargs=chunk_kwargs,
            aggregate_kwargs=aggregate_kwargs,
        )

    def var(self, ddof=1, split_every=None, split_out=1, numeric_only=False):
        if not numeric_only and not self._all_numeric():
            raise NotImplementedError(
                "'numeric_only=False' is not implemented in Dask."
            )
        result = new_collection(
            Var(
                self.obj.expr,
                ddof,
                numeric_only,
                split_out,
                split_every,
                self.sort,
                self.dropna,
                self.observed,
                *self.by,
            )
        )
        if (
            isinstance(self.obj, Series)
            or is_scalar(self._slice)
            and self._slice is not None
        ):
            result = result[result.columns[0]]
        return result

    def std(self, ddof=1, split_every=None, split_out=1, numeric_only=False):
        if not numeric_only and not self._all_numeric():
            raise NotImplementedError(
                "'numeric_only=False' is not implemented in Dask."
            )
        result = new_collection(
            Std(
                self.obj.expr,
                ddof,
                numeric_only,
                split_out,
                split_every,
                self.sort,
                self.dropna,
                self.observed,
                *self.by,
            )
        )
        if (
            isinstance(self.obj, Series)
            or is_scalar(self._slice)
            and self._slice is not None
        ):
            result = result[result.columns[0]]
        return result

    def aggregate(self, arg=None, split_every=8, split_out=1, **kwargs):
        if arg is None:
            raise NotImplementedError("arg=None not supported")

        if arg == "size":
            return self.size()

        return new_collection(
            GroupbyAggregation(
                self.obj.expr,
                arg,
                self.observed,
                self.dropna,
                split_every,
                split_out,
                self.sort,
                self._slice,
                *self.by,
            )
        )

    def agg(self, *args, **kwargs):
        return self.aggregate(*args, **kwargs)

    def _warn_if_no_meta(self, meta):
        if meta is no_default:
            msg = (
                "`meta` is not specified, inferred from partial data. "
                "Please provide `meta` if the result is unexpected.\n"
                "  Before: .apply(func)\n"
                "  After:  .apply(func, meta={'x': 'f8', 'y': 'f8'}) for dataframe result\n"
                "  or:     .apply(func, meta=('x', 'f8'))            for series result"
            )
            warnings.warn(msg, stacklevel=3)

    def apply(self, func, meta=no_default, *args, **kwargs):
        self._warn_if_no_meta(meta)
        return new_collection(
            GroupByApply(
                self.obj.expr,
                self.observed,
                self.dropna,
                self._slice,
                self.group_keys,
                func,
                meta,
                args,
                kwargs,
                *self.by,
            )
        )

    def _transform_like_op(self, expr_cls, func, meta=no_default, *args, **kwargs):
        return new_collection(
            expr_cls(
                self.obj.expr,
                self.observed,
                self.dropna,
                self._slice,
                self.group_keys,
                func,
                meta,
                args,
                kwargs,
                *self.by,
            )
        )

    def transform(self, func, meta=no_default, *args, **kwargs):
        self._warn_if_no_meta(meta)
        return self._transform_like_op(GroupByTransform, func, meta, *args, **kwargs)

    def shift(self, periods=1, meta=no_default, *args, **kwargs):
        self._warn_if_no_meta(meta)
        kwargs = {"periods": periods, **kwargs}
        return self._transform_like_op(GroupByShift, None, meta, *args, **kwargs)

    def median(
        self, split_every=None, split_out=True, shuffle_method=None, numeric_only=False
    ):
        result = new_collection(
            Median(
                self.obj.expr,
                self.observed,
                self.dropna,
                self._slice,
                self.group_keys,
                None,
                no_default,
                (),
                {"numeric_only": numeric_only},
                *self.by,
            )
        )
        if split_out is not True:
            result = result.repartition(npartitions=split_out)
        return result

    def rolling(self, window, min_periods=None, center=False, win_type=None, axis=0):
        from dask_expr._rolling import Rolling

        return Rolling(
            self.obj,
            window,
            min_periods=min_periods,
            center=center,
            win_type=win_type,
            groupby_kwargs={
                "by": self.by,
                "sort": self.sort,
                "observed": self.observed,
                "dropna": self.dropna,
                "group_keys": self.group_keys,
            },
            groupby_slice=self._slice,
        )


class SeriesGroupBy(GroupBy):
    def __init__(
        self,
        obj,
        by,
        sort=None,
        observed=None,
        dropna=None,
        slice=None,
    ):
        # Raise pandas errors if applicable
        if isinstance(obj, Series):
            if isinstance(by, FrameBase):
                obj._meta.groupby(by._meta, **_as_dict("observed", observed))
            elif isinstance(by, (list, tuple)) and any(
                isinstance(x, FrameBase) for x in by
            ):
                metas = [x._meta if isinstance(x, FrameBase) else x for x in by]
                obj._meta.groupby(metas, **_as_dict("observed", observed))
            elif isinstance(by, list):
                if len(by) == 0:
                    raise ValueError("No group keys passed!")

                non_series_items = [item for item in by if not isinstance(item, Series)]
                obj._meta.groupby(non_series_items, **_as_dict("observed", observed))
            else:
                obj._meta.groupby(by, **_as_dict("observed", observed))

        super().__init__(
            obj, by=by, slice=slice, observed=observed, dropna=dropna, sort=sort
        )

    def aggregate(self, arg=None, split_every=8, split_out=1, **kwargs):
        result = super().aggregate(
            arg=arg, split_every=split_every, split_out=split_out
        )
        if self._slice:
            try:
                result = result[self._slice]
            except KeyError:
                pass

        if (
            arg is not None
            and not isinstance(arg, (list, dict))
            and is_dataframe_like(result._meta)
        ):
            result = result[result.columns[0]]

        return result

    agg = aggregate

    def idxmin(
        self, split_every=None, split_out=1, skipna=True, numeric_only=False, **kwargs
    ):
        # pandas doesn't support numeric_only here, which is odd
        return self._single_agg(
            IdxMin,
            split_every=None,
            split_out=split_out,
            chunk_kwargs=dict(skipna=skipna),
        )

    def idxmax(
        self, split_every=None, split_out=1, skipna=True, numeric_only=False, **kwargs
    ):
        # pandas doesn't support numeric_only here, which is odd
        return self._single_agg(
            IdxMax,
            split_every=split_every,
            split_out=split_out,
            chunk_kwargs=dict(skipna=skipna),
        )

    def nunique(self, split_every=None, split_out=True):
        slice = self._slice or self.obj.name
        return new_collection(
            NUnique(
                self.obj.expr,
                self.observed,
                self.dropna,
                None,
                None,
                slice,
                split_every,
                split_out,
                self.sort,
                *self.by,
            )
        )

    def cov(self, *args, **kwargs):
        raise NotImplementedError("cov is not implemented for SeriesGroupBy objects.")

    def corr(self, *args, **kwargs):
        raise NotImplementedError("cov is not implemented for SeriesGroupBy objects.")

    def _all_numeric(self):
        return True

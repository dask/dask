from __future__ import annotations

import functools
import warnings
from collections.abc import Callable, Hashable, Mapping
from numbers import Number
from typing import Any, Literal

import numpy as np
import pandas as pd
from dask.base import DaskMethodsMixin, is_dask_collection, named_schedulers
from dask.dataframe import methods
from dask.dataframe.accessor import CachedAccessor
from dask.dataframe.core import (
    _concat,
    _Frame,
    check_divisions,
    is_dataframe_like,
    is_index_like,
    is_series_like,
    new_dd_object,
)
from dask.dataframe.dispatch import meta_nonempty
from dask.dataframe.utils import has_known_categories
from dask.utils import IndexCallable, random_state_data, typename
from fsspec.utils import stringify_path
from tlz import first

from dask_expr import _expr as expr
from dask_expr._align import AlignPartitions
from dask_expr._categorical import CategoricalAccessor
from dask_expr._concat import Concat
from dask_expr._expr import Eval, no_default
from dask_expr._merge import JoinRecursive, Merge
from dask_expr._quantiles import RepartitionQuantiles
from dask_expr._reductions import (
    DropDuplicates,
    Len,
    MemoryUsageFrame,
    MemoryUsageIndex,
    NLargest,
    NSmallest,
    PivotTable,
    Unique,
    ValueCounts,
)
from dask_expr._repartition import Repartition
from dask_expr._shuffle import SetIndex, SetIndexBlockwise, SortValues
from dask_expr._util import _convert_to_list, is_scalar

#
# Utilities to wrap Expr API
# (Helps limit boiler-plate code in collection APIs)
#


def _wrap_expr_api(*args, wrap_api=None, **kwargs):
    # Use Expr API, but convert to/from Expr objects
    assert wrap_api is not None
    result = wrap_api(
        *[arg.expr if isinstance(arg, FrameBase) else arg for arg in args],
        **kwargs,
    )
    if isinstance(result, expr.Expr):
        return new_collection(result)
    return result


def _wrap_expr_op(self, other, op=None):
    # Wrap expr operator
    assert op is not None
    if isinstance(other, FrameBase):
        other = other.expr

    if not isinstance(other, expr.Expr):
        return new_collection(getattr(self.expr, op)(other))
    elif expr.are_co_aligned(self.expr, other):
        return new_collection(getattr(self.expr, op)(other))
    else:
        return new_collection(
            getattr(AlignPartitions(self.expr, other), op)(
                AlignPartitions(other, self.expr)
            )
        )


def _wrap_unary_expr_op(self, op=None):
    # Wrap expr operator
    assert op is not None
    return new_collection(getattr(self.expr, op)())


#
# Collection classes
#


class FrameBase(DaskMethodsMixin):
    """Base class for Expr-backed Collections"""

    __dask_scheduler__ = staticmethod(
        named_schedulers.get("threads", named_schedulers["sync"])
    )
    __dask_optimize__ = staticmethod(lambda dsk, keys, **kwargs: dsk)

    def __init__(self, expr):
        self._expr = expr

    @property
    def expr(self) -> expr.Expr:
        return self._expr

    @property
    def _meta(self):
        return self.expr._meta

    @property
    def size(self):
        return new_collection(self.expr.size)

    @property
    def columns(self):
        return self._meta.columns

    def __len__(self):
        return new_collection(Len(self.expr)).compute()

    @property
    def nbytes(self):
        raise NotImplementedError("nbytes is not implemented on DataFrame")

    def __reduce__(self):
        return new_collection, (self._expr,)

    def __getitem__(self, other):
        if isinstance(other, FrameBase):
            return new_collection(self.expr.__getitem__(other.expr))
        return new_collection(self.expr.__getitem__(other))

    def __dask_graph__(self):
        out = self.expr
        out = out.optimize(fuse=False)
        return out.__dask_graph__()

    def __dask_keys__(self):
        out = self.expr
        out = out.optimize(fuse=False)
        return out.__dask_keys__()

    def simplify(self):
        return new_collection(self.expr.simplify())

    def lower_once(self):
        return new_collection(self.expr.lower_once())

    def optimize(self, combine_similar: bool = True, fuse: bool = True):
        return new_collection(
            self.expr.optimize(combine_similar=combine_similar, fuse=fuse)
        )

    @property
    def dask(self):
        return self.__dask_graph__()

    def __dask_postcompute__(self):
        state = self.optimize(fuse=False)
        if type(self) != type(state):
            return state.__dask_postcompute__()
        return _concat, ()

    def __dask_postpersist__(self):
        state = self.optimize(fuse=False)
        return from_graph, (state._meta, state.divisions, state._name)

    def __getattr__(self, key):
        try:
            # Prioritize `FrameBase` attributes
            return object.__getattribute__(self, key)
        except AttributeError as err:
            try:
                # Fall back to `expr` API
                # (Making sure to convert to/from Expr)
                val = getattr(self.expr, key)
                if callable(val):
                    return functools.partial(_wrap_expr_api, wrap_api=val)
                return val
            except AttributeError:
                # Raise original error
                raise err

    def visualize(self, tasks: bool = False, **kwargs):
        """Visualize the expression or task graph

        Parameters
        ----------
        tasks:
            Whether to visualize the task graph. By default
            the expression graph will be visualized instead.
        """
        if tasks:
            return super().visualize(**kwargs)
        return self.expr.visualize(**kwargs)

    @property
    def index(self):
        return new_collection(self.expr.index)

    def reset_index(self, drop=False):
        return new_collection(expr.ResetIndex(self.expr, drop))

    def head(self, n=5, compute=True):
        out = new_collection(expr.Head(self.expr, n=n))
        if compute:
            out = out.compute()
        return out

    def tail(self, n=5, compute=True):
        out = new_collection(expr.Tail(self.expr, n=n))
        if compute:
            out = out.compute()
        return out

    def copy(self):
        """Return a copy of this object"""
        return new_collection(self.expr)

    def isin(self, values):
        if isinstance(self, DataFrame):
            # DataFrame.isin does weird alignment stuff
            bad_types = (FrameBase, pd.Series, pd.DataFrame)
        else:
            bad_types = (FrameBase,)
        if isinstance(values, bad_types):
            raise NotImplementedError("Passing a %r to `isin`" % typename(type(values)))

        # We wrap values in a delayed for two reasons:
        # - avoid serializing data in every task
        # - avoid cost of traversal of large list in optimizations
        if isinstance(values, list):
            # Motivated by https://github.com/dask/dask/issues/9411.  This appears to be
            # caused by https://github.com/dask/distributed/issues/6368, and further
            # exacerbated by the fact that the list contains duplicates.  This is a patch until
            # we can create a better fix for Serialization.
            try:
                values = list(set(values))
            except TypeError:
                pass
            if not any(is_dask_collection(v) for v in values):
                try:
                    values = np.fromiter(values, dtype=object)
                except ValueError:
                    # Numpy 1.23 supports creating arrays of iterables, while lower
                    # version 1.21.x and 1.22.x do not
                    pass
        # TODO: use delayed for values
        return new_collection(expr.Isin(self.expr, values=values))

    def _partitions(self, index):
        # Used by `partitions` for partition-wise slicing

        # Convert index to list
        if isinstance(index, int):
            index = [index]
        index = np.arange(self.npartitions, dtype=object)[index].tolist()

        # Check that selection makes sense
        assert set(index).issubset(range(self.npartitions))

        # Return selected partitions
        return new_collection(expr.Partitions(self.expr, index))

    @property
    def partitions(self):
        """Partition-wise slicing of a collection

        Examples
        --------
        >>> df.partitions[0]
        >>> df.partitions[:3]
        >>> df.partitions[::10]
        """
        return IndexCallable(self._partitions)

    def shuffle(
        self,
        index: str | list,
        ignore_index: bool = False,
        npartitions: int | None = None,
        backend: str | None = None,
        **options,
    ):
        """Shuffle a collection by column names

        Parameters
        ----------
        index:
            Column names to shuffle by.
        ignore_index: optional
            Whether to ignore the index. Default is ``False``.
        npartitions: optional
            Number of output partitions. The partition count will
            be preserved by default.
        backend: optional
            Desired shuffle backend. Default chosen at optimization time.
        **options: optional
            Algorithm-specific options.
        """
        from dask_expr._shuffle import Shuffle

        # Preserve partition count by default
        npartitions = npartitions or self.npartitions

        # Returned shuffled result
        return new_collection(
            Shuffle(
                self.expr,
                index,
                npartitions,
                ignore_index,
                backend,
                options,
            )
        )

    def groupby(self, by, **kwargs):
        from dask_expr._groupby import GroupBy

        if isinstance(by, FrameBase) and not isinstance(by, Series):
            raise ValueError(
                f"`by` must be a column name or list of columns, got {by}."
            )

        return GroupBy(self, by, **kwargs)

    def map_partitions(
        self,
        func,
        *args,
        meta=no_default,
        enforce_metadata=True,
        transform_divisions=True,
        clear_divisions=False,
        align_dataframes=False,
        **kwargs,
    ):
        """Apply a Python function to each partition

        Parameters
        ----------
        func : function
            Function applied to each partition.
        args, kwargs :
            Arguments and keywords to pass to the function. Arguments and
            keywords may contain ``FrameBase`` or regular python objects.
            DataFrame-like args (both dask and pandas) must have the same
            number of partitions as ``self` or comprise a single partition.
            Key-word arguments, Single-partition arguments, and general
            python-object argments will be broadcasted to all partitions.
        enforce_metadata : bool, default True
            Whether to enforce at runtime that the structure of the DataFrame
            produced by ``func`` actually matches the structure of ``meta``.
            This will rename and reorder columns for each partition, and will
            raise an error if this doesn't work, but it won't raise if dtypes
            don't match.
        transform_divisions : bool, default True
            Whether to apply the function onto the divisions and apply those
            transformed divisions to the output.
        clear_divisions : bool, default False
            Whether divisions should be cleared. If True, `transform_divisions`
            will be ignored.
        meta : Any, optional
            DataFrame object representing the schema of the expected result.
        """

        if align_dataframes:
            # TODO: Handle alignment?
            # Perhaps we only handle the case that all `Expr` operands
            # have the same number of partitions or can be broadcasted
            # within `MapPartitions`. If so, the `map_partitions` API
            # will need to call `Repartition` on operands that are not
            # aligned with `self.expr`.
            raise NotImplementedError()
        new_expr = expr.MapPartitions(
            self.expr,
            func,
            meta,
            enforce_metadata,
            transform_divisions,
            clear_divisions,
            kwargs,
            *[arg.expr if isinstance(arg, FrameBase) else arg for arg in args],
        )
        return new_collection(new_expr)

    def repartition(self, npartitions=None, divisions=None, force=False):
        """Repartition a collection

        Exactly one of `divisions` or `npartitions` should be specified.
        A ``ValueError`` will be raised when that is not the case.

        Parameters
        ----------
        npartitions : int, optional
            Approximate number of partitions of output. The number of
            partitions used may be slightly lower than npartitions depending
            on data distribution, but will never be higher.
        divisions : list, optional
            The "dividing lines" used to split the dataframe into partitions.
            For ``divisions=[0, 10, 50, 100]``, there would be three output partitions,
            where the new index contained [0, 10), [10, 50), and [50, 100), respectively.
            See https://docs.dask.org/en/latest/dataframe-design.html#partitions.
        force : bool, default False
            Allows the expansion of the existing divisions.
            If False then the new divisions' lower and upper bounds must be
            the same as the old divisions'.
        """

        if sum([divisions is not None, npartitions is not None]) != 1:
            raise ValueError(
                "Please provide exactly one of the ``npartitions=`` or "
                "``divisions=`` keyword arguments."
            )

        return new_collection(Repartition(self.expr, npartitions, divisions, force))

    def to_dask_dataframe(self, optimize: bool = True, **optimize_kwargs) -> _Frame:
        """Convert to a dask-dataframe collection

        Parameters
        ----------
        optimize
            Whether to optimize the underlying `Expr` object before conversion.
        **optimize_kwargs
            Key-word arguments to pass through to `optimize`.
        """
        df = self.optimize(**optimize_kwargs) if optimize else self
        return new_dd_object(df.dask, df._name, df._meta, df.divisions)

    def sum(self, skipna=True, numeric_only=False, min_count=0):
        return new_collection(self.expr.sum(skipna, numeric_only, min_count))

    def prod(self, skipna=True, numeric_only=False, min_count=0):
        return new_collection(self.expr.prod(skipna, numeric_only, min_count))

    def var(self, axis=0, skipna=True, ddof=1, numeric_only=False):
        return new_collection(self.expr.var(axis, skipna, ddof, numeric_only))

    def std(self, axis=0, skipna=True, ddof=1, numeric_only=False):
        return new_collection(self.expr.std(axis, skipna, ddof, numeric_only))

    def mean(self, skipna=True, numeric_only=False, min_count=0):
        return new_collection(self.expr.mean(skipna, numeric_only))

    def max(self, skipna=True, numeric_only=False, min_count=0):
        return new_collection(self.expr.max(skipna, numeric_only, min_count))

    def any(self, skipna=True):
        return new_collection(self.expr.any(skipna))

    def all(self, skipna=True):
        return new_collection(self.expr.all(skipna))

    def idxmin(self, skipna=True, numeric_only=False):
        return new_collection(self.expr.idxmin(skipna, numeric_only))

    def idxmax(self, skipna=True, numeric_only=False):
        return new_collection(self.expr.idxmax(skipna, numeric_only))

    def mode(self, dropna=True):
        return new_collection(self.expr.mode(dropna))

    def min(self, skipna=True, numeric_only=False, min_count=0):
        return new_collection(self.expr.min(skipna, numeric_only, min_count))

    def count(self, numeric_only=False):
        return new_collection(self.expr.count(numeric_only))

    def abs(self):
        return new_collection(self.expr.abs())

    def astype(self, dtypes):
        return new_collection(self.expr.astype(dtypes))

    def clip(self, lower=None, upper=None):
        return new_collection(self.expr.clip(lower, upper))

    def combine_first(self, other):
        return new_collection(self.expr.combine_first(other.expr))

    def to_timestamp(self, freq=None, how="start"):
        return new_collection(self.expr.to_timestamp(freq, how))

    def isna(self):
        return new_collection(self.expr.isna())

    def round(self, decimals=0):
        return new_collection(self.expr.round(decimals))

    def apply(self, function, *args, **kwargs):
        return new_collection(self.expr.apply(function, *args, **kwargs))

    def replace(self, to_replace=None, value=no_default, regex=False):
        return new_collection(self.expr.replace(to_replace, value, regex))

    def fillna(self, value=None):
        return new_collection(self.expr.fillna(value))

    def rename_axis(
        self, mapper=no_default, index=no_default, columns=no_default, axis=0
    ):
        return new_collection(self.expr.rename_axis(mapper, index, columns, axis))

    def align(self, other, join="outer", fill_value=None):
        return self.expr.align(other.expr, join, fill_value)

    def nunique_approx(self):
        return new_collection(self.expr.nunique_approx())


# Add operator attributes
for op in [
    "__add__",
    "__radd__",
    "__sub__",
    "__rsub__",
    "__mul__",
    "__rmul__",
    "__truediv__",
    "__rtruediv__",
    "__lt__",
    "__rlt__",
    "__gt__",
    "__rgt__",
    "__le__",
    "__rle__",
    "__ge__",
    "__rge__",
    "__eq__",
    "__ne__",
    "__and__",
    "__rand__",
    "__or__",
    "__ror__",
    "__xor__",
    "__rxor__",
]:
    setattr(FrameBase, op, functools.partialmethod(_wrap_expr_op, op=op))

for op in [
    "__invert__",
    "__neg__",
    "__pos__",
]:
    setattr(FrameBase, op, functools.partialmethod(_wrap_unary_expr_op, op=op))


class DataFrame(FrameBase):
    """DataFrame-like Expr Collection"""

    def assign(self, **pairs):
        result = self
        data = self.copy()
        for k, v in pairs.items():
            if not isinstance(k, str):
                raise TypeError(f"Column name cannot be type {type(k)}")

            if callable(v):
                v = v(data)

            if isinstance(v, (Scalar, Series)):
                result = new_collection(expr.Assign(result.expr, k, v.expr))
            elif not isinstance(v, FrameBase) and isinstance(v, Hashable):
                result = new_collection(expr.Assign(result.expr, k, v))
            else:
                raise TypeError(f"Column assignment doesn't support type {type(v)}")

        return result

    def merge(
        self,
        right,
        how="inner",
        on=None,
        left_on=None,
        right_on=None,
        left_index=False,
        right_index=False,
        suffixes=("_x", "_y"),
        indicator=False,
        shuffle_backend=None,
    ):
        """Merge the DataFrame with another DataFrame

        Parameters
        ----------
        right: FrameBase
        how : {'left', 'right', 'outer', 'inner'}, default: 'inner'
            How to handle the operation of the two objects:
            - left: use calling frame's index (or column if on is specified)
            - right: use other frame's index
            - outer: form union of calling frame's index (or column if on is
              specified) with other frame's index, and sort it
              lexicographically
            - inner: form intersection of calling frame's index (or column if
              on is specified) with other frame's index, preserving the order
              of the calling's one
        on : label or list
            Column or index level names to join on. These must be found in both
            DataFrames. If on is None and not merging on indexes then this
            defaults to the intersection of the columns in both DataFrames.
        left_on : label or list, or array-like
            Column to join on in the left DataFrame. Other than in pandas
            arrays and lists are only support if their length is 1.
        right_on : label or list, or array-like
            Column to join on in the right DataFrame. Other than in pandas
            arrays and lists are only support if their length is 1.
        left_index : boolean, default False
            Use the index from the left DataFrame as the join key.
        right_index : boolean, default False
            Use the index from the right DataFrame as the join key.
        suffixes : 2-length sequence (tuple, list, ...)
            Suffix to apply to overlapping column names in the left and
            right side, respectively
        indicator : boolean or string, default False
            Passed through to the backend DataFrame library.
        shuffle_backend: optional
            Shuffle backend to use if shuffling is necessary.
        """

        left = self.expr
        right = (
            right.expr if isinstance(right, FrameBase) else from_pandas(right, 1).expr
        )
        assert is_dataframe_like(right._meta)

        for o in [on, left_on, right_on]:
            if isinstance(o, FrameBase):
                raise NotImplementedError()
        if (
            not on
            and not left_on
            and not right_on
            and not left_index
            and not right_index
        ):
            on = [c for c in left.columns if c in right.columns]
            if not on:
                left_index = right_index = True

        if on and not left_on and not right_on:
            left_on = right_on = on
            on = None

        supported_how = ("left", "right", "outer", "inner")
        if how not in supported_how:
            raise ValueError(
                f"dask.dataframe.merge does not support how='{how}'."
                f"Options are: {supported_how}."
            )

        return new_collection(
            Merge(
                left,
                right,
                how=how,
                left_on=left_on,
                right_on=right_on,
                left_index=left_index,
                right_index=right_index,
                suffixes=suffixes,
                indicator=indicator,
                shuffle_backend=shuffle_backend,
            )
        )

    def join(
        self,
        other,
        on=None,
        how="left",
        lsuffix="",
        rsuffix="",
        shuffle_backend=None,
    ):
        if (
            not isinstance(other, list)
            and not is_dataframe_like(other._meta)
            and hasattr(other._meta, "name")
        ):
            other = new_collection(expr.ToFrame(other.expr))

        if not isinstance(other, FrameBase):
            if not isinstance(other, list) or not all(
                isinstance(o, FrameBase) for o in other
            ):
                raise ValueError("other must be DataFrame or list of DataFrames")
            if how not in ("outer", "left"):
                raise ValueError("merge_multi only supports left or outer joins")

            return new_collection(
                JoinRecursive([self.expr] + [o.expr for o in other], how=how)
            )

        return self.merge(
            right=other,
            left_index=on is None,
            right_index=True,
            left_on=on,
            how=how,
            suffixes=(lsuffix, rsuffix),
            shuffle_backend=shuffle_backend,
        )

    def __setitem__(self, key, value):
        out = self.assign(**{key: value})
        self._expr = out._expr

    def __delitem__(self, key):
        columns = [c for c in self.columns if c != key]
        out = self[columns]
        self._expr = out._expr

    def __getattr__(self, key):
        try:
            # Prioritize `DataFrame` attributes
            return object.__getattribute__(self, key)
        except AttributeError as err:
            try:
                # Check if key is in columns if key
                # is not a normal attribute
                if key in self.expr._meta.columns:
                    return Series(self.expr[key])
                raise err
            except AttributeError:
                # Fall back to `BaseFrame.__getattr__`
                return super().__getattr__(key)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(set(dir(expr.Expr)))
        o.update(c for c in self.columns if (isinstance(c, str) and c.isidentifier()))
        return list(o)

    def map(self, func, na_action=None):
        return new_collection(expr.Map(self.expr, arg=func, na_action=na_action))

    def __repr__(self):
        return f"<dask_expr.expr.DataFrame: expr={self.expr}>"

    def nlargest(self, n=5, columns=None):
        return new_collection(NLargest(self.expr, n=n, _columns=columns))

    def nsmallest(self, n=5, columns=None):
        return new_collection(NSmallest(self.expr, n=n, _columns=columns))

    def memory_usage(self, deep=False, index=True):
        return new_collection(MemoryUsageFrame(self.expr, deep=deep, _index=index))

    def drop_duplicates(self, subset=None, ignore_index=False):
        # Fail early if subset is not valid, e.g. missing columns
        subset = _convert_to_list(subset)
        meta_nonempty(self._meta).drop_duplicates(subset=subset)
        return new_collection(
            DropDuplicates(self.expr, subset=subset, ignore_index=ignore_index)
        )

    def dropna(self, how=no_default, subset=None, thresh=no_default):
        if how is not no_default and thresh is not no_default:
            raise TypeError(
                "You cannot set both the how and thresh arguments at the same time."
            )
        subset = _convert_to_list(subset)
        return new_collection(
            expr.DropnaFrame(self.expr, how=how, subset=subset, thresh=thresh)
        )

    def sample(self, n=None, frac=None, replace=False, random_state=None):
        if n is not None:
            msg = (
                "sample does not support the number of sampled items "
                "parameter, 'n'. Please use the 'frac' parameter instead."
            )
            if isinstance(n, Number) and 0 <= n <= 1:
                warnings.warn(msg)
                frac = n
            else:
                raise ValueError(msg)

        if frac is None:
            raise ValueError("frac must not be None")

        if random_state is None:
            random_state = np.random.RandomState()

        state_data = random_state_data(self.npartitions, random_state)
        return new_collection(
            expr.Sample(self.expr, state_data=state_data, frac=frac, replace=replace)
        )

    def rename(self, columns):
        return new_collection(expr.RenameFrame(self.expr, columns=columns))

    def explode(self, column):
        column = _convert_to_list(column)
        return new_collection(expr.ExplodeFrame(self.expr, column=column))

    def drop(self, labels=None, columns=None, errors="raise"):
        if columns is None:
            columns = labels
        if columns is None:
            raise TypeError("must either specify 'columns' or 'labels'")
        return new_collection(expr.Drop(self.expr, columns=columns, errors=errors))

    def to_parquet(self, path, **kwargs):
        from dask_expr.io.parquet import to_parquet

        return to_parquet(self, path, **kwargs)

    def select_dtypes(self, include=None, exclude=None):
        columns = self._meta.select_dtypes(include=include, exclude=exclude).columns
        return new_collection(self.expr[columns])

    def eval(self, expr, **kwargs):
        return new_collection(Eval(self.expr, _expr=expr, expr_kwargs=kwargs))

    def set_index(
        self,
        other,
        drop=True,
        sorted=False,
        npartitions: int | None = None,
        divisions=None,
        sort: bool = True,
        upsample: float = 1.0,
    ):
        if isinstance(other, DataFrame):
            raise TypeError("other can't be of type DataFrame")
        if isinstance(other, Series):
            if other._name == self.index._name:
                return self
        elif other == self.index.name:
            return self

        if divisions is not None:
            check_divisions(divisions)
        other = other.expr if isinstance(other, Series) else other

        if (sorted or not sort) and npartitions is not None:
            raise ValueError(
                "Specifying npartitions with sort=False or sorted=True is not "
                "supported. Call `repartition` afterwards."
            )

        if sorted:
            return new_collection(
                SetIndexBlockwise(self.expr, other, drop, new_divisions=divisions)
            )
        elif not sort:
            return new_collection(SetIndexBlockwise(self.expr, other, drop, None))

        return new_collection(
            SetIndex(
                self.expr,
                other,
                drop,
                user_divisions=divisions,
                npartitions=npartitions,
                upsample=upsample,
            )
        )

    def sort_values(
        self,
        by: str | list[str],
        npartitions: int | None = None,
        ascending: bool | list[bool] = True,
        na_position: Literal["first"] | Literal["last"] = "last",
        partition_size: float = 128e6,
        sort_function: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
        sort_function_kwargs: Mapping[str, Any] | None = None,
        upsample: float = 1.0,
    ):
        """See DataFrame.sort_values for docstring"""
        if na_position not in ("first", "last"):
            raise ValueError("na_position must be either 'first' or 'last'")
        if not isinstance(by, list):
            by = [by]
        if any(not isinstance(b, str) for b in by):
            raise NotImplementedError(
                "Dataframes only support sorting by named columns which must be passed as a "
                "string or a list of strings.\n"
                "You passed %s" % str(by)
            )

        if not isinstance(ascending, bool):
            # support [True] as input
            if (
                isinstance(ascending, list)
                and len(ascending) == 1
                and isinstance(ascending[0], bool)
            ):
                ascending = ascending[0]
            else:
                raise NotImplementedError(
                    f"Dask currently only supports a single boolean for ascending. You passed {str(ascending)}"
                )

        return new_collection(
            SortValues(
                self.expr,
                by,
                ascending,
                na_position,
                npartitions,
                partition_size,
                sort_function,
                sort_function_kwargs,
                upsample,
            )
        )

    def add_prefix(self, prefix):
        return new_collection(expr.AddPrefix(self.expr, prefix))

    def add_suffix(self, suffix):
        return new_collection(expr.AddSuffix(self.expr, suffix))

    def pivot_table(self, index, columns, values, aggfunc="mean"):
        if not is_scalar(index) or index not in self._meta.columns:
            raise ValueError("'index' must be the name of an existing column")
        if not is_scalar(columns) or columns not in self._meta.columns:
            raise ValueError("'columns' must be the name of an existing column")
        if not methods.is_categorical_dtype(self._meta[columns]):
            raise ValueError("'columns' must be category dtype")
        if not has_known_categories(self._meta[columns]):
            raise ValueError("'columns' categories must be known")

        if not (
            is_scalar(values)
            and values in self._meta.columns
            or not is_scalar(values)
            and all(is_scalar(x) and x in self._meta.columns for x in values)
        ):
            raise ValueError("'values' must refer to an existing column or columns")

        return new_collection(
            PivotTable(
                self.expr, index=index, columns=columns, values=values, aggfunc=aggfunc
            )
        )


class Series(FrameBase):
    """Series-like Expr Collection"""

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(set(dir(expr.Expr)))
        return list(o)

    @property
    def name(self):
        return self.expr._meta.name

    @property
    def nbytes(self):
        return new_collection(self.expr.nbytes)

    def map(self, arg, na_action=None):
        return new_collection(expr.Map(self.expr, arg=arg, na_action=na_action))

    def __repr__(self):
        return f"<dask_expr.expr.Series: expr={self.expr}>"

    def to_frame(self, name=no_default):
        return new_collection(expr.ToFrame(self.expr, name=name))

    def value_counts(self, sort=None, ascending=False, dropna=True, normalize=False):
        return new_collection(
            ValueCounts(self.expr, sort, ascending, dropna, normalize)
        )

    def nlargest(self, n=5):
        return new_collection(NLargest(self.expr, n=n))

    def nsmallest(self, n=5):
        return new_collection(NSmallest(self.expr, n=n))

    def memory_usage(self, deep=False, index=True):
        return new_collection(MemoryUsageFrame(self.expr, deep=deep, _index=index))

    def unique(self):
        return new_collection(Unique(self.expr))

    def drop_duplicates(self, ignore_index=False):
        return new_collection(DropDuplicates(self.expr, ignore_index=ignore_index))

    def dropna(self):
        return new_collection(expr.DropnaSeries(self.expr))

    def between(self, left, right, inclusive="both"):
        return new_collection(
            expr.Between(self.expr, left=left, right=right, inclusive=inclusive)
        )

    def explode(self):
        return new_collection(expr.ExplodeSeries(self.expr))

    cat = CachedAccessor("cat", CategoricalAccessor)

    def _repartition_quantiles(self, npartitions, upsample=1.0, random_state=None):
        return new_collection(
            RepartitionQuantiles(self.expr, npartitions, upsample, random_state)
        )


class Index(Series):
    """Index-like Expr Collection"""

    def __repr__(self):
        return f"<dask_expr.expr.Index: expr={self.expr}>"

    def to_frame(self, index=True, name=no_default):
        if not index:
            raise NotImplementedError
        return new_collection(expr.ToFrameIndex(self.expr, index=index, name=name))

    def memory_usage(self, deep=False):
        return new_collection(MemoryUsageIndex(self.expr, deep=deep))

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(set(dir(expr.Expr)))
        return list(o)


class Scalar(FrameBase):
    """Scalar Expr Collection"""

    def __repr__(self):
        return f"<dask_expr.expr.Scalar: expr={self.expr}>"

    def __dask_postcompute__(self):
        return first, ()


def new_collection(expr):
    """Create new collection from an expr"""

    meta = expr._meta
    expr._name  # Ensure backend is imported
    if is_dataframe_like(meta):
        return DataFrame(expr)
    elif is_series_like(meta):
        return Series(expr)
    elif is_index_like(meta):
        return Index(expr)
    else:
        return Scalar(expr)


def optimize(collection, fuse=True):
    return new_collection(expr.optimize(collection.expr, fuse=fuse))


def from_pandas(data, npartitions=1, sort=True):
    from dask_expr.io.io import FromPandas

    return new_collection(FromPandas(data.copy(), npartitions=npartitions, sort=sort))


def from_graph(*args, **kwargs):
    from dask_expr.io.io import FromGraph

    return new_collection(FromGraph(*args, **kwargs))


def from_dask_dataframe(ddf: _Frame, optimize: bool = True) -> FrameBase:
    """Create a dask-expr collection from a dask-dataframe collection

    Parameters
    ----------
    optimize
        Whether to optimize the graph before conversion.
    """
    graph = ddf.dask
    if optimize:
        graph = ddf.__dask_optimize__(graph, ddf.__dask_keys__())
    return from_graph(graph, ddf._meta, ddf.divisions, ddf._name)


def read_csv(path, *args, **kwargs):
    from dask_expr.io.csv import ReadCSV

    if not isinstance(path, str):
        path = stringify_path(path)
    return new_collection(ReadCSV(path, *args, **kwargs))


def read_parquet(
    path=None,
    columns=None,
    filters=None,
    categories=None,
    index=None,
    storage_options=None,
    dtype_backend=None,
    calculate_divisions=False,
    ignore_metadata_file=False,
    metadata_task_size=None,
    split_row_groups="infer",
    blocksize="default",
    aggregate_files=None,
    parquet_file_extension=(".parq", ".parquet", ".pq"),
    filesystem="fsspec",
    engine=None,
    **kwargs,
):
    from dask_expr.io.parquet import ReadParquet, _set_parquet_engine

    if not isinstance(path, str):
        path = stringify_path(path)

    kwargs["dtype_backend"] = dtype_backend

    return new_collection(
        ReadParquet(
            path,
            columns=_convert_to_list(columns),
            filters=filters,
            categories=categories,
            index=index,
            storage_options=storage_options,
            calculate_divisions=calculate_divisions,
            ignore_metadata_file=ignore_metadata_file,
            metadata_task_size=metadata_task_size,
            split_row_groups=split_row_groups,
            blocksize=blocksize,
            aggregate_files=aggregate_files,
            parquet_file_extension=parquet_file_extension,
            filesystem=filesystem,
            engine=_set_parquet_engine(engine),
            kwargs=kwargs,
        )
    )


def concat(
    dfs,
    axis=0,
    join="outer",
    ignore_unknown_divisions=False,
    ignore_order=False,
    **kwargs,
):
    if not isinstance(dfs, list):
        raise TypeError("dfs must be a list of DataFrames/Series objects")
    if len(dfs) == 0:
        raise ValueError("No objects to concatenate")
    if len(dfs) == 1:
        return dfs[0]

    if join not in ("inner", "outer"):
        raise ValueError("'join' must be 'inner' or 'outer'")

    dfs = [from_pandas(df) if not is_dask_collection(df) else df for df in dfs]

    if axis == 1:
        dfs = [df for df in dfs if len(df.columns) > 0]

    return new_collection(
        Concat(
            join,
            ignore_order,
            kwargs,
            axis,
            ignore_unknown_divisions,
            *[df.expr for df in dfs],
        )
    )

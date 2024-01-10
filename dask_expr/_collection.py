from __future__ import annotations

import datetime
import functools
import inspect
import warnings
from collections.abc import Callable, Hashable, Mapping
from numbers import Integral, Number
from typing import Any, ClassVar, Literal

import numpy as np
import pandas as pd
from dask import compute
from dask.array import Array
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
from dask.dataframe.dispatch import is_categorical_dtype, make_meta, meta_nonempty
from dask.dataframe.multi import warn_dtype_mismatch
from dask.dataframe.utils import has_known_categories, index_summary
from dask.utils import (
    IndexCallable,
    get_default_shuffle_method,
    memory_repr,
    put_lines,
    random_state_data,
    typename,
)
from fsspec.utils import stringify_path
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_timedelta64_dtype,
)
from tlz import first

from dask_expr import _expr as expr
from dask_expr._align import AlignPartitions
from dask_expr._categorical import CategoricalAccessor, Categorize, GetCategories
from dask_expr._concat import Concat
from dask_expr._datetime import DatetimeAccessor
from dask_expr._describe import DescribeNonNumeric, DescribeNumeric
from dask_expr._expr import (
    BFill,
    Diff,
    Eval,
    FFill,
    Query,
    Shift,
    ToDatetime,
    ToNumeric,
    ToTimedelta,
    no_default,
)
from dask_expr._merge import JoinRecursive, Merge
from dask_expr._quantile import SeriesQuantile
from dask_expr._quantiles import RepartitionQuantiles
from dask_expr._reductions import (
    DropDuplicates,
    IsMonotonicDecreasing,
    IsMonotonicIncreasing,
    Len,
    MemoryUsageFrame,
    MemoryUsageIndex,
    NLargest,
    NSmallest,
    PivotTable,
    Unique,
    ValueCounts,
)
from dask_expr._repartition import Repartition, RepartitionFreq
from dask_expr._shuffle import SetIndex, SetIndexBlockwise, SortValues
from dask_expr._str_accessor import StringAccessor
from dask_expr._util import (
    RaiseAttributeError,
    _BackendData,
    _convert_to_list,
    _get_shuffle_preferring_order,
    _maybe_from_pandas,
    _raise_if_object_series,
    _validate_axis,
    is_scalar,
)
from dask_expr.io import FromPandasDivisions, FromScalars

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


def _wrap_expr_method_operator(name, class_):
    """
    Add method operators to Series or DataFrame like DataFrame.add.
    _wrap_expr_method_operator("add", DataFrame)
    """
    if class_ == DataFrame:

        def method(self, other, axis="columns", level=None, fill_value=None):
            if level is not None:
                raise NotImplementedError("level must be None")

            axis = _validate_axis(axis)

            if axis in (1, "columns"):
                if isinstance(other, Series):
                    msg = f"Unable to {name} dd.Series with axis=1"
                    raise ValueError(msg)
            return new_collection(
                expr.MethodOperator(
                    name=name,
                    left=self,
                    right=other,
                    axis=axis,
                    level=level,
                    fill_value=fill_value,
                )
            )

    elif class_ == Series:

        def method(self, other, level=None, fill_value=None, axis=0):
            if level is not None:
                raise NotImplementedError("level must be None")

            axis = _validate_axis(axis)

            return new_collection(
                expr.MethodOperator(
                    name=name,
                    left=self,
                    right=other,
                    axis=axis,
                    fill_value=fill_value,
                    level=level,
                )
            )

    else:
        raise NotImplementedError(f"Cannot create method operator for {class_=}")

    method.__name__ = name
    return method


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

    @functools.cached_property
    def _meta_nonempty(self):
        return meta_nonempty(self._meta)

    @property
    def divisions(self):
        return self.expr.divisions

    @property
    def dtypes(self):
        return self.expr._meta.dtypes

    @property
    def size(self):
        return new_collection(self.expr.size)

    @property
    def columns(self):
        return self._meta.columns

    @columns.setter
    def columns(self, columns):
        if len(columns) != len(self.columns):
            # surface pandas error
            self._expr._meta.columns = columns
        self._expr = expr.ColumnsSetter(self, columns)

    def clear_divisions(self):
        return new_collection(expr.ClearDivisions(self))

    def __len__(self):
        return new_collection(Len(self)).compute()

    @property
    def nbytes(self):
        raise NotImplementedError("nbytes is not implemented on DataFrame")

    def __reduce__(self):
        return new_collection, (self._expr,)

    def __getitem__(self, other):
        if isinstance(other, FrameBase):
            return new_collection(self.expr.__getitem__(other.expr))
        elif isinstance(other, slice):
            return self.loc[other]
        return new_collection(self.expr.__getitem__(other))

    def __bool__(self):
        raise ValueError(
            f"The truth value of a {self.__class__.__name__} is ambiguous. "
            "Use a.any() or a.all()."
        )

    def persist(self, fuse=True, **kwargs):
        out = self.optimize(fuse=fuse)
        return DaskMethodsMixin.persist(out, **kwargs)

    def compute(self, fuse=True, **kwargs):
        out = self
        if not isinstance(out, Scalar):
            out = out.repartition(npartitions=1)
        out = out.optimize(fuse=fuse)
        return DaskMethodsMixin.compute(out, **kwargs)

    def __dask_graph__(self):
        out = self.expr
        out = out.lower_completely()
        return out.__dask_graph__()

    def __dask_keys__(self):
        out = self.expr
        out = out.lower_completely()
        return out.__dask_keys__()

    def simplify(self):
        return new_collection(self.expr.simplify())

    def lower_once(self):
        return new_collection(self.expr.lower_once())

    def optimize(self, fuse: bool = True):
        return new_collection(self.expr.optimize(fuse=fuse))

    @property
    def dask(self):
        return self.__dask_graph__()

    def __dask_postcompute__(self):
        state = new_collection(self.expr.lower_completely())
        if type(self) != type(state):
            return state.__dask_postcompute__()
        return _concat, ()

    def __dask_postpersist__(self):
        state = new_collection(self.expr.lower_completely())
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

    @index.setter
    def index(self, value):
        assert expr.are_co_aligned(
            self.expr, value.expr
        ), "value needs to be aligned with the index"
        _expr = expr.AssignIndex(self, value)
        self._expr = _expr

    def reset_index(self, drop=False):
        return new_collection(expr.ResetIndex(self, drop))

    def head(self, n=5, npartitions=1, compute=True):
        out = new_collection(expr.Head(self, n=n, npartitions=npartitions))
        if compute:
            out = out.compute()
        return out

    def tail(self, n=5, compute=True):
        out = new_collection(expr.Tail(self, n=n))
        if compute:
            out = out.compute()
        return out

    def copy(self, deep=False):
        """Return a copy of this object"""
        if deep is not False:
            raise ValueError(
                "The `deep` value must be False. This is strictly a shallow copy "
                "of the underlying computational graph."
            )
        return new_collection(self.expr)

    def eq(self, other):
        return self.__eq__(other)

    def ne(self, other):
        return self.__ne__(other)

    def gt(self, other):
        return self.__gt__(other)

    def ge(self, other):
        return self.__ge__(other)

    def lt(self, other):
        return self.__lt__(other)

    def le(self, other):
        return self.__le__(other)

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
        return new_collection(expr.Isin(self, values=values))

    def _partitions(self, index):
        # Used by `partitions` for partition-wise slicing

        # Convert index to list
        if isinstance(index, int):
            index = [index]
        index = np.arange(self.npartitions, dtype=object)[index].tolist()

        # Check that selection makes sense
        assert set(index).issubset(range(self.npartitions))

        # Return selected partitions
        return new_collection(expr.Partitions(self, index))

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

    def get_partition(self, n):
        return self.partitions[n]

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

        if isinstance(index, FrameBase):
            if not expr.are_co_aligned(self.expr, index.expr):
                raise TypeError(
                    "index must be aligned with the DataFrame to use as shuffle index."
                )

        # Returned shuffled result
        return new_collection(
            Shuffle(
                self,
                index,
                npartitions,
                ignore_index,
                backend,
                options,
            )
        )

    def resample(self, rule, **kwargs):
        from dask_expr._resample import Resampler

        return Resampler(self, rule, **kwargs)

    def rolling(self, window, **kwargs):
        from dask_expr._rolling import Rolling

        return Rolling(self, window, **kwargs)

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
            python-object arguments will be broadcasted to all partitions.
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

        return map_partitions(
            func,
            self,
            *args,
            meta=meta,
            enforce_metadata=enforce_metadata,
            transform_divisions=transform_divisions,
            clear_divisions=clear_divisions,
            align_dataframes=align_dataframes,
            **kwargs,
        )

    def map_overlap(
        self,
        func,
        before,
        after,
        *args,
        meta=no_default,
        enforce_metadata=True,
        transform_divisions=True,
        clear_divisions=False,
        align_dataframes=False,
        **kwargs,
    ):
        return map_overlap(
            self,
            func,
            before,
            after,
            *args,
            meta=meta,
            enforce_metadata=enforce_metadata,
            transform_divisions=transform_divisions,
            clear_divisions=clear_divisions,
            align_dataframes=align_dataframes,
            **kwargs,
        )

    def repartition(
        self,
        divisions: tuple | None = None,
        npartitions: int | None = None,
        partition_size: str = None,
        freq=None,
        force: bool = False,
    ):
        """Repartition a collection

        Exactly one of `divisions`, `npartitions` or `partition_size` should be
        specified. A ``ValueError`` will be raised when that is not the case.

        Parameters
        ----------
        divisions : list, optional
            The "dividing lines" used to split the dataframe into partitions.
            For ``divisions=[0, 10, 50, 100]``, there would be three output partitions,
            where the new index contained [0, 10), [10, 50), and [50, 100), respectively.
            See https://docs.dask.org/en/latest/dataframe-design.html#partitions.
        npartitions : int, Callable, optional
            Approximate number of partitions of output. The number of
            partitions used may be slightly lower than npartitions depending
            on data distribution, but will never be higher.
            The Callable gets the number of partitions of the input as an argument
            and should return an int.
        partition_size : str, optional
            Max number of bytes of memory for each partition. Use numbers or strings
            like 5MB. If specified npartitions and divisions will be ignored. Note that
            the size reflects the number of bytes used as computed by
            pandas.DataFrame.memory_usage, which will not necessarily match the size
            when storing to disk.
        force : bool, default False
            Allows the expansion of the existing divisions.
            If False then the new divisions' lower and upper bounds must be
            the same as the old divisions'.
        freq : str, pd.Timedelta
            A period on which to partition timeseries data like ``'7D'`` or
            ``'12h'`` or ``pd.Timedelta(hours=12)``.  Assumes a datetime index.
        """

        if (
            sum(
                [
                    divisions is not None,
                    npartitions is not None,
                    partition_size is not None,
                    freq is not None,
                ]
            )
            != 1
        ):
            raise ValueError(
                "Please provide exactly one of the ``npartitions=`` or "
                "``divisions=`` keyword arguments."
            )
        if freq is not None:
            if not isinstance(self.divisions[0], pd.Timestamp):
                raise TypeError("Can only repartition on frequency for timeseries")
            return new_collection(RepartitionFreq(self, freq))
        else:
            return new_collection(
                Repartition(self, npartitions, divisions, force, partition_size, freq)
            )

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

    def to_dask_array(
        self, lengths=None, meta=None, optimize: bool = True, **optimize_kwargs
    ) -> Array:
        return self.to_dask_dataframe(optimize, **optimize_kwargs).to_dask_array(
            lengths=lengths, meta=meta
        )

    @property
    def values(self):
        return self.to_dask_array()

    def sum(self, skipna=True, numeric_only=False, min_count=0, split_every=False):
        result = new_collection(
            self.expr.sum(skipna, numeric_only, min_count, split_every)
        )
        return self._apply_min_count(result, min_count)

    def _apply_min_count(self, result, min_count):
        if min_count:
            cond = self.notnull().sum() >= min_count
            cond_meta = cond._meta
            if not is_series_like(cond_meta):
                result = result.to_series()
                cond = cond.to_series()

            result = result.where(cond, other=np.nan)
            if not is_series_like(cond_meta):
                return result.min()
            else:
                return result
        else:
            return result

    def prod(self, skipna=True, numeric_only=False, min_count=0, split_every=False):
        result = new_collection(
            self.expr.prod(skipna, numeric_only, min_count, split_every)
        )
        return self._apply_min_count(result, min_count)

    product = prod

    def var(self, axis=0, skipna=True, ddof=1, numeric_only=False, split_every=False):
        _raise_if_object_series(self, "var")
        return new_collection(
            self.expr.var(axis, skipna, ddof, numeric_only, split_every=split_every)
        )

    def std(self, axis=0, skipna=True, ddof=1, numeric_only=False, split_every=False):
        _raise_if_object_series(self, "std")
        return new_collection(
            self.expr.std(axis, skipna, ddof, numeric_only, split_every=split_every)
        )

    def mean(self, skipna=True, numeric_only=False, split_every=False):
        _raise_if_object_series(self, "mean")
        return new_collection(
            self.expr.mean(skipna, numeric_only, split_every=split_every)
        )

    def max(self, skipna=True, numeric_only=False, split_every=False):
        return new_collection(self.expr.max(skipna, numeric_only, split_every))

    def any(self, skipna=True, split_every=False):
        return new_collection(self.expr.any(skipna, split_every))

    def all(self, skipna=True, split_every=False):
        return new_collection(self.expr.all(skipna, split_every))

    def idxmin(self, skipna=True, numeric_only=False):
        return new_collection(self.expr.idxmin(skipna, numeric_only))

    def idxmax(self, skipna=True, numeric_only=False):
        return new_collection(self.expr.idxmax(skipna, numeric_only))

    def min(self, skipna=True, numeric_only=False, split_every=False):
        return new_collection(self.expr.min(skipna, numeric_only, split_every))

    def count(self, numeric_only=False, split_every=False):
        return new_collection(self.expr.count(numeric_only, split_every))

    def abs(self):
        # Raise pandas errors
        _raise_if_object_series(self, "abs")
        meta_nonempty(self._meta).abs()
        return new_collection(self.expr.abs())

    def astype(self, dtypes):
        return new_collection(self.expr.astype(dtypes))

    def clip(self, lower=None, upper=None):
        return new_collection(self.expr.clip(lower, upper))

    def combine_first(self, other):
        other = self._create_alignable_frame(other, "outer")
        left, right = self.expr._align_divisions(other.expr, axis=0)
        return new_collection(left.combine_first(right))

    def to_timestamp(self, freq=None, how="start"):
        return new_collection(self.expr.to_timestamp(freq, how))

    def isna(self):
        return new_collection(self.expr.isna())

    def isnull(self):
        return new_collection(self.expr.isnull())

    def mask(self, cond, other=np.nan):
        return new_collection(self.expr.mask(cond, other))

    def round(self, decimals=0):
        return new_collection(self.expr.round(decimals))

    def where(self, cond, other=np.nan):
        return new_collection(self.expr.where(cond, other))

    def apply(self, function, *args, **kwargs):
        return new_collection(self.expr.apply(function, *args, **kwargs))

    def replace(self, to_replace=None, value=no_default, regex=False):
        return new_collection(self.expr.replace(to_replace, value, regex))

    def ffill(self, axis=0, _inplace=False, limit=None, _downcast=None):
        axis = _validate_axis(axis)
        if axis == 1:
            raise NotImplementedError("ffill on axis 1 not implemented")
        return new_collection(FFill(self, limit))

    def bfill(self, axis=0, _inplace=False, limit=None, _downcast=None):
        axis = _validate_axis(axis)
        if axis == 1:
            raise NotImplementedError("bfill on axis 1 not implemented")
        return new_collection(BFill(self, limit))

    def fillna(self, value=None):
        return new_collection(self.expr.fillna(value))

    def shift(self, periods=1, freq=None, axis=0):
        if not isinstance(periods, Integral):
            raise TypeError("periods must be an integer")

        axis = _validate_axis(axis)
        if axis == 0:
            return new_collection(Shift(self, periods, freq))

        return self.map_partitions(
            func=Shift.func,
            enforce_metadata=False,
            transform_divisions=False,
            periods=periods,
            axis=axis,
            freq=freq,
        )

    def diff(self, periods=1, axis=0):
        axis = _validate_axis(axis)
        if axis == 0:
            return new_collection(Diff(self, periods))
        return self.map_partitions(
            func=Diff.func,
            enforce_metadata=False,
            transform_divisions=False,
            clear_divisions=False,
            periods=periods,
            axis=axis,
        )

    def rename_axis(
        self, mapper=no_default, index=no_default, columns=no_default, axis=0
    ):
        return new_collection(self.expr.rename_axis(mapper, index, columns, axis))

    def _create_alignable_frame(self, other, join):
        if not is_dask_collection(other):
            if join in ("inner", "left"):
                npartitions = 1
            else:
                # We have to trigger alignment, otherwise pandas will add
                # the same values to every partition
                npartitions = 2
            other = from_pandas(other, npartitions=npartitions)
        return other

    def align(self, other, join="outer", axis=None, fill_value=None):
        other = self._create_alignable_frame(other, join)
        return self.expr.align(other.expr, join, axis, fill_value)

    def nunique_approx(self, split_every=None):
        return new_collection(self.expr.nunique_approx(split_every=split_every))

    def cumsum(self, skipna=True):
        return new_collection(self.expr.cumsum(skipna=skipna))

    def cumprod(self, skipna=True):
        return new_collection(self.expr.cumprod(skipna=skipna))

    def cummax(self, skipna=True):
        return new_collection(self.expr.cummax(skipna=skipna))

    def cummin(self, skipna=True):
        return new_collection(self.expr.cummin(skipna=skipna))

    def memory_usage_per_partition(self, index=True, deep=False):
        return new_collection(self.expr.memory_usage_per_partition(index, deep))

    @property
    def loc(self):
        from dask_expr._indexing import LocIndexer

        return LocIndexer(self)

    def notnull(self):
        return new_collection(expr.NotNull(self))

    def isnull(self):
        return ~self.notnull()

    @classmethod
    def from_dict(
        cls, data, *, npartitions=1, orient="columns", dtype=None, columns=None
    ):
        return from_dict(data, npartitions, orient, dtype=dtype, columns=columns)

    def to_json(self, filename, *args, **kwargs):
        """See dd.to_json docstring for more information"""
        from dask.dataframe.io import to_json

        return to_json(self, filename, *args, **kwargs)

    def to_sql(
        self,
        name: str,
        uri: str,
        schema=None,
        if_exists: str = "fail",
        index: bool = True,
        index_label=None,
        chunksize=None,
        dtype=None,
        method=None,
        compute=True,
        parallel=False,
        engine_kwargs=None,
    ):
        from dask_expr.io.sql import to_sql

        return to_sql(
            self,
            name,
            uri,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=method,
            compute=compute,
            parallel=parallel,
            engine_kwargs=engine_kwargs,
        )

    def to_orc(self, path, *args, **kwargs):
        """See dd.to_orc docstring for more information"""
        from dask_expr.io.orc import to_orc

        return to_orc(self, path, *args, **kwargs)

    def to_csv(self, filename, **kwargs):
        """See dd.to_csv docstring for more information"""
        from dask_expr.io.csv import to_csv

        return to_csv(self, filename, **kwargs)

    def to_records(self, index=False, lengths=None):
        from dask_expr.io.records import to_records

        if lengths is True:
            lengths = tuple(self.map_partitions(len).compute())

        frame = self.to_dask_dataframe()
        records = to_records(frame)

        chunks = frame._validate_chunks(records, lengths)
        records._chunks = (chunks[0],)

        return records

    def to_bag(self, index=False, format="tuple"):
        """Create a Dask Bag from a Series"""
        from dask_expr.io.bag import to_bag

        return to_bag(self, index, format=format)

    def to_hdf(self, path_or_buf, key, mode="a", append=False, **kwargs):
        """See dd.to_hdf docstring for more information"""
        from dask_expr.io.hdf import to_hdf

        return to_hdf(self, path_or_buf, key, mode, append, **kwargs)


# Add operator attributes
for op in [
    "__add__",
    "__radd__",
    "__sub__",
    "__rsub__",
    "__mul__",
    "__rmul__",
    "__mod__",
    "__rmod__",
    "__truediv__",
    "__rtruediv__",
    "__pow__",
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

    _accessors: ClassVar[set[str]] = set()
    _partition_type = pd.DataFrame

    @property
    def shape(self):
        return self.size / max(len(self.columns), 1), len(self.columns)

    def keys(self):
        return self.columns

    def __iter__(self):
        return iter(self.columns)

    def items(self):
        for i, name in enumerate(self.columns):
            yield (name, self.iloc[:, i])

    @property
    def axes(self):
        return [self.index, self.columns]

    def assign(self, **pairs):
        result = self
        for k, v in pairs.items():
            v = _maybe_from_pandas([v])[0]
            if not isinstance(k, str):
                raise TypeError(f"Column name cannot be type {type(k)}")

            if callable(v):
                v = v(result)

            if isinstance(v, (Scalar, Series)):
                if isinstance(v, Series):
                    if not expr.are_co_aligned(self.expr, v.expr):
                        raise NotImplementedError(
                            "Setting a Series with a different base is not supported",
                        )

                result = new_collection(expr.Assign(result, k, v))
            elif not isinstance(v, FrameBase) and isinstance(v, Hashable):
                result = new_collection(expr.Assign(result, k, v))
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
        shuffle_method=None,
        npartitions=None,
        broadcast=None,
    ):
        """Merge the DataFrame with another DataFrame

        Parameters
        ----------
        right: FrameBase or pandas DataFrame
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
        shuffle_method: optional
            Shuffle method to use if shuffling is necessary.
        npartitions : int, optional
            The number of output partitions
        broadcast : float, bool, optional
            Whether to use a broadcast-based join in lieu of a shuffle-based join for
            supported cases. By default, a simple heuristic will be used to select
            the underlying algorithm. If a floating-point value is specified, that
            number will be used as the broadcast_bias within the simple heuristic
            (a large number makes Dask more likely to choose the broacast_join code
            path). See broadcast_join for more information.
        """
        return merge(
            self,
            right,
            how,
            on,
            left_on,
            right_on,
            left_index,
            right_index,
            suffixes,
            indicator,
            shuffle_method,
            npartitions=npartitions,
            broadcast=broadcast,
        )

    def join(
        self,
        other,
        on=None,
        how="left",
        lsuffix="",
        rsuffix="",
        shuffle_method=None,
        npartitions=None,
    ):
        if not isinstance(other, list) and not is_dask_collection(other):
            other = from_pandas(other, npartitions=1)
        if (
            not isinstance(other, list)
            and not is_dataframe_like(other._meta)
            and hasattr(other._meta, "name")
        ):
            other = new_collection(expr.ToFrame(other))

        if not isinstance(other, FrameBase):
            if not isinstance(other, list) or not all(
                isinstance(o, FrameBase) for o in other
            ):
                raise ValueError("other must be DataFrame or list of DataFrames")
            if how not in ("outer", "left"):
                raise ValueError("merge_multi only supports left or outer joins")

            other = [
                from_pandas(o, npartitions=1) if not is_dask_collection(o) else o
                for o in other
            ]

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
            shuffle_method=shuffle_method,
            npartitions=npartitions,
        )

    def groupby(
        self, by, group_keys=True, sort=None, observed=None, dropna=None, **kwargs
    ):
        from dask_expr._groupby import GroupBy

        if isinstance(by, FrameBase) and not isinstance(by, Series):
            raise ValueError(
                f"`by` must be a column name or list of columns, got {by}."
            )

        return GroupBy(
            self,
            by,
            group_keys=group_keys,
            sort=sort,
            observed=observed,
            dropna=dropna,
            **kwargs,
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

    def map(self, func, na_action=None, meta=None):
        return new_collection(expr.Map(self, arg=func, na_action=na_action, meta=meta))

    def __repr__(self):
        return f"<dask_expr.expr.DataFrame: expr={self.expr}>"

    def nlargest(self, n=5, columns=None):
        return new_collection(NLargest(self, n=n, _columns=columns))

    def nsmallest(self, n=5, columns=None):
        return new_collection(NSmallest(self, n=n, _columns=columns))

    def memory_usage(self, deep=False, index=True):
        return new_collection(MemoryUsageFrame(self, deep=deep, _index=index))

    def combine(self, other, func, fill_value=None, overwrite=True):
        other = self._create_alignable_frame(other, "outer")
        left, right = self.expr._align_divisions(other.expr, axis=0)
        return new_collection(
            expr.CombineFrame(left, right, func, fill_value, overwrite)
        )

    def drop_duplicates(
        self,
        subset=None,
        ignore_index=False,
        split_every=None,
        split_out=True,
        shuffle_method=None,
        keep=None,
    ):
        shuffle_method = _get_shuffle_preferring_order(shuffle_method)
        if keep is False:
            raise NotImplementedError("drop_duplicates with keep=False")
        if keep is not None and get_default_shuffle_method() == "p2p":
            warnings.warn(
                "P2P shuffle doesn't have ordering guarantees, so keep='first' and "
                "keep='last' might return unexpected results",
                UserWarning,
            )
        elif keep is None:
            keep = "first"
        # Fail early if subset is not valid, e.g. missing columns
        subset = _convert_to_list(subset)
        meta_nonempty(self._meta).drop_duplicates(subset=subset, keep=keep)
        return new_collection(
            DropDuplicates(
                self,
                subset=subset,
                ignore_index=ignore_index,
                split_out=split_out,
                split_every=split_every,
                shuffle_method=shuffle_method,
                keep=keep,
            )
        )

    def dropna(self, how=no_default, subset=None, thresh=no_default):
        if how is not no_default and thresh is not no_default:
            raise TypeError(
                "You cannot set both the how and thresh arguments at the same time."
            )
        subset = _convert_to_list(subset)
        return new_collection(
            expr.DropnaFrame(self, how=how, subset=subset, thresh=thresh)
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
            expr.Sample(self, state_data=state_data, frac=frac, replace=replace)
        )

    def rename(self, columns):
        return new_collection(expr.RenameFrame(self, columns=columns))

    def explode(self, column):
        column = _convert_to_list(column)
        return new_collection(expr.ExplodeFrame(self, column=column))

    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        if columns is None and labels is None:
            raise TypeError("must either specify 'columns' or 'labels'")

        axis = _validate_axis(axis)

        if axis == 1:
            columns = labels or columns
        elif axis == 0 and columns is None:
            raise NotImplementedError(
                "Drop currently only works for axis=1 or when columns is not None"
            )
        return new_collection(expr.Drop(self, columns=columns, errors=errors))

    def to_parquet(self, path, **kwargs):
        from dask_expr.io.parquet import to_parquet

        return to_parquet(self, path, **kwargs)

    def select_dtypes(self, include=None, exclude=None):
        columns = list(
            self._meta.select_dtypes(include=include, exclude=exclude).columns
        )
        return new_collection(self.expr[columns])

    def eval(self, expr, **kwargs):
        if "inplace" in kwargs:
            raise NotImplementedError("inplace is not supported for eval")
        return new_collection(Eval(self, _expr=expr, expr_kwargs=kwargs))

    def set_index(
        self,
        other,
        drop=True,
        sorted=False,
        npartitions: int | None = None,
        divisions=None,
        sort: bool = True,
        shuffle_method=None,
        upsample: float = 1.0,
        partition_size: float = 128e6,
        append: bool = False,
        **options,
    ):
        if isinstance(other, list) and len(other) == 1:
            other = other[0]
        if isinstance(other, list):
            if any([isinstance(c, FrameBase) for c in other]):
                raise TypeError("List[FrameBase] not supported by set_index")
            elif not sorted:
                raise NotImplementedError(
                    "Dask dataframe does not yet support multi-indexes.\n"
                    f"You tried to index with this index: {other}\n"
                    "Indexes must be single columns only."
                )
        if isinstance(other, DataFrame):
            raise NotImplementedError(
                "Dask dataframe does not yet support multi-indexes.\n"
                f"You tried to index with a frame with these columns: {list(other.columns)}\n"
                "Indexes must be single columns only."
            )
        if isinstance(other, Series):
            if other._name == self.index._name:
                return self
        elif other == self.index.name:
            return self

        if divisions is not None:
            check_divisions(divisions)

        if (sorted or not sort) and npartitions is not None:
            raise ValueError(
                "Specifying npartitions with sort=False or sorted=True is not "
                "supported. Call `repartition` afterwards."
            )

        if sorted:
            if divisions is not None and len(divisions) - 1 != self.npartitions:
                msg = (
                    "When doing `df.set_index(col, sorted=True, divisions=...)`, "
                    "divisions indicates known splits in the index column. In this "
                    "case divisions must be the same length as the existing "
                    "divisions in `df`\n\n"
                    "If the intent is to repartition into new divisions after "
                    "setting the index, you probably want:\n\n"
                    "`df.set_index(col, sorted=True).repartition(divisions=divisions)`"
                )
                raise ValueError(msg)
            return new_collection(
                SetIndexBlockwise(
                    self, other, drop, new_divisions=divisions, append=append
                )
            )
        elif not sort:
            return new_collection(
                SetIndexBlockwise(self, other, drop, None, append=append)
            )

        return new_collection(
            SetIndex(
                self,
                other,
                drop,
                user_divisions=divisions,
                npartitions=npartitions,
                upsample=upsample,
                partition_size=partition_size,
                shuffle_method=shuffle_method,
                append=append,
                options=options,
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
        ignore_index: bool | None = False,
        shuffle_method: str | None = None,
        **options,
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

        if not isinstance(ascending, bool) and self.npartitions > 1:
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
                self,
                by,
                ascending,
                na_position,
                npartitions,
                partition_size,
                sort_function,
                sort_function_kwargs,
                upsample,
                ignore_index,
                shuffle_method,
                options=options,
            )
        )

    def query(self, expr, **kwargs):
        return new_collection(Query(self, expr, kwargs))

    def mode(self, dropna=True, split_every=False):
        modes = []
        for _, col in self.items():
            modes.append(col.mode(dropna=dropna, split_every=split_every))
        return concat(modes, axis=1)

    def add_prefix(self, prefix):
        return new_collection(expr.AddPrefix(self, prefix))

    def add_suffix(self, suffix):
        return new_collection(expr.AddSuffix(self, suffix))

    def pivot_table(self, index, columns, values, aggfunc="mean"):
        return pivot_table(self, index, columns, values, aggfunc)

    @property
    def iloc(self):
        from dask_expr._indexing import ILocIndexer

        return ILocIndexer(self)

    def categorize(self, columns=None, index=None, split_every=None, **kwargs):
        """Convert columns of the DataFrame to category dtype.

        .. warning:: This method eagerly computes the categories of the chosen columns.

        Parameters
        ----------
        columns : list, optional
            A list of column names to convert to categoricals. By default any
            column with an object dtype is converted to a categorical, and any
            unknown categoricals are made known.
        index : bool, optional
            Whether to categorize the index. By default, object indices are
            converted to categorical, and unknown categorical indices are made
            known. Set True to always categorize the index, False to never.
        split_every : int, optional
            Group partitions into groups of this size while performing a
            tree-reduction. If set to False, no tree-reduction will be used.
        kwargs
            Keyword arguments are passed on to compute.
        """
        df = self
        meta = df._meta
        if columns is None:
            columns = list(meta.select_dtypes(["object", "string", "category"]).columns)
        elif is_scalar(columns):
            columns = [columns]

        # Filter out known categorical columns
        columns = [
            c
            for c in columns
            if not (is_categorical_dtype(meta[c]) and has_known_categories(meta[c]))
        ]

        if index is not False:
            if is_categorical_dtype(meta.index):
                index = not has_known_categories(meta.index)
            elif index is None:
                index = str(meta.index.dtype) in ("object", "string")

        # Nothing to do
        if not len(columns) and index is False:
            return df

        from dask_expr._collection import new_collection

        # Eagerly compute the categories
        categories, index = new_collection(
            GetCategories(self, columns=columns, index=index, split_every=split_every)
        ).compute()

        # Some operations like get_dummies() rely on the order of categories
        categories = {k: v.sort_values() for k, v in categories.items()}

        # Categorize each partition
        return new_collection(Categorize(self, categories, index))

    def nunique(self, axis=0, dropna=True):
        if axis == 1:
            return new_collection(expr.NUniqueColumns(self, axis=axis, dropna=dropna))
        else:
            return concat(
                [
                    col.nunique(dropna=dropna).to_series(name)
                    for name, col in self.items()
                ]
            )

    def quantile(self, q=0.5, axis=0, numeric_only=False, method="default"):
        """Approximate row-wise and precise column-wise quantiles of DataFrame

        Parameters
        ----------
        q : list/array of floats, default 0.5 (50%)
            Iterable of numbers ranging from 0 to 1 for the desired quantiles
        axis : {0, 1, 'index', 'columns'} (default 0)
            0 or 'index' for row-wise, 1 or 'columns' for column-wise
        method : {'default', 'tdigest', 'dask'}, optional
            What method to use. By default will use dask's internal custom
            algorithm (``'dask'``).  If set to ``'tdigest'`` will use tdigest
            for floats and ints and fallback to the ``'dask'`` otherwise.
        """
        allowed_methods = ["default", "dask", "tdigest"]
        if method not in allowed_methods:
            raise ValueError("method can only be 'default', 'dask' or 'tdigest'")
        meta = make_meta(self._meta.quantile(q=q, numeric_only=numeric_only))

        if numeric_only:
            frame = self.select_dtypes("number")
        else:
            frame = self

        collections = []
        for _, col in frame.items():
            collections.append(col.quantile(q=q, method=method))

        if len(collections) > 0 and isinstance(collections[0], Scalar):
            return _from_scalars(collections, meta, frame.expr.columns)

        return concat(collections, axis=1)

    def median(self, axis=0, numeric_only=False):
        if axis == 1 or self.npartitions == 1:
            return self.median_approximate(axis=axis, numeric_only=numeric_only)
        raise NotImplementedError(
            "Dask doesn't implement an exact median in all cases as this is hard to do in parallel. "
            "See the `median_approximate` method instead, which uses an approximate algorithm."
        )

    def median_approximate(self, axis=0, method="default", numeric_only=False):
        return self.quantile(
            axis=axis, method=method, numeric_only=numeric_only
        ).rename(None)

    def describe(
        self,
        split_every=False,
        percentiles=None,
        percentiles_method="default",
        include=None,
        exclude=None,
    ):
        # TODO: duplicated columns
        if include is None and exclude is None:
            _include = [np.number, np.timedelta64, np.datetime64]
            columns = self._meta.select_dtypes(include=_include).columns
            if len(columns) == 0:
                columns = self._meta.columns
        elif include == "all":
            if exclude is not None:
                raise ValueError("exclude must be None when include is 'all'")
            columns = self._meta.columns
        else:
            columns = self._meta.select_dtypes(include=include, exclude=exclude).columns

        stats = [
            self[col].describe(
                split_every=split_every,
                percentiles=percentiles,
                percentiles_method=percentiles_method,
            )
            for col in columns
        ]
        return concat(stats, axis=1)

    def info(self, buf=None, verbose=False, memory_usage=False):
        """
        Concise summary of a Dask DataFrame
        """
        if buf is None:
            import sys

            buf = sys.stdout
        lines = [str(type(self)).replace("._collection", "")]

        if len(self.columns) == 0:
            lines.append(f"{type(self.index._meta).__name__}: 0 entries")
            lines.append(f"Empty {type(self).__name__}")
            put_lines(buf, lines)
            return

        # Group and execute the required computations
        computations = {}
        if verbose:
            computations.update({"index": self.index, "count": self.count()})
        if memory_usage:
            computations["memory_usage"] = self.memory_usage(deep=True, index=True)

        computations = dict(zip(computations.keys(), compute(*computations.values())))

        if verbose:
            import textwrap

            index = computations["index"]
            counts = computations["count"]
            lines.append(index_summary(index))
            lines.append(f"Data columns (total {len(self.columns)} columns):")

            from pandas.io.formats.printing import pprint_thing

            space = max(len(pprint_thing(k)) for k in self.columns) + 1
            column_width = max(space, 7)

            header = (
                textwrap.dedent(
                    """\
             #   {{column:<{column_width}}} Non-Null Count  Dtype
            ---  {{underl:<{column_width}}} --------------  -----"""
                )
                .format(column_width=column_width)
                .format(column="Column", underl="------")
            )
            column_template = textwrap.dedent(
                """\
            {{i:^3}}  {{name:<{column_width}}} {{count}} non-null      {{dtype}}""".format(
                    column_width=column_width
                )
            )
            column_info = [
                column_template.format(
                    i=pprint_thing(i),
                    name=pprint_thing(name),
                    count=pprint_thing(count),
                    dtype=pprint_thing(dtype),
                )
                for i, (name, count, dtype) in enumerate(
                    # NOTE: Use `counts.values` for cudf support
                    zip(self.columns, counts.values, self.dtypes)
                )
            ]
            lines.extend(header.split("\n"))
        else:
            column_info = [index_summary(self.columns, name="Columns")]

        lines.extend(column_info)
        dtype_counts = [
            "%s(%d)" % k for k in sorted(self.dtypes.value_counts().items(), key=str)
        ]
        lines.append("dtypes: {}".format(", ".join(dtype_counts)))

        if memory_usage:
            memory_int = computations["memory_usage"].sum()
            lines.append(f"memory usage: {memory_repr(memory_int)}\n")

        put_lines(buf, lines)


class Series(FrameBase):
    """Series-like Expr Collection"""

    _accessors: ClassVar[set[str]] = set()
    _partition_type = pd.Series

    @property
    def shape(self):
        return (self.size,)

    @property
    def axes(self):
        return [self.index]

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(set(dir(expr.Expr)))
        for accessor in ["cat", "str"]:
            if not hasattr(self._meta, accessor):
                o.remove(accessor)
        return list(o)

    @property
    def name(self):
        return self.expr._meta.name

    @name.setter
    def name(self, name):
        self._expr = self.rename(index=name)._expr

    @property
    def dtype(self):
        return self.expr._meta.dtype

    @property
    def nbytes(self):
        return new_collection(self.expr.nbytes)

    def keys(self):
        return self.index

    def map(self, arg, na_action=None, meta=None):
        if isinstance(arg, Series):
            if not expr.are_co_aligned(self.expr, arg.expr):
                if not self.divisions == arg.divisions:
                    raise NotImplementedError(
                        "passing a Series as arg isn't implemented yet"
                    )
        return new_collection(expr.Map(self, arg=arg, na_action=na_action, meta=meta))

    def __repr__(self):
        return f"<dask_expr.expr.Series: expr={self.expr}>"

    def to_frame(self, name=no_default):
        return new_collection(expr.ToFrame(self, name=name))

    def value_counts(
        self,
        sort=None,
        ascending=False,
        dropna=True,
        normalize=False,
        split_every=None,
        split_out=1,
    ):
        length = None
        if (split_out > 1 or split_out is True) and normalize:
            frame = self if not dropna else self.dropna()
            length = Len(frame)

        return new_collection(
            ValueCounts(
                self, sort, ascending, dropna, normalize, split_every, split_out, length
            )
        )

    def mode(self, dropna=True, split_every=False):
        return new_collection(self.expr.mode(dropna, split_every))

    def nlargest(self, n=5):
        return new_collection(NLargest(self, n=n))

    def nsmallest(self, n=5):
        return new_collection(NSmallest(self, n=n))

    def memory_usage(self, deep=False, index=True):
        return new_collection(MemoryUsageFrame(self, deep=deep, _index=index))

    def unique(self, split_every=None, split_out=True, shuffle_method=None):
        shuffle_method = _get_shuffle_preferring_order(shuffle_method)
        return new_collection(Unique(self, split_every, split_out, shuffle_method))

    def nunique(self, dropna=True):
        uniqs = self.drop_duplicates()
        if dropna:
            # count mimics pandas behavior and excludes NA values
            if isinstance(uniqs, Index):
                uniqs = uniqs.to_series()
            return uniqs.count()
        else:
            return uniqs.size

    def drop_duplicates(
        self,
        ignore_index=False,
        split_every=None,
        split_out=True,
        shuffle_method=None,
        keep=None,
    ):
        shuffle_method = _get_shuffle_preferring_order(shuffle_method)
        if keep is False:
            raise NotImplementedError("drop_duplicates with keep=False")
        if keep is not None and shuffle_method == "p2p":
            warnings.warn(
                "P2P shuffle doesn't have ordering guarantees, so keep='first' and "
                "keep='last' might return unexpected results",
                UserWarning,
            )
        elif keep is None:
            keep = "first"
        return new_collection(
            DropDuplicates(
                self,
                ignore_index=ignore_index,
                split_out=split_out,
                split_every=split_every,
                shuffle_method=shuffle_method,
                keep=keep,
            )
        )

    def dropna(self):
        return new_collection(expr.DropnaSeries(self))

    def between(self, left, right, inclusive="both"):
        return new_collection(
            expr.Between(self, left=left, right=right, inclusive=inclusive)
        )

    def combine(self, other, func, fill_value=None):
        other = self._create_alignable_frame(other, "outer")
        left, right = self.expr._align_divisions(other.expr, axis=0)
        return new_collection(expr.CombineSeries(left, right, func, fill_value))

    def explode(self):
        return new_collection(expr.ExplodeSeries(self))

    def add_prefix(self, prefix):
        return new_collection(expr.AddPrefixSeries(self, prefix))

    def add_suffix(self, suffix):
        return new_collection(expr.AddSuffixSeries(self, suffix))

    cat = CachedAccessor("cat", CategoricalAccessor)
    dt = CachedAccessor("dt", DatetimeAccessor)
    str = CachedAccessor("str", StringAccessor)

    def _repartition_quantiles(self, npartitions, upsample=1.0, random_state=None):
        return new_collection(
            RepartitionQuantiles(self, npartitions, upsample, random_state)
        )

    def groupby(self, by, **kwargs):
        from dask_expr._groupby import SeriesGroupBy

        return SeriesGroupBy(self, by, **kwargs)

    def rename(self, index, sorted_index=False):
        return new_collection(expr.RenameSeries(self, index, sorted_index))

    def quantile(self, q=0.5, method="default"):
        """Approximate quantiles of Series

        Parameters
        ----------
        q : list/array of floats, default 0.5 (50%)
            Iterable of numbers ranging from 0 to 1 for the desired quantiles
        method : {'default', 'tdigest', 'dask'}, optional
            What method to use. By default will use dask's internal custom
            algorithm (``'dask'``).  If set to ``'tdigest'`` will use tdigest
            for floats and ints and fallback to the ``'dask'`` otherwise.
        """
        _raise_if_object_series(self, "quantile")
        allowed_methods = ["default", "dask", "tdigest"]
        if method not in allowed_methods:
            raise ValueError("method can only be 'default', 'dask' or 'tdigest'")
        return new_collection(SeriesQuantile(self, q, method))

    def median(self):
        if self.npartitions == 1:
            return self.median_approximate()
        raise NotImplementedError(
            "Dask doesn't implement an exact median in all cases as this is hard to do in parallel. "
            "See the `median_approximate` method instead, which uses an approximate algorithm."
        )

    def median_approximate(self, method="default"):
        return self.quantile(method=method)

    def describe(
        self,
        split_every=False,
        percentiles=None,
        percentiles_method="default",
        include=None,
        exclude=None,
    ):
        if (
            is_numeric_dtype(self.dtype)
            and not is_bool_dtype(self.dtype)
            or is_timedelta64_dtype(self.dtype)
            or is_datetime64_any_dtype(self.dtype)
        ):
            return new_collection(
                DescribeNumeric(self, split_every, percentiles, percentiles_method)
            )
        else:
            return new_collection(
                DescribeNonNumeric(self, split_every, percentiles, percentiles_method)
            )

    @property
    def is_monotonic_increasing(self):
        return new_collection(IsMonotonicIncreasing(self))

    @property
    def is_monotonic_decreasing(self):
        return new_collection(IsMonotonicDecreasing(self))


for name in [
    "add",
    "sub",
    "mul",
    "div",
    "divide",
    "truediv",
    "floordiv",
    "mod",
    "pow",
    "radd",
    "rsub",
    "rmul",
    "rdiv",
    "rtruediv",
    "rfloordiv",
    "rmod",
    "rpow",
]:
    assert not hasattr(DataFrame, name), name
    setattr(DataFrame, name, _wrap_expr_method_operator(name, DataFrame))

    assert not hasattr(Series, name), name
    setattr(Series, name, _wrap_expr_method_operator(name, Series))


class Index(Series):
    """Index-like Expr Collection"""

    _accessors: ClassVar[set[str]] = set()
    _partition_type = pd.Index

    _dt_attributes = {
        "nanosecond",
        "microsecond",
        "millisecond",
        "dayofyear",
        "minute",
        "hour",
        "day",
        "dayofweek",
        "second",
        "week",
        "weekday",
        "weekofyear",
        "month",
        "quarter",
        "year",
    }

    _cat_attributes = {
        "known",
        "as_known",
        "as_unknown",
        "add_categories",
        "categories",
        "remove_categories",
        "reorder_categories",
        "as_ordered",
        "codes",
        "remove_unused_categories",
        "set_categories",
        "as_unordered",
        "ordered",
        "rename_categories",
    }

    def __getattr__(self, key):
        if (
            isinstance(self._meta.dtype, pd.CategoricalDtype)
            and key in self._cat_attributes
        ):
            return getattr(self.cat, key)
        elif key in self._dt_attributes:
            return getattr(self.dt, key)

        if hasattr(super(), key):  # Doesn't trigger super().__getattr__
            # Not a magic attribute. This is a real method or property of Series that
            # has been overridden by RaiseAttributeError().
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute {key!r}"
            )
        return super().__getattr__(key)

    def __repr__(self):
        return f"<dask_expr.expr.Index: expr={self.expr}>"

    def to_series(self, index=None, name=no_default):
        if index is not None:
            raise NotImplementedError
        return new_collection(expr.ToSeriesIndex(self, index=index, name=name))

    def to_frame(self, index=True, name=no_default):
        if not index:
            raise NotImplementedError
        return new_collection(expr.ToFrameIndex(self, index=index, name=name))

    def memory_usage(self, deep=False):
        return new_collection(MemoryUsageIndex(self, deep=deep))

    def shift(self, periods=1, freq=None):
        return new_collection(expr.ShiftIndex(self, periods, freq))

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(set(dir(expr.Expr)))
        o.update(self._dt_attributes)
        if isinstance(self.dtype, pd.CategoricalDtype):
            o.update(self._cat_attributes)
        return list(o)

    # Methods and properties of Series that are not implemented on Index
    index = RaiseAttributeError()
    sum = RaiseAttributeError()
    prod = RaiseAttributeError()
    count = RaiseAttributeError()
    mean = RaiseAttributeError()
    std = RaiseAttributeError()
    var = RaiseAttributeError()
    idxmin = RaiseAttributeError()
    idxmax = RaiseAttributeError()


class Scalar(FrameBase):
    """Scalar Expr Collection"""

    def __repr__(self):
        return f"<dask_expr.expr.Scalar: expr={self.expr}>"

    def __bool__(self):
        raise TypeError(
            f"Trying to convert {self} to a boolean value. Because Dask objects are "
            "lazily evaluated, they cannot be converted to a boolean value or used "
            "in boolean conditions like if statements. Try calling .compute() to "
            "force computation prior to converting to a boolean value or using in "
            "a conditional statement."
        )

    def __dask_postcompute__(self):
        return first, ()

    def to_series(self, index=0) -> Series:
        return new_collection(expr.ScalarToSeries(self, index=index))


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


def from_pandas(data, npartitions=None, sort=True, chunksize=None):
    if chunksize is not None and npartitions is not None:
        raise TypeError("can't pass chunksize and npartitions")
    elif chunksize is None and npartitions is None:
        npartitions = 1

    from dask_expr.io.io import FromPandas

    return new_collection(
        FromPandas(
            _BackendData(data.copy()),
            npartitions=npartitions,
            sort=sort,
            chunksize=chunksize,
        )
    )


def from_array(arr, chunksize=50_000, columns=None, meta=None):
    import dask.array as da

    if isinstance(arr, da.Array):
        return from_dask_array(arr, columns=columns, meta=meta)

    from dask_expr.io.io import FromArray

    return new_collection(
        FromArray(
            arr,
            chunksize=chunksize,
            original_columns=columns,
            meta=meta,
        )
    )


def from_graph(*args, **kwargs):
    from dask_expr.io.io import FromGraph

    return new_collection(FromGraph(*args, **kwargs))


def from_dict(
    data,
    npartitions,
    orient="columns",
    dtype=None,
    columns=None,
    constructor=pd.DataFrame,
):
    """
    Construct a Dask DataFrame from a Python Dictionary

    Parameters
    ----------
    data : dict
        Of the form {field : array-like} or {field : dict}.
    npartitions : int
        The number of partitions of the index to create. Note that depending on
        the size and index of the dataframe, the output may have fewer
        partitions than requested.
    orient : {'columns', 'index', 'tight'}, default 'columns'
        The "orientation" of the data. If the keys of the passed dict
        should be the columns of the resulting DataFrame, pass 'columns'
        (default). Otherwise if the keys should be rows, pass 'index'.
        If 'tight', assume a dict with keys
        ['index', 'columns', 'data', 'index_names', 'column_names'].
    dtype: bool
        Data type to force, otherwise infer.
    columns: string, optional
        Column labels to use when ``orient='index'``. Raises a ValueError
        if used with ``orient='columns'`` or ``orient='tight'``.
    constructor: class, default pd.DataFrame
        Class with which ``from_dict`` should be called with.

    Examples
    --------
    >>> import dask.dataframe as dd
    >>> ddf = dd.from_dict({"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]}, npartitions=2)
    """

    collection_types = {type(v) for v in data.values() if is_dask_collection(v)}
    if collection_types:
        raise NotImplementedError(
            "from_dict doesn't currently support Dask collections as inputs. "
            f"Objects of type {collection_types} were given in the input dict."
        )

    return from_pandas(
        constructor.from_dict(data, orient, dtype, columns),
        npartitions,
    )


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


def from_dask_array(x, columns=None, index=None, meta=None):
    from dask.dataframe.io import from_dask_array

    df = from_dask_array(x, columns=columns, index=index, meta=meta)
    return from_dask_dataframe(df, optimize=True)


def read_csv(path, *args, usecols=None, **kwargs):
    from dask_expr.io.csv import ReadCSV

    if not isinstance(path, str):
        path = stringify_path(path)
    return new_collection(ReadCSV(path, *args, columns=usecols, **kwargs))


read_table = read_csv


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
    interleave_partitions=False,
    **kwargs,
):
    if not isinstance(dfs, list):
        raise TypeError("dfs must be a list of DataFrames/Series objects")
    if len(dfs) == 0:
        raise ValueError("No objects to concatenate")
    if len(dfs) == 1:
        if axis == 1 and isinstance(dfs[0], Series):
            return dfs[0].to_frame()
        return dfs[0]

    if join not in ("inner", "outer"):
        raise ValueError("'join' must be 'inner' or 'outer'")

    dfs = [from_pandas(df) if not isinstance(df, FrameBase) else df for df in dfs]

    if axis == 1:
        dfs = [df for df in dfs if len(df.columns) > 0 or isinstance(df, Series)]

    return new_collection(
        Concat(
            join,
            ignore_order,
            kwargs,
            axis,
            ignore_unknown_divisions,
            interleave_partitions,
            *dfs,
        )
    )


def merge(
    left,
    right,
    how="inner",
    on=None,
    left_on=None,
    right_on=None,
    left_index=False,
    right_index=False,
    suffixes=("_x", "_y"),
    indicator=False,
    shuffle_method=None,
    npartitions=None,
    broadcast=None,
):
    for o in [on, left_on, right_on]:
        if isinstance(o, FrameBase):
            raise NotImplementedError()
    if not on and not left_on and not right_on and not left_index and not right_index:
        on = [c for c in left.columns if c in right.columns]
        if not on:
            left_index = right_index = True

    if on and not left_on and not right_on:
        left_on = right_on = on

    supported_how = ("left", "right", "outer", "inner")
    if how not in supported_how:
        raise ValueError(
            f"dask.dataframe.merge does not support how='{how}'."
            f"Options are: {supported_how}."
        )

    # Transform pandas objects into dask.dataframe objects
    if not is_dask_collection(left):
        if right_index and left_on:  # change to join on index
            left = left.set_index(left[left_on])
            left_on = None
            left_index = True
        left = from_pandas(left, npartitions=1)

    if not is_dask_collection(right):
        if left_index and right_on:  # change to join on index
            right = right.set_index(right[right_on])
            right_on = None
            right_index = True
        right = from_pandas(right, npartitions=1)

    assert is_dataframe_like(right._meta)
    if left_on and right_on:
        warn_dtype_mismatch(left, right, left_on, right_on)

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
            shuffle_method=shuffle_method,
            _npartitions=npartitions,
            broadcast=broadcast,
        )
    )


def from_map(
    func,
    *iterables,
    args=None,
    meta=no_default,
    divisions=None,
    label=None,
    enforce_metadata=False,
    **kwargs,
):
    """Create a dask-expr collection from a custom function map

    NOTE: The underlying ``Expr`` object produced by this API
    will support column projection (via ``simplify``) if
    the ``func`` argument has "columns" in its signature.
    """
    from dask.dataframe.io.utils import DataFrameIOFunction

    from dask_expr.io import FromMap, FromMapProjectable

    if "token" in kwargs:
        # This option doesn't really make sense in dask-expr
        raise NotImplementedError("dask_expr does not support a token argument.")

    # Check if `func` supports column projection
    allow_projection = True
    if "columns" in inspect.signature(func).parameters:
        allow_projection = True
    elif isinstance(func, DataFrameIOFunction):
        warnings.warn(
            "dask_expr does not support the DataFrameIOFunction "
            "protocol for column projection. To enable column "
            "projection, please ensure that the signature of `func` "
            "includes a `columns=` keyword argument instead."
        )
    else:
        allow_projection = False

    args = [] if args is None else args
    kwargs = {} if kwargs is None else kwargs
    if allow_projection:
        columns = kwargs.pop("columns", None)
        return new_collection(
            FromMapProjectable(
                func,
                iterables,
                columns,
                args,
                kwargs,
                meta,
                enforce_metadata,
                divisions,
                label,
            )
        )
    else:
        return new_collection(
            FromMap(
                func,
                iterables,
                args,
                kwargs,
                meta,
                enforce_metadata,
                divisions,
                label,
            )
        )


def repartition(df, divisions, force=False):
    if isinstance(df, FrameBase):
        return df.repartition(divisions=divisions, force=force)
    elif is_dataframe_like(df) or is_series_like(df):
        return new_collection(
            FromPandasDivisions(_BackendData(df), divisions=divisions)
        )
    else:
        raise NotImplementedError(f"repartition is not implemented for {type(df)}.")


def pivot_table(df, index, columns, values, aggfunc="mean"):
    if not is_scalar(index) or index not in df._meta.columns:
        raise ValueError("'index' must be the name of an existing column")
    if not is_scalar(columns) or columns not in df._meta.columns:
        raise ValueError("'columns' must be the name of an existing column")
    if not methods.is_categorical_dtype(df._meta[columns]):
        raise ValueError("'columns' must be category dtype")
    if not has_known_categories(df._meta[columns]):
        raise ValueError("'columns' categories must be known")

    if not (
        is_scalar(values)
        and values in df._meta.columns
        or not is_scalar(values)
        and all(is_scalar(x) and x in df._meta.columns for x in values)
    ):
        raise ValueError("'values' must refer to an existing column or columns")

    return new_collection(
        PivotTable(df, index=index, columns=columns, values=values, aggfunc=aggfunc)
    )


def to_numeric(arg, errors="raise", downcast=None):
    if not isinstance(arg, Series):
        raise TypeError("arg must be a Series")
    return new_collection(ToNumeric(frame=arg, errors=errors, downcast=downcast))


def to_datetime(arg, **kwargs):
    if not isinstance(arg, FrameBase):
        raise TypeError("arg must be a Series or a DataFrame")
    return new_collection(ToDatetime(frame=arg, kwargs=kwargs))


def to_timedelta(arg, unit=None, errors="raise"):
    if not isinstance(arg, Series):
        raise TypeError("arg must be a Series")
    return new_collection(ToTimedelta(frame=arg, unit=unit, errors=errors))


def _from_scalars(scalars, meta, names):
    return new_collection(FromScalars(meta, names, *scalars))


def map_partitions(
    func,
    *args,
    meta=no_default,
    enforce_metadata=True,
    transform_divisions=True,
    clear_divisions=False,
    align_dataframes=False,
    **kwargs,
):
    if align_dataframes:
        # TODO: Handle alignment?
        # Perhaps we only handle the case that all `Expr` operands
        # have the same number of partitions or can be broadcasted
        # within `MapPartitions`. If so, the `map_partitions` API
        # will need to call `Repartition` on operands that are not
        # aligned with `self.expr`.
        raise NotImplementedError()
    new_expr = expr.MapPartitions(
        args[0],
        func,
        meta,
        enforce_metadata,
        transform_divisions,
        clear_divisions,
        align_dataframes,
        kwargs,
        *args[1:],
    )
    return new_collection(new_expr)


def map_overlap(
    df,
    func,
    before,
    after,
    *args,
    meta=no_default,
    enforce_metadata=True,
    transform_divisions=True,
    clear_divisions=False,
    align_dataframes=False,
    **kwargs,
):
    if align_dataframes:
        raise NotImplementedError()

    if isinstance(before, str):
        before = pd.to_timedelta(before)
    if isinstance(after, str):
        after = pd.to_timedelta(after)

    if isinstance(before, datetime.timedelta) or isinstance(after, datetime.timedelta):
        if isinstance(df, FrameBase):
            inferred_type = df.index._meta_nonempty.inferred_type
        else:
            inferred_type = df.index.inferred_type

        if not is_datetime64_any_dtype(inferred_type):
            raise TypeError(
                "Must have a `DatetimeIndex` when using string offset "
                "for `before` and `after`"
            )

    elif not (
        isinstance(before, Integral)
        and before >= 0
        and isinstance(after, Integral)
        and after >= 0
    ):
        raise ValueError("before and after must be positive integers")

    new_expr = expr.MapOverlap(
        df,
        func,
        before,
        after,
        meta,
        enforce_metadata,
        transform_divisions,
        clear_divisions,
        align_dataframes,
        kwargs,
        *args,
    )
    return new_collection(new_expr)


def isna(arg):
    if isinstance(arg, FrameBase):
        return arg.isna()
    else:
        return from_pandas(arg).isna()

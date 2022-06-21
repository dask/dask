from __future__ import annotations

import operator
import warnings
from dataclasses import replace
from functools import partial
from numbers import Integral
from typing import ClassVar

import numpy as np
import pandas as pd
from tlz import first

from dask import threaded
from dask.base import DaskMethodsMixin, is_dask_collection
from dask.dataframe import methods
from dask.dataframe.core import _extract_meta
from dask.dataframe.dispatch import meta_nonempty
from dask.dataframe.utils import (
    PANDAS_GT_120,
    is_categorical_dtype,
    raise_on_meta_error,
)
from dask.operation.dataframe.core import _FrameOperation, _ScalarOperation
from dask.operation.dataframe.dispatch import get_operation_type
from dask.utils import (
    M,
    OperatorMethodMixin,
    derived_from,
    funcname,
    is_dataframe_like,
    is_index_like,
    is_series_like,
    typename,
)

no_default = "__no_default__"

#
# Operation-Compatible Collection Classes
#


class Scalar(DaskMethodsMixin, OperatorMethodMixin):
    def __init__(self, operation):
        # divisions is ignored, only present to be compatible with other
        # objects.

        if not isinstance(operation, _ScalarOperation):
            raise ValueError(f"Expected _ScalarOperation, got {type(operation)}")

        self._operation = operation
        if (
            is_dataframe_like(self._meta)
            or is_series_like(self._meta)
            or is_index_like(self._meta)
        ):
            raise TypeError(
                f"Expected meta to specify scalar, got {typename(type(self._meta))}"
            )

    @property
    def operation(self):
        return self._operation

    @property
    def dask(self):
        return self.operation.dask

    @property
    def _name(self):
        return self.operation.name

    @property
    def _meta(self):
        return self.operation.meta

    @_meta.setter
    def _meta(self, value):
        self._operation = replace(self.operation, _meta=value)

    @property
    def _parent_meta(self):
        return self.operation.parent_meta

    def __getstate__(self):
        return self.operation

    def __setstate__(self, state):
        self._operation = state

    def __dask_graph__(self):
        return self.dask

    def __dask_keys__(self):
        return self.operation.collection_keys

    def __dask_tokenize__(self):
        return self._name

    def __dask_layers__(self):
        return (self._name,)

    __dask_scheduler__ = staticmethod(threaded.get)

    def __dask_postcompute__(self):
        return first, ()

    @property
    def _meta_nonempty(self):
        return self._meta

    @property
    def dtype(self):
        return self._meta.dtype

    def __array__(self):
        # array interface is required to support pandas instance + Scalar
        # Otherwise, above op results in pd.Series of Scalar (object dtype)
        return np.asarray(self.compute())

    def __bool__(self):
        raise TypeError(
            f"Trying to convert {self} to a boolean value. Because Dask objects are "
            "lazily evaluated, they cannot be converted to a boolean value or used "
            "in boolean conditions like if statements. Try calling .compute() to "
            "force computation prior to converting to a boolean value or using in "
            "a conditional statement."
        )

    @classmethod
    def _get_unary_operator(cls, op):
        return lambda self: elemwise(op, self)

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        if inv:
            return lambda self, other: elemwise(op, other, self)
        else:
            return lambda self, other: elemwise(op, self, other)


class _Frame(DaskMethodsMixin, OperatorMethodMixin):
    def __init__(self, operation):

        if not isinstance(operation, _FrameOperation):
            raise ValueError(f"Expected _FrameOperation, got {type(operation)}")

        self._operation = operation

        if not self._is_partition_type(self._meta):
            raise TypeError(
                f"Expected meta to specify type {type(self).__name__}, got type "
                f"{typename(type(self._meta))}"
            )

    def repartition(
        self,
        divisions=None,
        npartitions=None,
        partition_size=None,
        freq=None,
        force=False,
    ):
        from dask.operation.dataframe.repartition import repartition

        if isinstance(divisions, int):
            npartitions = divisions
            divisions = None
        if isinstance(divisions, str):
            partition_size = divisions
            divisions = None
        if (
            sum(
                [
                    partition_size is not None,
                    divisions is not None,
                    npartitions is not None,
                    freq is not None,
                ]
            )
            != 1
        ):
            raise ValueError(
                "Please provide exactly one of ``npartitions=``, ``freq=``, "
                "``divisions=``, ``partition_size=`` keyword arguments"
            )

        if partition_size is not None:
            raise NotImplementedError
        elif npartitions is not None:
            raise NotImplementedError
        elif divisions is not None:
            return repartition(self, divisions, force=force)
        elif freq is not None:
            raise NotImplementedError

    def shuffle(
        self,
        on,
        npartitions=None,
        max_branch=32,
        shuffle="tasks",
        ignore_index=False,
    ):
        from dask.operation.dataframe.shuffle import ShuffleOnColumns

        # Only support in-memory shuffle for now
        if shuffle != "tasks":
            raise NotImplementedError

        # Only support shuffle on list of column names
        list_like = pd.api.types.is_list_like(on) and not is_dask_collection(on)
        if not (isinstance(on, str) or list_like):
            raise NotImplementedError
        on = [on] if isinstance(on, str) else list(on)
        nset = set(on)
        if not (nset & set(self.columns) == nset):
            raise NotImplementedError

        return new_dd_collection(
            ShuffleOnColumns(
                self.operation,
                on,
                _npartitions=npartitions,
                max_branch=max_branch,
                ignore_index=ignore_index,
            )
        )

    def persist(self, **kwargs):
        raise NotImplementedError

    def compute(self, **kwargs):
        from dask.base import compute
        from dask.operation.dataframe.core import optimize as opt

        new_opt = opt(self.operation)
        new_df = new_dd_collection(new_opt)
        (result,) = compute(new_df, traverse=False, **kwargs)
        return result

    @property
    def operation(self):
        return self._operation

    @property
    def dask(self):
        return self.operation.dask

    @property
    def _name(self):
        return self.operation.name

    @property
    def _meta(self):
        return self.operation.meta

    @_meta.setter
    def _meta(self, value):
        self._operation = self.operation.replace_meta(value)

    @property
    def _divisions(self):
        warnings.warn(
            "_divisions property is deprecated. Use divisions instead.",
            FutureWarning,
        )
        return self.divisions

    @_divisions.setter
    def _divisions(self, value):
        warnings.warn(
            "Settable _divisions property is deprecated. "
            "Set divisions attribute directly instead.",
            FutureWarning,
        )
        self.divisions = value

    @property
    def divisions(self):
        return self.operation.divisions

    @divisions.setter
    def divisions(self, value):
        if not isinstance(value, tuple):
            raise TypeError("divisions must be a tuple")

        if hasattr(self, "divisions") and len(value) != len(self.divisions):
            n = len(self.divisions)
            raise ValueError(
                f"This dataframe has npartitions={n - 1}, divisions should be a "
                f"tuple of length={n}, got {len(value)}"
            )
        if None in value:
            if any(v is not None for v in value):
                raise ValueError(
                    "divisions may not contain a mix of None and non-None values"
                )
        else:
            # Known divisions, check monotonically increasing
            # XXX: if the index dtype is an ordered categorical dtype, then we skip the
            # sortedness check, since the order is dtype dependent
            index_dtype = getattr(self._meta, "index", self._meta).dtype
            if not (is_categorical_dtype(index_dtype) and index_dtype.ordered):
                if value != tuple(sorted(value)):
                    raise ValueError("divisions must be sorted")

        self._operation = replace(self.operation, _divisions=value)

    @property
    def known_divisions(self):
        """Whether divisions are already known"""
        return len(self.divisions) > 0 and self.divisions[0] is not None

    @property
    def npartitions(self) -> int:
        """Return number of partitions"""
        return len(self.divisions) - 1

    @property
    def _meta_nonempty(self):
        """A non-empty version of `_meta` with fake data."""
        return meta_nonempty(self._meta)

    def __dask_graph__(self):
        return self.dask

    def __dask_keys__(self):
        return self.operation.collection_keys

    def __dask_layers__(self):
        return (self._name,)

    def __dask_tokenize__(self):
        return self._name

    __dask_scheduler__ = staticmethod(threaded.get)

    def __dask_postcompute__(self):
        return finalize, ()

    def __getstate__(self):
        return self.operation

    def __setstate__(self, state):
        self._operation = state

    def copy(self, deep=False):
        return new_dd_collection(self.operation.copy())

    def __array__(self, dtype=None, **kwargs):
        self._computed = self.compute()
        x = np.array(self._computed)
        return x

    @property
    def index(self):
        from dask.operation.dataframe.core import Elemwise

        operation = Elemwise(
            getattr,
            [self.operation, "index"],
            {},
            _transform_divisions=False,
            _meta=self._meta.index,
        )
        return new_dd_collection(operation)

    @index.setter
    def index(self, value):
        from dask.dataframe import methods
        from dask.operation.dataframe.core import Elemwise

        self._operation = Elemwise(
            methods.assign_index,
            [self.operation, value],
            {},
            _transform_divisions=False,
            _meta=self._meta.index,
            _divisions=value.divisions,
        )

    def head(self, n=5, npartitions=1, compute=True):
        """First n rows of the dataset
        Parameters
        ----------
        n : int, optional
            The number of rows to return. Default is 5.
        npartitions : int, optional
            Elements are only taken from the first ``npartitions``, with a
            default of 1. If there are fewer than ``n`` rows in the first
            ``npartitions`` a warning will be raised and any found rows
            returned. Pass -1 to use all partitions.
        compute : bool, optional
            Whether to compute the result, default is True.
        """
        from dask.operation.dataframe.core import Head

        if npartitions > self.npartitions:
            raise ValueError(
                f"only {self.npartitions} partitions, head received {npartitions}"
            )

        result = new_dd_collection(
            Head(
                self.operation,
                n,
                npartitions,
            )
        )

        if compute:
            result = result.compute()
        return result

    @classmethod
    def _get_unary_operator(cls, op):
        return lambda self: elemwise(op, self)

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        if inv:
            return lambda self, other: elemwise(op, other, self)
        else:
            return lambda self, other: elemwise(op, self, other)


class Series(_Frame):

    _partition_type = pd.Series
    _is_partition_type = staticmethod(is_series_like)
    _token_prefix = "series-"
    _accessors: ClassVar[set[str]] = set()

    @property
    def name(self):
        return self._meta.name

    @name.setter
    def name(self, name):
        from dask.dataframe.core import _rename
        from dask.operation.dataframe.core import Elemwise

        self._operation = Elemwise(
            _rename,
            [self.operation, name],
            {},
            _transform_divisions=False,
        )

    @property
    def dtype(self):
        return self._meta.dtype

    @derived_from(pd.Series)
    def round(self, decimals=0):
        return elemwise(M.round, self, decimals)

    @derived_from(pd.DataFrame)
    def to_timestamp(self, freq=None, how="start", axis=0):
        df = elemwise(M.to_timestamp, self, freq, how, axis)
        df.divisions = tuple(pd.Index(self.divisions).to_timestamp())
        return df

    @derived_from(pd.Series)
    def groupby(self, *args, **kwargs):
        raise NotImplementedError

    @derived_from(pd.Series)
    def mean(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _bind_operator_method(cls, name, op, original=pd.Series):
        """bind operator method like Series.add to this class"""
        from dask.operation.dataframe.core import Elemwise

        def meth(self, other, level=None, fill_value=None, axis=0):
            if level is not None:
                raise NotImplementedError("level must be None")
            axis = self._validate_axis(axis)
            meta = _emulate(op, self, other, axis=axis, fill_value=fill_value)
            operation = Elemwise(
                op,
                [self.operation],
                {
                    "other": other,
                    "axis": axis,
                    "fill_value": fill_value,
                },
                _meta=meta,  # TODO Avoid this
            )
            return new_dd_collection(operation)
            # return map_partitions(
            #     op, self, other, meta=meta, axis=axis, fill_value=fill_value
            # )

        meth.__name__ = name
        setattr(cls, name, derived_from(original)(meth))

    @classmethod
    def _bind_comparison_method(cls, name, comparison, original=pd.Series):
        """bind comparison method like Series.eq to this class"""

        def meth(self, other, level=None, fill_value=None, axis=0):
            if level is not None:
                raise NotImplementedError("level must be None")
            axis = self._validate_axis(axis)
            if fill_value is None:
                return elemwise(comparison, self, other, axis=axis)
            else:
                op = partial(comparison, fill_value=fill_value)
                return elemwise(op, self, other, axis=axis)

        meth.__name__ = name
        setattr(cls, name, derived_from(original)(meth))


class Index(Series):
    _partition_type = pd.Index
    _is_partition_type = staticmethod(is_index_like)
    _token_prefix = "index-"
    _accessors: ClassVar[set[str]] = set()


class DataFrame(_Frame):

    _partition_type = pd.DataFrame
    _is_partition_type = staticmethod(is_dataframe_like)
    _token_prefix = "dataframe-"
    _accessors: ClassVar[set[str]] = set()

    @property
    def columns(self):
        return self._meta.columns

    @columns.setter
    def columns(self, columns):
        from dask.dataframe.core import _rename
        from dask.operation.dataframe.core import Elemwise

        self._operation = Elemwise(
            _rename,
            [self.operation, columns],
            {},
            _transform_divisions=False,
        )

    def __getitem__(self, key):
        from dask.operation.dataframe.selection import ColumnSelection, SeriesSelection

        if np.isscalar(key) or isinstance(key, (tuple, str)):

            if isinstance(self._meta.index, (pd.DatetimeIndex, pd.PeriodIndex)):
                if key not in self._meta.columns:
                    if PANDAS_GT_120:
                        warnings.warn(
                            "Indexing a DataFrame with a datetimelike index using a single "
                            "string to slice the rows, like `frame[string]`, is deprecated "
                            "and will be removed in a future version. Use `frame.loc[string]` "
                            "instead.",
                            FutureWarning,
                        )
                    return self.loc[key]

            return new_dd_collection(ColumnSelection(self.operation, key))

        elif isinstance(key, slice):
            from pandas.api.types import is_float_dtype

            is_integer_slice = any(
                isinstance(i, Integral) for i in (key.start, key.step, key.stop)
            )
            # Slicing with integer labels is always iloc based except for a
            # float indexer for some reason
            if is_integer_slice and not is_float_dtype(self.index.dtype):
                # NOTE: this always fails currently, as iloc is mostly
                # unsupported, but we call it anyway here for future-proofing
                # and error-attribution purposes
                return self.iloc[key]
            else:
                return self.loc[key]
        if isinstance(key, (np.ndarray, list)) or (
            not is_dask_collection(key) and (is_series_like(key) or is_index_like(key))
        ):
            return new_dd_collection(ColumnSelection(self.operation, key))
        if isinstance(key, Series):
            # do not perform dummy calculation, as columns will not be changed.
            if self.divisions != key.divisions:
                from dask.dataframe.multi import _maybe_align_partitions

                self, key = _maybe_align_partitions([self, key])
                if not hasattr(self, "operation"):
                    return self[key]
            return new_dd_collection(SeriesSelection(self.operation, key.operation))
        if isinstance(key, DataFrame):
            return self.where(key, np.nan)

        raise NotImplementedError(key)

    def __setitem__(self, key, value):
        if isinstance(key, (tuple, list)) and isinstance(value, DataFrame):
            df = self.assign(**{k: value[c] for k, c in zip(key, value.columns)})
        elif isinstance(key, pd.Index) and not isinstance(value, DataFrame):
            key = list(key)
            df = self.assign(**{k: value for k in key})
        elif (
            is_dataframe_like(key)
            or is_series_like(key)
            or isinstance(key, (DataFrame, Series))
        ):
            df = self.where(~key, value)
        elif not isinstance(key, str):
            raise NotImplementedError(f"Item assignment with {type(key)} not supported")
        else:
            df = self.assign(**{key: value})

        self._operation = df.operation

    def __setattr__(self, key, value):
        if key == "_operation":
            object.__setattr__(self, key, value)

        try:
            columns = object.__getattribute__(self, "_meta").columns
        except AttributeError:
            columns = ()

        # exclude protected attributes from setitem
        if key in columns and key not in [
            "operation",
            "divisions",
            "dask",
            "_name",
            "_meta",
        ]:
            self[key] = value
        else:
            object.__setattr__(self, key, value)

    @derived_from(pd.DataFrame)
    def assign(self, **kwargs):

        data = self.copy()
        for k, v in kwargs.items():
            if not (
                isinstance(v, Scalar)
                or is_series_like(v)
                or callable(v)
                or pd.api.types.is_scalar(v)
                or is_index_like(v)
            ):
                raise TypeError(
                    f"Column assignment doesn't support type {typename(type(v))}"
                )
            if callable(v):
                kwargs[k] = v(data)

            pairs = [k, kwargs[k]]

            # Figure out columns of the output
            df2 = data._meta_nonempty.assign(
                **_extract_meta({k: kwargs[k]}, nonempty=True)
            )
            data = elemwise(methods.assign, data, *pairs, meta=df2)

        return data

    @derived_from(pd.DataFrame)
    def to_timestamp(self, freq=None, how="start", axis=0):
        df = elemwise(M.to_timestamp, self, freq, how, axis)
        df.divisions = tuple(pd.Index(self.divisions).to_timestamp())
        return df

    @derived_from(pd.DataFrame)
    def groupby(self, *args, **kwargs):
        raise NotImplementedError

    @derived_from(pd.DataFrame)
    def mean(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _validate_axis(cls, axis=0):
        if axis not in (0, 1, "index", "columns", None):
            raise ValueError(f"No axis named {axis}")
        # convert to numeric axis
        return {None: 0, "index": 0, "columns": 1}.get(axis, axis)

    @classmethod
    def _bind_operator_method(cls, name, op, original=pd.DataFrame):
        """bind operator method like DataFrame.add to this class"""
        from dask.operation.dataframe.core import Elemwise

        # name must be explicitly passed for div method whose name is truediv

        def meth(self, other, axis="columns", level=None, fill_value=None):
            if level is not None:
                raise NotImplementedError("level must be None")

            axis = self._validate_axis(axis)

            if axis in (1, "columns"):
                # When axis=1 and other is a series, `other` is transposed
                # and the operator is applied broadcast across rows. This
                # isn't supported with dd.Series.
                if isinstance(other, Series):
                    msg = f"Unable to {name} dd.Series with axis=1"
                    raise ValueError(msg)
                elif is_series_like(other):
                    # Special case for pd.Series to avoid unwanted partitioning
                    # of other. We pass it in as a kwarg to prevent this.
                    meta = _emulate(
                        op, self, other=other, axis=axis, fill_value=fill_value
                    )
                    operation = Elemwise(
                        op,
                        [
                            self.operation,
                            other.operation
                            if isinstance(other, _FrameOperation)
                            else other,
                        ],
                        {
                            "axis": axis,
                            "fill_value": fill_value,
                        },
                        _meta=meta,  # TODO Avoid this
                    )
                    return new_dd_collection(operation)
                    # return map_partitions(
                    #     op,
                    #     self,
                    #     other=other,
                    #     meta=meta,
                    #     axis=axis,
                    #     fill_value=fill_value,
                    #     enforce_metadata=False,
                    # )

            meta = _emulate(op, self, other, axis=axis, fill_value=fill_value)
            operation = Elemwise(
                op,
                [
                    self.operation,
                    other.operation if isinstance(other, _FrameOperation) else other,
                ],
                {
                    "axis": axis,
                    "fill_value": fill_value,
                },
                _meta=meta,  # TODO Avoid this
            )
            return new_dd_collection(operation)
            # return map_partitions(
            #     op,
            #     self,
            #     other,
            #     meta=meta,
            #     axis=axis,
            #     fill_value=fill_value,
            #     enforce_metadata=False,
            # )

        meth.__name__ = name
        setattr(cls, name, derived_from(original)(meth))

    @classmethod
    def _bind_comparison_method(cls, name, comparison, original=pd.DataFrame):
        """bind comparison method like DataFrame.eq to this class"""

        def meth(self, other, axis="columns", level=None):
            if level is not None:
                raise NotImplementedError("level must be None")
            axis = self._validate_axis(axis)
            return elemwise(comparison, self, other, axis=axis)

        meth.__name__ = name
        setattr(cls, name, derived_from(original)(meth))

    @derived_from(pd.DataFrame)
    def applymap(self, func, meta=no_default):
        return elemwise(M.applymap, self, func, meta=meta)

    @derived_from(pd.DataFrame)
    def round(self, decimals=0):
        return elemwise(M.round, self, decimals)


# bind operators
# TODO: dynamically bound operators are defeating type annotations
for op in [
    operator.abs,
    operator.add,
    operator.and_,
    operator.eq,
    operator.gt,
    operator.ge,
    operator.inv,
    operator.lt,
    operator.le,
    operator.mod,
    operator.mul,
    operator.ne,
    operator.neg,
    operator.or_,
    operator.pow,
    operator.sub,
    operator.truediv,
    operator.floordiv,
    operator.xor,
]:
    _Frame._bind_operator(op)
    Scalar._bind_operator(op)

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
    meth = getattr(pd.DataFrame, name)
    DataFrame._bind_operator_method(name, meth)

    meth = getattr(pd.Series, name)
    Series._bind_operator_method(name, meth)

for name in ["lt", "gt", "le", "ge", "ne", "eq"]:
    meth = getattr(pd.DataFrame, name)
    DataFrame._bind_comparison_method(name, meth)

    meth = getattr(pd.Series, name)
    Series._bind_comparison_method(name, meth)


def elemwise(op, *args, meta=no_default, out=None, transform_divisions=True, **kwargs):
    from dask.dataframe.core import handle_out
    from dask.operation.dataframe.core import Elemwise

    # TODO: Handle division alignment and broadcasting!
    convert = [
        True
        for arg in args
        if (is_series_like(arg) or is_dataframe_like(arg))
        and not is_dask_collection(arg)
    ]
    if convert:
        raise NotImplementedError("Cannot coerce from Pandas yet")
    dasks = [arg for arg in args if is_dask_collection(arg)]
    if any([not hasattr(x, "operation") for x in dasks]):
        raise NotImplementedError("Cannot intermingle operation and legacy collections")
    dfs = [df for df in dasks if isinstance(df, _Frame)]
    divisions = dfs[0].divisions
    if not all(df.divisions == divisions for df in dfs):
        raise NotImplementedError("Cannot broadcast or align partitions yet")

    # TODO: Handle "Array" input/cleanup

    operation = Elemwise(
        op,
        [x.operation if hasattr(x, "operation") else x for x in args],
        kwargs,
        _transform_divisions=transform_divisions,
        _meta=meta,
    )
    result = new_dd_collection(operation)
    return handle_out(out, result)


def new_dd_collection(operation):
    """Generic constructor for dask.dataframe objects.

    Decides the appropriate output class based on the type of `meta` provided.
    """

    if has_abstract_type(operation.meta):
        return get_operation_type(operation.meta)(operation)
    else:
        from dask.dataframe.core import new_dd_object

        return new_dd_object(
            operation.dask,
            operation.name,
            operation.meta,
            operation.divisions,
        )


def has_abstract_type(x):
    """Does this object have a _Frame equivalent?"""
    return get_operation_type(x) is not Scalar


def _emulate(func, *args, udf=False, **kwargs):
    """
    Apply a function using args / kwargs. If arguments contain dd.DataFrame /
    dd.Series, using internal cache (``_meta``) for calculation
    """
    with raise_on_meta_error(funcname(func), udf=udf):
        return func(*_extract_meta(args, True), **_extract_meta(kwargs, True))


def _concat(args, ignore_index=False):
    if not args:
        return args
    # We filter out empty partitions here because pandas frequently has
    # inconsistent dtypes in results between empty and non-empty frames.
    # Ideally this would be handled locally for each operation, but in practice
    # this seems easier. TODO: don't do this.
    args2 = [i for i in args if len(i)]
    return (
        args[0]
        if not args2
        else methods.concat(args2, uniform=True, ignore_index=ignore_index)
    )


def finalize(results):
    return _concat(results)

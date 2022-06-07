from __future__ import annotations

import operator
import warnings
from dataclasses import replace
from functools import partial
from numbers import Integral, Number

import numpy as np
import pandas as pd

from dask.array.core import Array
from dask.base import is_dask_collection
from dask.dataframe import methods
from dask.dataframe.core import DataFrame as LegacyDataFrame
from dask.dataframe.core import Index as LegacyIndex
from dask.dataframe.core import Scalar as LegacyScalar
from dask.dataframe.core import Series as LegacySeries
from dask.dataframe.core import _extract_meta
from dask.dataframe.core import _Frame as LegacyFrame
from dask.dataframe.utils import PANDAS_GT_120, is_categorical_dtype
from dask.operation.dataframe.core import _FrameOperation, _ScalarOperation
from dask.operation.dataframe.dispatch import get_operation_type
from dask.utils import (
    M,
    OperatorMethodMixin,
    derived_from,
    is_dataframe_like,
    is_index_like,
    is_series_like,
    typename,
)

no_default = "__no_default__"

#
# Operation-Compatible Collection Classes
#


class Scalar(LegacyScalar, OperatorMethodMixin):
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


class _Frame(LegacyFrame):
    def __init__(self, operation):

        if not isinstance(operation, _FrameOperation):
            raise ValueError(f"Expected _FrameOperation, got {type(operation)}")

        self._operation = operation

        if not self._is_partition_type(self._meta):
            raise TypeError(
                f"Expected meta to specify type {type(self).__name__}, got type "
                f"{typename(type(self._meta))}"
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

    def __getstate__(self):
        return self.operation

    def __setstate__(self, state):
        self._operation = state

    def copy(self, deep=False):
        return new_dd_collection(self.operation.copy())

    @property
    def index(self):
        return self.map_partitions(
            getattr,
            "index",
            token=self._name + "-index",
            meta=self._meta.index,
            enforce_metadata=False,
        )

    @index.setter
    def index(self, value):
        from dask.dataframe import methods

        self.divisions = value.divisions
        result = self.map_partitions(
            methods.assign_index, value, enforce_metadata=False
        )
        self._operation = result.operation

    def _head(self, n, npartitions, compute, safe):
        from dask.operation.dataframe.core import Head

        if npartitions <= -1:
            npartitions = self.npartitions
        if npartitions > self.npartitions:
            raise ValueError(
                f"only {self.npartitions} partitions, head received {npartitions}"
            )

        result = new_dd_collection(
            Head(
                self.operation,
                n,
                npartitions,
                safe,
            )
        )

        if compute:
            result = result.compute()
        return result

    def repartition(
        self,
        divisions=None,
        npartitions=None,
        partition_size=None,
        freq=None,
        force=False,
    ):
        from dask.dataframe.operation.repartition import repartition, repartition_freq

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
            raise ValueError("Not supported yet")
            # return repartition_size(self, partition_size)
        elif npartitions is not None:
            raise ValueError("Not supported yet")
            # return repartition_npartitions(self, npartitions)
        elif divisions is not None:
            return repartition(self, divisions, force=force)
        elif freq is not None:
            return repartition_freq(self, freq=freq)

    def map_partitions(self, func, *args, **kwargs):
        return map_partitions(func, self, *args, **kwargs)

    def __array_ufunc__(self, numpy_ufunc, method, *inputs, **kwargs):
        out = kwargs.get("out", ())
        for x in inputs + out:
            # ufuncs work with 0-dimensional NumPy ndarrays
            # so we don't want to raise NotImplemented
            if isinstance(x, np.ndarray) and x.shape == ():
                continue
            elif not isinstance(
                x, (Number, Scalar, _Frame, Array, pd.DataFrame, pd.Series, pd.Index)
            ):
                return NotImplemented

        if method == "__call__":
            if numpy_ufunc.signature is not None:
                return NotImplemented
            if numpy_ufunc.nout > 1:
                # ufuncs with multiple output values
                # are not yet supported for frames
                return NotImplemented
            else:
                return elemwise(numpy_ufunc, *inputs, **kwargs)
        else:
            # ufunc methods are not yet supported for frames
            return NotImplemented

    @property
    def _elemwise(self):
        return elemwise

    @classmethod
    def _get_unary_operator(cls, op):
        return lambda self: elemwise(op, self)

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        if inv:
            return lambda self, other: elemwise(op, other, self)
        else:
            return lambda self, other: elemwise(op, self, other)


class Series(_Frame, LegacySeries):
    @property
    def name(self):
        return self._meta.name

    @name.setter
    def name(self, name):
        from dask.dataframe.core import _rename_dask

        self._meta.name = name
        renamed = _rename_dask(self, name)
        # update myself
        self._operation = renamed.operation

    def rename(self, index=None, inplace=False, sorted_index=False):
        from pandas.api.types import is_dict_like, is_list_like, is_scalar

        import dask.dataframe as dd

        if is_scalar(index) or (
            is_list_like(index)
            and not is_dict_like(index)
            and not isinstance(index, dd.Series)
        ):
            if inplace:
                warnings.warn(
                    "'inplace' argument for dask series will be removed in future versions",
                    PendingDeprecationWarning,
                )
            res = self if inplace else self.copy()
            res.name = index
        else:
            res = self.map_partitions(M.rename, index, enforce_metadata=False)
            if self.known_divisions:
                if sorted_index and (callable(index) or is_dict_like(index)):
                    old = pd.Series(range(self.npartitions + 1), index=self.divisions)
                    new = old.rename(index).index
                    if not new.is_monotonic_increasing:
                        msg = (
                            "sorted_index=True, but the transformed index "
                            "isn't monotonic_increasing"
                        )
                        raise ValueError(msg)
                    res.divisions = tuple(dd.methods.tolist(new))
                else:
                    res = res.clear_divisions()
            if inplace:
                self._operation = res.operation
                res = self
        return res

    @derived_from(pd.Series)
    def round(self, decimals=0):
        return elemwise(M.round, self, decimals)

    @derived_from(pd.DataFrame)
    def to_timestamp(self, freq=None, how="start", axis=0):
        df = elemwise(M.to_timestamp, self, freq, how, axis)
        df.divisions = tuple(pd.Index(self.divisions).to_timestamp())
        return df

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


class Index(Series, LegacyIndex):
    pass


class DataFrame(_Frame, LegacyDataFrame):
    @property
    def columns(self):
        return self._meta.columns

    @columns.setter
    def columns(self, columns):
        from dask.dataframe.core import _rename_dask

        renamed = _rename_dask(self, columns)
        self._operation = renamed.operation

    def __getitem__(self, key):
        from dask.operation.dataframe.core import ColumnSelection, SeriesSelection

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

    def __delitem__(self, key):
        result = self.drop([key], axis=1)
        self._operation = result.operation

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
                or isinstance(v, Array)
            ):
                raise TypeError(
                    f"Column assignment doesn't support type {typename(type(v))}"
                )
            if callable(v):
                kwargs[k] = v(data)
            if isinstance(v, Array):
                raise ValueError

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
    def applymap(self, func, meta="__no_default__"):
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
    from dask.dataframe.multi import _maybe_align_partitions
    from dask.operation.dataframe.core import Elemwise

    args = _maybe_from_pandas(args)

    args = _maybe_align_partitions(args)
    dasks = [arg for arg in args if is_dask_collection(arg)]

    if any([not hasattr(x, "operation") for x in dasks]):
        raise ValueError

    # TODO: Test division alignment and handle Array cleanup

    operation = Elemwise(
        op,
        [x.operation if hasattr(x, "operation") else x for x in args],
        kwargs,
        transform_divisions,
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


def _maybe_from_pandas(dfs):
    from dask.dataframe.io import from_pandas

    dfs = [
        from_pandas(df, 1, use_operation_api=True)
        if (is_series_like(df) or is_dataframe_like(df)) and not is_dask_collection(df)
        else df
        for df in dfs
    ]
    return dfs


def map_partitions(
    func,
    *args,
    meta=no_default,
    enforce_metadata=True,
    transform_divisions=True,
    align_dataframes=True,
    **kwargs,
):
    # from dask.array.core import Array
    # from dask.blockwise import BlockwiseDep

    # Use legacy map_partitions if any collection inputs
    # are not supported
    # dasks = [arg for arg in args if is_dask_collection(arg)]
    # dasks = [arg for arg in args if isinstance(arg, (LegacyFrame, Array, BlockwiseDep))]
    if (
        True
    ):  # any([not hasattr(d, "operation") or isinstance(d, BlockwiseDep) for d in dasks]):
        from dask.dataframe.core import map_partitions as mp_legacy

        return mp_legacy(
            func,
            *args,
            meta=meta,
            enforce_metadata=enforce_metadata,
            transform_divisions=transform_divisions,
            align_dataframes=align_dataframes,
            **kwargs,
        )

    # name = kwargs.pop("token", None)
    # parent_meta = kwargs.pop("parent_meta", None)

    # assert callable(func)
    # if name is not None:
    #     token = tokenize(meta, *args, **kwargs)
    # else:
    #     name = funcname(func)
    #     token = tokenize(func, meta, *args, **kwargs)
    # name = f"{name}-{token}"

    # from dask.dataframe.multi import _maybe_align_partitions

    # if align_dataframes:
    #     args = _maybe_from_pandas(args)
    #     try:
    #         args = _maybe_align_partitions(args)
    #     except ValueError as e:
    #         raise ValueError(
    #             f"{e}. If you don't want the partitions to be aligned, and are "
    #             "calling `map_partitions` directly, pass `align_dataframes=False`."
    #         ) from e

    # dfs = [df for df in args if isinstance(df, _Frame)]
    # meta_index = getattr(make_meta(dfs[0]), "index", None) if dfs else None
    # if parent_meta is None and dfs:
    #     parent_meta = dfs[0]._meta

    # if meta is no_default:
    #     # Use non-normalized kwargs here, as we want the real values (not
    #     # delayed values)
    #     meta = _emulate(func, *args, udf=True, **kwargs)
    #     meta_is_emulated = True
    # else:
    #     meta = make_meta(meta, index=meta_index, parent_meta=parent_meta)
    #     meta_is_emulated = False

    # if all(isinstance(arg, Scalar) for arg in args):
    #     layer = {
    #         (name, 0): (
    #             apply,
    #             func,
    #             (tuple, [(arg._name, 0) for arg in args]),
    #             kwargs,
    #         )
    #     }
    #     graph = HighLevelGraph.from_collections(name, layer, dependencies=args)
    #     return Scalar(graph, name, meta)
    # elif not (has_parallel_type(meta) or is_arraylike(meta) and meta.shape):
    #     if not meta_is_emulated:
    #         warnings.warn(
    #             "Meta is not valid, `map_partitions` expects output to be a pandas object. "
    #             "Try passing a pandas object as meta or a dict or tuple representing the "
    #             "(name, dtype) of the columns. In the future the meta you passed will not work.",
    #             FutureWarning,
    #         )
    #     # If `meta` is not a pandas object, the concatenated results will be a
    #     # different type
    #     meta = make_meta(_concat([meta]), index=meta_index)

    # # Ensure meta is empty series
    # meta = make_meta(meta, parent_meta=parent_meta)

    # args2 = []
    # dependencies = []
    # for arg in args:
    #     if isinstance(arg, _Frame):
    #         args2.append(arg)
    #         dependencies.append(arg)
    #         continue
    #     arg = normalize_arg(arg)
    #     arg2, collections = unpack_collections(arg)
    #     if collections:
    #         args2.append(arg2)
    #         dependencies.extend(collections)
    #     else:
    #         args2.append(arg)

    # kwargs3 = {}
    # simple = True
    # for k, v in kwargs.items():
    #     v = normalize_arg(v)
    #     v, collections = unpack_collections(v)
    #     dependencies.extend(collections)
    #     kwargs3[k] = v
    #     if collections:
    #         simple = False

    # if align_dataframes:
    #     divisions = dfs[0].divisions
    # else:
    #     # Unaligned, dfs is a mix of 1 partition and 1+ partition dataframes,
    #     # use longest divisions found
    #     divisions = max((d.divisions for d in dfs), key=len)

    # if transform_divisions and isinstance(dfs[0], Index) and len(dfs) == 1:
    #     try:
    #         divisions = func(
    #             *[pd.Index(a.divisions) if a is dfs[0] else a for a in args], **kwargs
    #         )
    #         if isinstance(divisions, pd.Index):
    #             divisions = methods.tolist(divisions)
    #     except Exception:
    #         pass
    #     else:
    #         if not valid_divisions(divisions):
    #             divisions = [None] * (dfs[0].npartitions + 1)

    # if has_keyword(func, "partition_info"):
    #     partition_info = {
    #         (i,): {"number": i, "division": division}
    #         for i, division in enumerate(divisions[:-1])
    #     }

    #     args2.insert(0, BlockwiseDepDict(partition_info))
    #     orig_func = func

    #     def func(partition_info, *args, **kwargs):
    #         return orig_func(*args, **kwargs, partition_info=partition_info)

    # if enforce_metadata:
    #     dsk = partitionwise_graph(
    #         apply_and_enforce,
    #         name,
    #         *args2,
    #         dependencies=dependencies,
    #         _func=func,
    #         _meta=meta,
    #         **kwargs3,
    #     )
    # else:
    #     kwargs4 = kwargs if simple else kwargs3
    #     dsk = partitionwise_graph(
    #         func, name, *args2, **kwargs4, dependencies=dependencies
    #     )

    # graph = HighLevelGraph.from_collections(name, dsk, dependencies=dependencies)
    # return new_dd_object(graph, name, meta, divisions)

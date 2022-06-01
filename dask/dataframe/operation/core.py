from __future__ import annotations

import operator
import warnings
from dataclasses import dataclass, replace
from functools import cached_property, partial
from numbers import Integral
from typing import Any, Callable, Tuple

import numpy as np
import pandas as pd
from tlz import remove

from dask.base import is_dask_collection, tokenize
from dask.dataframe.core import DataFrame as LegacyDataFrame
from dask.dataframe.core import Index as LegacyIndex
from dask.dataframe.core import Scalar as LegacyScalar
from dask.dataframe.core import Series as LegacySeries
from dask.dataframe.core import _extract_meta
from dask.dataframe.core import _Frame as LegacyFrame
from dask.dataframe.dispatch import get_abstract_type
from dask.dataframe.utils import (
    PANDAS_GT_120,
    is_categorical_dtype,
    raise_on_meta_error,
    valid_divisions,
)
from dask.operation import (
    LiteralInputs,
    _CollectionOperation,
    _FusableOperation,
    fuse_subgraph_callables,
    map_fusion,
)
from dask.optimization import SubgraphCallable
from dask.utils import (
    M,
    OperatorMethodMixin,
    apply,
    funcname,
    is_dataframe_like,
    is_index_like,
    is_series_like,
    partial_by_order,
    typename,
)

no_default = "__no_default__"

PartitionKey = Tuple[str, int]

#
# Abstract DataFrame-Related Collection Classes
#


class _ScalarOperation(_CollectionOperation[PartitionKey]):
    """Abstract Scalar-based _CollectionOperation"""

    # NOTE: This class is not used anywhere yet!

    # Required Attributes
    _meta: Any

    @property
    def meta(self) -> Any:
        """Return Scalar metadata"""
        if self._meta is no_default:
            return self.default_meta
        return self._meta

    @property
    def default_meta(self) -> Any:
        """Return 'Proper' Scalar metadata"""
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[PartitionKey]:
        """Return list of all collection keys"""
        return [(self.name, 0)]


class _FrameOperation(_CollectionOperation[PartitionKey]):
    """Abtract DataFrame-based _CollectionOperation"""

    # Required Attributes
    _meta: Any
    _divisions: tuple | None

    @property
    def meta(self) -> Any:
        """Return DataFrame metadata"""
        if self._meta is no_default:
            return self.default_meta
        return self._meta

    @property
    def default_meta(self) -> Any:
        """Return 'Proper' DataFrame metadata"""
        raise NotImplementedError

    @property
    def divisions(self) -> tuple:
        """Return DataFrame divisions"""
        if self._divisions is None:
            return self.default_divisions
        return self._divisions

    @property
    def default_divisions(self) -> Any:
        """Return 'Proper' DataFrame divisions"""
        raise NotImplementedError

    @property
    def npartitions(self) -> int:
        """Return partition count"""
        return len(self.divisions) - 1

    @cached_property
    def collection_keys(self) -> list[PartitionKey]:
        """Return list of all collection keys"""
        return [(self.name, i) for i in range(self.npartitions)]


class _PartitionwiseOperation(_FusableOperation, _FrameOperation):
    """Abstract _PartitionwiseOperation class"""

    @property
    def func(self):
        raise NotImplementedError

    @property
    def args(self):
        raise NotImplementedError

    @property
    def kwargs(self):
        raise NotImplementedError

    @cached_property
    def dependencies(self) -> frozenset[_CollectionOperation]:
        return frozenset(
            arg for arg in self.args if isinstance(arg, _CollectionOperation)
        )

    @cached_property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[_CollectionOperation]]:
        task = [self.func]
        inkeys = []
        for arg in self.args:
            if isinstance(arg, _CollectionOperation):
                inkeys.append(arg.name)
                task.append(inkeys[-1])
            else:
                task.append(arg)

        subgraph = {
            self.name: (
                apply,
                task[0],
                task[1:],
                self.kwargs,
            )
            if self.kwargs
            else tuple(task)
        }
        return (
            SubgraphCallable(
                dsk=subgraph,
                outkey=self.name,
                inkeys=inkeys,
            ),
            self.dependencies,
        )

    def subgraph(self, keys: list[tuple]) -> tuple[dict, dict]:

        # Check if we have MapInput dependencies to fuse
        dep_subgraphs = {}
        dep_keys = {}
        for dep in self.dependencies:
            dep_name = dep.name
            input_op_keys = [(dep_name,) + tuple(key[1:]) for key in keys]
            if isinstance(dep, LiteralInputs):
                dep_subgraphs.update(dep.subgraph(input_op_keys)[0])
            else:
                dep_keys[dep] = input_op_keys

        # Build subgraph with LiteralInputs dependencies fused
        dsk = {}
        for key in keys:
            task = [self.func]
            for arg in self.args:
                if isinstance(arg, _CollectionOperation):
                    dep_key = (arg.name,) + tuple(key[1:])
                    task.append(dep_subgraphs.get(dep_key, dep_key))
                else:
                    task.append(arg)
            dsk[key] = tuple(task)
        return dsk, dep_keys

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> _PartitionwiseOperation:
        args = [
            replace_dependencies[arg.name]
            if isinstance(arg, _CollectionOperation)
            else arg
            for arg in self.args
        ]
        _changes = {"args": args, **changes}
        return replace(self, **_changes)


class _SimpleFrameOperation(_FrameOperation):
    """Abstract _SimpleFrameOperation class

    Template for operations with a single dependency
    """

    # Required Attributes
    source: _FrameOperation
    _meta: Any
    _divisions: tuple | None

    @property
    def name(self) -> str:
        raise NotImplementedError

    def subgraph(self, keys: list[PartitionKey]) -> tuple[dict, dict]:
        raise NotImplementedError

    @property
    def dependencies(self) -> frozenset[_CollectionOperation]:
        return frozenset({self.source})

    @cached_property
    def default_meta(self) -> Any:
        # By default, assume operation does not
        # change the metadata
        return self.source.meta

    @cached_property
    def default_divisions(self) -> tuple:
        # By default, assume operation does not
        # change the divisions
        return self.source.divisions

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> _SimpleFrameOperation:
        _dependencies = {replace_dependencies[dep.name] for dep in self.dependencies}
        _changes = {"_dependencies": _dependencies, **changes}
        return replace(self, **_changes)

    def copy(self) -> _SimpleFrameOperation:
        return replace(self, _meta=self.meta.copy())


#
# Concrete DataFrame-Related Collection Classes
#


@dataclass(frozen=True)
class Head(_SimpleFrameOperation):

    source: _FrameOperation
    _n: int
    _npartitions: int
    safe: bool
    _meta: Any = no_default
    _divisions: tuple | None = None

    @cached_property
    def name(self) -> str:
        label = f"head-{self._npartitions}-{self._n}"
        token = tokenize(
            self.meta, self.divisions, self.dependencies, self._npartitions, self._n
        )
        return f"{label}-{token}"

    @cached_property
    def default_divisions(self) -> tuple:
        dep = self.source
        return (dep.divisions[0], dep.divisions[self._npartitions])

    def subgraph(self, keys: list[PartitionKey]) -> tuple[dict, dict]:
        from dask.dataframe.core import _concat, safe_head
        from dask.utils import M

        dep = self.source

        head = safe_head if self.safe else M.head
        if self._npartitions > 1:
            name_p = f"head-partial-{self._n}-{dep.name}"

            dsk: dict[tuple, Any] = {}
            dep_keys = []
            for i in range(self._npartitions):
                dep_keys.append((dep.name, i))
                dsk[(name_p, i)] = (M.head, dep_keys[-1], self._n)

            concat = (_concat, [(name_p, i) for i in range(self._npartitions)])
            dsk[(self.name, 0)] = (head, concat, self._n)
        else:
            dep_keys = [(dep.name, 0)]
            dsk = {(self.name, 0): (head, dep_keys[-1], self._n)}

        return dsk, {dep: dep_keys}

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class FrameCreation(_FusableOperation, _FrameOperation):

    io_func: Callable
    inputs: list
    label: str
    _meta: Any
    _divisions: tuple

    @cached_property
    def name(self) -> str:
        token = tokenize(self.io_func, self._meta, self.inputs, self._divisions)
        return f"{self.label}-{token}"

    @cached_property
    def dependencies(self) -> frozenset[_CollectionOperation]:
        _dep = LiteralInputs({(i,): val for i, val in enumerate(self.inputs)})
        return frozenset({_dep})

    @cached_property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[_CollectionOperation]]:
        inkeys = [d.name for d in self.dependencies]
        subgraph = {self.name: (self.io_func, inkeys[-1])}
        return (
            SubgraphCallable(
                dsk=subgraph,
                outkey=self.name,
                inkeys=inkeys,
            ),
            self.dependencies,
        )

    def subgraph(self, keys: list[tuple]) -> tuple[dict, dict]:

        # Always fuse MapInput operations
        input_op = next(iter(self.dependencies))
        input_op_name = input_op.name
        input_op_keys = [(input_op_name,) + tuple(key[1:]) for key in keys]
        dep_subgraph, _ = input_op.subgraph(input_op_keys)

        # Build subgraph with LiteralInputs dependencies fused
        dsk = {}
        for key in keys:
            dep_key = (input_op_name,) + tuple(key[1:])
            dsk[key] = (self.io_func, dep_subgraph.get(dep_key, dep_key))
        return dsk, {}

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> FrameCreation:
        # TODO: Support column projection and predicate pushdown
        return replace(self, **changes)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class Elemwise(_PartitionwiseOperation):

    _func: Callable
    _args: list[Any]
    _kwargs: dict
    _transform_divisions: bool
    _meta: Any = no_default
    _divisions: tuple | None = None

    @cached_property
    def name(self) -> str:
        token = tokenize(self.func, self.args, self.kwargs)
        return f"{funcname(self.func)}-{token}"

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @cached_property
    def default_meta(self) -> Any:
        from dask.dataframe.core import is_broadcastable

        dasks = [
            new_dd_collection(arg)
            for arg in self.args
            if isinstance(arg, (_FrameOperation, _ScalarOperation))
        ]
        dfs = [df for df in dasks if isinstance(df, _Frame)]
        _is_broadcastable = partial(is_broadcastable, dfs)
        dfs = list(remove(_is_broadcastable, dfs))

        other = [
            (i, arg)
            for i, arg in enumerate(self.args)
            if not isinstance(arg, (_FrameOperation, _ScalarOperation))
        ]
        if len(dfs) >= 2 and not all(hasattr(d, "npartitions") for d in dasks):
            # should not occur in current funcs
            msg = "elemwise with 2 or more DataFrames and Scalar is not supported"
            raise NotImplementedError(msg)
        # For broadcastable series, use no rows.
        parts = [d._meta if _is_broadcastable(d) else d._meta_nonempty for d in dasks]
        with raise_on_meta_error(funcname(self.func)):
            meta = partial_by_order(*parts, function=self.func, other=other)
        return meta

    @cached_property
    def default_divisions(self) -> tuple:
        from dask.dataframe import methods

        dasks = [
            new_dd_collection(arg)
            for arg in self.args
            if isinstance(arg, (_FrameOperation, _ScalarOperation))
        ]
        dfs = [df for df in dasks if isinstance(df, _Frame)]

        # TODO: Handle division alignment and Array cleanup

        divisions = dfs[0].divisions
        if self._transform_divisions and isinstance(dfs[0], Index) and len(dfs) == 1:

            try:
                divisions = self.func(
                    *[
                        pd.Index(arg.divisions) if arg is dfs[0] else arg
                        for arg in self.args
                    ],
                    **self.kwargs,
                )
                if isinstance(divisions, pd.Index):
                    divisions = methods.tolist(divisions)
            except Exception:
                pass
            else:
                if not valid_divisions(divisions):
                    divisions = [None] * (dfs[0].npartitions + 1)

        return tuple(divisions)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class ColumnSelection(_PartitionwiseOperation):

    source: Any
    key: str
    _meta: Any = no_default
    _divisions: tuple | None = None

    @cached_property
    def name(self) -> str:
        token = tokenize(self.func, self.args, self.kwargs)
        return f"getitem-columns-{token}"

    @property
    def func(self):
        return operator.getitem

    @property
    def args(self):
        return [self.source, self.key]

    @property
    def kwargs(self):
        return {}

    @cached_property
    def default_meta(self) -> Any:
        return self.source.meta[_extract_meta(self.key)]

    @property
    def default_divisions(self) -> tuple:
        return self.source.divisions

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class SeriesSelection(_PartitionwiseOperation):

    source: Any
    key: str
    _meta: Any = no_default
    _divisions: tuple | None = None

    @cached_property
    def name(self) -> str:
        token = tokenize(self.func, self.args, self.kwargs)
        return f"getitem-series-{token}"

    @property
    def func(self):
        return operator.getitem

    @property
    def args(self):
        return [self.source, self.key]

    @property
    def kwargs(self):
        return {}

    @cached_property
    def default_meta(self) -> Any:
        return self.source.meta

    @property
    def default_divisions(self) -> tuple:
        return self.source.divisions

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class FusedFrameOperations(_FusableOperation, _FrameOperation):
    """FusedFrameOperations class

    A specialized ``_FusableOperation`` class corresponding
    to multiple 'fused' ``_FusableOperation`` objects.
    """

    func: SubgraphCallable
    inkey_mapping: dict[str, str]
    label: str
    _dependencies: frozenset
    _meta: Any
    _divisions: tuple | None

    @classmethod
    def from_operation(
        cls,
        operation: _CollectionOperation,
        fusable: set | bool,
        label: str,
    ):
        # Check inputs
        if not isinstance(operation, _PartitionwiseOperation):
            raise ValueError(
                f"FusedData_FrameOperation.from_operation only supports "
                f"_PartitionwiseOperation. Got {type(operation)}"
            )
        elif not fusable:
            return operation

        # Build fused SubgraphCallable and extract dependencies
        _dependencies = set()
        inkey_mapping = {}
        _subgraph_callable, all_deps = fuse_subgraph_callables(operation, fusable)
        for dep in all_deps:
            key = dep.name
            _dependencies.add(dep)
            inkey_mapping[key] = key

        # Return new FusedOperations object
        return cls(
            _subgraph_callable,
            inkey_mapping,
            label,
            frozenset(_dependencies),
            operation.meta,
            operation.divisions,
        )

    @cached_property
    def name(self) -> str:
        token = tokenize(
            self.func,
            self.inkey_mapping,
            self._dependencies,
            self._meta,
            self._divisions,
        )
        return f"{self.label}-{token}"

    @property
    def collection_keys(self) -> list[PartitionKey]:
        if self.npartitions is None:
            raise ValueError
        return [(self.name, i) for i in range(self.npartitions)]

    @property
    def subgraph_callable(
        self,
    ) -> tuple[SubgraphCallable, frozenset[_CollectionOperation]]:
        return self.func, self.dependencies

    @property
    def dependencies(self):
        return self._dependencies

    def subgraph(self, keys) -> tuple[dict, dict]:
        func, deps = self.subgraph_callable

        # Check if we have MapInput dependencies to fuse
        dep_subgraphs = {}
        dep_keys = {}
        for dep in deps:
            dep_name = dep.name
            input_op_keys = [(dep_name,) + tuple(key[1:]) for key in keys]
            if isinstance(dep, LiteralInputs):
                dep_subgraphs.update(dep.subgraph(input_op_keys)[0])
            else:
                dep_keys[dep] = input_op_keys

        _dependencies_dict = {d.name: d for d in self.dependencies}

        # Build subgraph with LiteralInputs dependencies fused
        dsk = {}
        for key in keys:
            task = [func]
            for inkey in func.inkeys:
                dep_name = self.inkey_mapping[inkey]
                real_dep = _dependencies_dict[dep_name]
                dep_key = (real_dep.name,) + tuple(key[1:])
                task.append(dep_subgraphs.get(dep_key, dep_key))
            dsk[key] = tuple(task)
        return dsk, dep_keys

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> FusedFrameOperations:
        # Update dependencies
        _dependencies = set()
        inkey_mapping = {}
        _dependency_dict = {d.name: d for d in self.dependencies}

        for inkey in self.func.inkeys:
            dep_name = self.inkey_mapping[inkey]
            dep = _dependency_dict[dep_name]
            _dep = replace_dependencies[dep.name]
            _dependencies.add(_dep)
            inkey_mapping[inkey] = _dep.name

        _changes = {
            "inkey_mapping": inkey_mapping,
            "_dependencies": _dependencies,
            **changes,
        }
        return replace(self, **_changes)

    def __hash__(self):
        return hash(self.name)


#
# Optimization
#


def optimize(
    operation,
    fuse_operations=True,
):
    new = operation

    # Apply map fusion
    if fuse_operations:
        new = map_fusion(new, FusedFrameOperations.from_operation)

    # Return new operation
    return new


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


def elemwise(op, *args, meta=no_default, out=None, transform_divisions=True, **kwargs):
    from dask.dataframe.core import handle_out

    # TODO: Handle division alignment and Array cleanup

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
        return get_abstract_type(operation.meta)(operation)
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
    return get_abstract_type(x) is not Scalar

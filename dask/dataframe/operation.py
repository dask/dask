from __future__ import annotations

import itertools
import operator
from collections.abc import Mapping
from functools import cached_property, singledispatch
from typing import Any

import numpy as np

from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.operation import (
    CollectionOperation,
    FusedOperation,
    MapInputs,
    MapOperation,
    MemoizingVisitor,
    fuse_subgraph_callables,
    map_fusion,
    operations,
    regenerate,
)
from dask.optimization import SubgraphCallable
from dask.utils import apply, is_arraylike


class DataFrameOperation(CollectionOperation):
    """Abtract Frame-based CollectionOperation"""

    _meta: Any
    _divisions: tuple | None = None
    _columns: set | None = None

    @property
    def meta(self):
        """Return DataFrame metadata"""
        return self._meta

    @property
    def divisions(self) -> tuple | None:
        """Return DataFrame divisions"""
        return self._divisions

    @property
    def npartitions(self) -> int | None:
        if not self.divisions:
            return None
        return len(self.divisions) - 1

    @property
    def collection_keys(self) -> list[tuple]:
        if self.npartitions is None:
            raise ValueError
        return [(self.name, i) for i in range(self.npartitions)]

    @property
    def columns(self):
        """Set of column names required for this operation

        None means that the required-column set is unknown.
        """
        return self._columns


class FusedDataFrameOperation(FusedOperation, DataFrameOperation):
    def __init__(
        self,
        func: SubgraphCallable,
        inkey_mapping: dict,
        dependencies: dict,
        divisions,
        meta,
        label=None,
    ):
        super().__init__(func, inkey_mapping, dependencies, label=label)
        self._meta = meta
        self._divisions = divisions

    @property
    def collection_keys(self) -> list[tuple]:
        if self.npartitions is None:
            raise ValueError
        return [(self.name, i) for i in range(self.npartitions)]

    @classmethod
    def from_operation(
        cls,
        operation: CollectionOperation,
        fusable: set | bool,
        label: str | None = None,
    ):
        # Check inputs
        if not isinstance(operation, DataFrameMapOperation):
            raise ValueError(
                f"FusedDataFrameOperation.from_operation only supports "
                f"DataFrameMapOperation. Got {type(operation)}"
            )
        elif not fusable:
            return operation

        # Build fused SubgraphCallable and extract dependencies
        _dependencies = {}
        _inkey_mapping = {}
        _subgraph_callable, all_deps = fuse_subgraph_callables(operation, fusable)
        for key in _subgraph_callable.inkeys:
            _dependencies[key] = all_deps[key]
            _inkey_mapping[key] = key

        # Return new FusedOperation object
        return cls(
            _subgraph_callable,
            _inkey_mapping,
            _dependencies,
            operation.divisions,
            operation.meta,
            label=label,
        )

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        # Update dependencies
        _dependencies = {}
        _inkey_mapping = {}

        for inkey in self._subgraph_callable.inkeys:
            dep_name = self._inkey_mapping[inkey]
            dep = self.dependencies[dep_name]
            _dep = new_dependencies[dep.name]
            _dependencies[_dep.name] = _dep
            _inkey_mapping[inkey] = _dep.name

        # Return new object
        kwargs = {
            "meta": self.meta,
            "divisions": self._divisions,
            "label": self._label,
        }
        kwargs.update(new_kwargs)
        return type(self)(
            self._subgraph_callable,
            _inkey_mapping,
            _dependencies,
            **kwargs,
        )

    def copy(self):
        return type(self)(
            self._subgraph_callable,
            self._inkey_mapping,
            self.dependencies,
            self.divisions,
            self.meta,
            label=self._label,
        )


class CompatFrameOperation(DataFrameOperation):
    """Pass-through DataFrameOperation

    This class simply acts as a container for a "legacy"
    collections name, meta, divisions, and graph (HLG).
    """

    def __init__(self, dsk, name=None, meta=None, divisions=None, parent_meta=None):
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(name, dsk, dependencies=[])
        if name is None:
            raise ValueError
        if meta is None:
            raise ValueError
        self._dask = dsk
        self._name = name
        self._meta = meta
        self._parent_meta = parent_meta
        self._divisions = tuple(divisions) if divisions is not None else None

    def copy(self):
        return type(self)(
            self.dask,
            name=self.name,
            meta=self.meta.copy(),
            divisions=self.divisions,
            parent_meta=self._parent_meta,
        )

    @property
    def dask(self) -> HighLevelGraph:
        if self._dask is None:
            raise ValueError
        return self._dask

    def subgraph(self, keys: list[tuple]) -> tuple[dict, dict]:
        if self.dask is None:
            raise ValueError("Graph is undefined")
        return self.dask.to_dict(), {}

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        return {}

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        kwargs = {
            "name": self.name,
            "meta": self.meta,
            "divisions": self._divisions,
            "parent_meta": self._parent_meta,
        }
        kwargs.update(new_kwargs)
        return CompatFrameOperation(self._dask, **kwargs)


class DataFrameCreation(DataFrameOperation, MapOperation):
    def __init__(
        self,
        io_func,
        meta,
        inputs,
        columns=None,
        divisions=None,
        label=None,
        token=None,
        creation_info=None,
    ):
        from dask.dataframe.io.utils import DataFrameIOFunction

        if columns is not None and isinstance(io_func, DataFrameIOFunction):
            self.io_func = io_func.project_columns(list(columns))
            self._meta = meta[columns]
        else:
            self.io_func = io_func
            self._meta = meta
        self._columns = columns
        self.label = label or "create-frame"
        token = token or tokenize(self.io_func, meta, inputs, columns, divisions)
        self._name = f"{self.label}-{token}"
        self.inputs = inputs
        _dep = MapInputs({(i,): val for i, val in enumerate(inputs)})
        self._dependencies = {_dep.name: _dep}
        divisions = divisions or (None,) * (len(inputs) + 1)
        self._divisions = tuple(divisions)
        self.creation_info = creation_info or {}

    def copy(self):
        return type(self)(
            self.io_func,
            self.meta.copy(),
            self.inputs,
            columns=self.columns,
            divisions=self.divisions,
            label=self.label,
            creation_info=self.creation_info,
        )

    @cached_property
    def subgraph_callable(self):
        inkeys = list(self.dependencies)
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
        input_op_name, input_op = next(iter(self.dependencies.items()))
        input_op_keys = [(input_op_name,) + tuple(key[1:]) for key in keys]
        dep_subgraph, _ = input_op.subgraph(input_op_keys)

        # Build subgraph with MapInput dependencies fused
        dsk = {}
        for key in keys:
            dep_key = (input_op_name,) + tuple(key[1:])
            dsk[key] = (self.io_func, dep_subgraph.get(dep_key, dep_key))
        return dsk, {}

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        return self._dependencies

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        if "filters" in new_kwargs:
            if not self.creation_info:
                raise ValueError(
                    "cannot regenerate a DataFrameCreation with new "
                    "filters unless ``creation_info`` is defined."
                )
            kwargs = self.creation_info.get("kwargs", {})
            kwargs.update(new_kwargs)
            return self.creation_info["func"](
                *self.creation_info.get("args", []),
                **kwargs,
            ).operation
        else:
            kwargs = {
                "columns": self.columns,
                "divisions": self.divisions,
                "label": self.label,
            }
            kwargs.update(new_kwargs)
            return type(self)(
                self.io_func,
                self.meta,
                self.inputs,
                **kwargs,
            )


class DataFrameMapOperation(DataFrameOperation, MapOperation):
    def __init__(
        self,
        func,
        meta,
        *args,
        divisions=None,
        label=None,
        token=None,
        columns=None,
        **kwargs,
    ):
        self.label = label or "map-partitions"
        token = token or tokenize(func, meta, args, divisions)
        self._name = f"{self.label}-{token}"
        self._func = func
        self._meta = meta
        assert len(args)
        self.args = args
        self._dependencies = {
            arg.name: arg for arg in self.args if isinstance(arg, CollectionOperation)
        }
        divisions = divisions or (None,) * (
            len(next(iter(self._dependencies.values()))) + 1
        )
        self._divisions = tuple(divisions)
        self._columns = columns
        self.kwargs = kwargs

    @property
    def func(self):
        return self._func

    def copy(self):
        return type(self)(
            self.func,
            self.meta.copy(),
            *self.args,
            columns=self.columns,
            divisions=self.divisions,
            label=self.label,
            **self.kwargs,
        )

    @cached_property
    def subgraph_callable(self):
        task = [self.func]
        inkeys = []
        for arg in self.args:
            if isinstance(arg, CollectionOperation):
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
        for dep_name, dep in self.dependencies.items():
            input_op_keys = [(dep_name,) + tuple(key[1:]) for key in keys]
            if isinstance(dep, MapInputs):
                dep_subgraphs.update(dep.subgraph(input_op_keys)[0])
            else:
                dep_keys[dep] = input_op_keys

        # Build subgraph with MapInputs dependencies fused
        dsk = {}
        for key in keys:
            task = [self.func]
            for arg in self.args:
                if isinstance(arg, CollectionOperation):
                    dep_key = (arg.name,) + tuple(key[1:])
                    task.append(dep_subgraphs.get(dep_key, dep_key))
                else:
                    task.append(arg)
            dsk[key] = tuple(task)
        return dsk, dep_keys

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        return self._dependencies

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        kwargs = {
            "divisions": self.divisions,
            "label": self.label,
            "columns": self.columns,
            **self.kwargs,
        }
        kwargs.update(new_kwargs.get(self.name, {}))
        args = [
            new_dependencies[arg.name] if isinstance(arg, CollectionOperation) else arg
            for arg in self.args
        ]
        return type(self)(
            self.func,
            self.meta,
            *args,
            **kwargs,
        )


class DataFrameColumnSelection(DataFrameMapOperation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        key = self.args[1]
        if np.isscalar(key) or isinstance(key, (tuple, str)):
            self._columns = {key}
        else:
            self._columns = set(key)


class DataFrameSeriesSelection(DataFrameMapOperation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._columns = set()


class DataFrameElementwise(DataFrameMapOperation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._columns = set()


#
# DataFrameOperation-specific optimization pass
#


def optimize(
    operation,
    predicate_pushdown=True,
    column_projection=True,
    fuse_map_operations=True,
):
    if isinstance(operation, CompatFrameOperation):
        return operation
    new = operation

    # Apply predicate pushdown
    if predicate_pushdown:
        new = optimize_predicate_pushdown(new)

    # Apply column projection
    if column_projection:
        new = project_columns(new)

    # Apply map fusion
    if fuse_map_operations:
        new = map_fusion(new, FusedDataFrameOperation)

    # Return new operation
    return new


#
# Column Projection Logic
#


@singledispatch
def _required_columns(operation, visitor):

    required = {}
    for dep_name, dep in operation.dependencies.items():
        required.update(visitor(dep))

    for creation_name in required:
        columns = required[creation_name]
        if columns is None or operation.columns is None:
            # Required columns unknown, cannot project
            columns = None
        else:
            # Required clumns known - Use set union
            columns |= operation.columns
        required[creation_name] = columns

    return required


@_required_columns.register(DataFrameCreation)
def _(operation, visitor):
    return {operation.name: operation.columns}


@_required_columns.register(DataFrameColumnSelection)
def _(operation, visitor):
    required = {}
    for dep_name, dep in operation.dependencies.items():
        required.update(visitor(dep))
    for creation_name in required:
        required[creation_name] = operation.columns
    return required


def _creation_ops(operation):
    all_ops = operations(operation)
    creation_ops = {
        op_name for op_name, op in all_ops.items() if isinstance(op, DataFrameCreation)
    }
    return creation_ops


def project_columns(operation):

    if not _creation_ops(operation):
        return operation

    new_kwargs = {}
    for creation, columns in MemoizingVisitor(_required_columns)(operation).items():
        new_kwargs[creation] = {"columns": columns}

    new = regenerate(operation, operation_kwargs=new_kwargs)
    new.projected_columns = columns
    return new


#
# Predicate Pushdown Logic
#


class Or(frozenset):
    """Helper class for 'OR' expressions"""

    def to_list_tuple(self):
        # NDF "or" is List[List[Tuple]]
        def _maybe_list(val):
            if isinstance(val, tuple) and val and isinstance(val[0], (tuple, list)):
                return list(val)
            return [val]

        return [
            _maybe_list(val.to_list_tuple())
            if hasattr(val, "to_list_tuple")
            else _maybe_list(val)
            for val in self
        ]


class And(frozenset):
    """Helper class for 'AND' expressions"""

    def to_list_tuple(self):
        # NDF "and" is List[Tuple]
        return tuple(
            val.to_list_tuple() if hasattr(val, "to_list_tuple") else val
            for val in self
        )


def to_dnf(expr):
    """Normalize a boolean filter expression to disjunctive normal form (DNF)"""

    # Credit: https://stackoverflow.com/a/58372345
    if not isinstance(expr, (Or, And)):
        result = Or((And((expr,)),))
    elif isinstance(expr, Or):
        result = Or(se for e in expr for se in to_dnf(e))
    elif isinstance(expr, And):
        total = []
        for c in itertools.product(*[to_dnf(e) for e in expr]):
            total.append(And(se for e in c for se in e))
        result = Or(total)
    return result


# Define all supported comparison functions
# (and their mapping to a string expression)
_comparison_symbols = {
    operator.eq: "==",
    operator.ne: "!=",
    operator.lt: "<",
    operator.le: "<=",
    operator.gt: ">",
    operator.ge: ">=",
    np.greater: ">",
    np.greater_equal: ">=",
    np.less: "<",
    np.less_equal: "<=",
    np.equal: "==",
    np.not_equal: "!=",
}

# Define all supported logical functions
_logical_ops = {
    operator.and_,
    operator.or_,
}


def _get_operation_arg(arg, visitor):
    if isinstance(arg, CollectionOperation):
        return visitor(arg)
    return arg


@singledispatch
def _filter_expression(operation, visitor):
    raise ValueError


@_filter_expression.register(DataFrameMapOperation)
def _(operation, visitor):
    op = operation.func
    if isinstance(op, SubgraphCallable):
        op = op.dsk[op.outkey][0]
    if op in _comparison_symbols:
        # Return DNF expression pattern for a simple comparison
        left = _get_operation_arg(operation.args[0], visitor)
        right = _get_operation_arg(operation.args[1], visitor)

        def _inv(symbol: str):
            return {
                ">": "<",
                "<": ">",
                ">=": "<=",
                "<=": ">=",
            }.get(symbol, symbol)

        if is_arraylike(left) and hasattr(left, "item") and left.size == 1:
            left = left.item()
            # Need inverse comparison in read_parquet
            return (right, _inv(_comparison_symbols[op]), left)
        if is_arraylike(right) and hasattr(right, "item") and right.size == 1:
            right = right.item()
        return to_dnf((left, _comparison_symbols[op], right))
    elif op in _logical_ops:
        # Return DNF expression pattern for logical "and" or "or"
        left = _get_operation_arg(operation.args[0], visitor)
        right = _get_operation_arg(operation.args[1], visitor)
        if op == operator.or_:
            return to_dnf(Or([left, right]))
        elif op == operator.and_:
            return to_dnf(And([left, right]))
        else:
            raise ValueError
    else:
        raise ValueError


@_filter_expression.register(DataFrameSeriesSelection)
def _(operation, visitor):
    return _get_operation_arg(operation.args[1], visitor)


@_filter_expression.register(DataFrameColumnSelection)
def _(operation, visitor):
    return _get_operation_arg(operation.args[1], visitor)


def optimize_predicate_pushdown(operation):

    # Predicate pushdown only works for graphs terminating in
    # a DataFrameSeriesSelection operation. Therefore, we must
    # check if the terminating layer is a series selection, or
    # if all operations after a series selection only have a
    # single dependency
    filter_operation = operation
    while True:
        if isinstance(filter_operation, DataFrameSeriesSelection):
            break
        elif len(filter_operation.dependencies) == 1:
            filter_operation = next(iter(filter_operation.dependencies.values()))
        else:
            break
    if not isinstance(filter_operation, DataFrameSeriesSelection):
        return operation

    # Check that there is only one DataFrameCreation operation
    creation_ops = _creation_ops(filter_operation)
    if len(creation_ops) != 1:
        return operation

    # Try to extract a DNF-formatted filter expression
    try:
        filters = MemoizingVisitor(_filter_expression)(filter_operation).to_list_tuple()
    except ValueError:
        # All "expected" failures should raise ValueError
        return operation

    # We now have `filters` defined, and may regenerate the operation
    try:
        new_kwargs = {creation_ops.pop(): {"filters": filters}}
        new = regenerate(operation, operation_kwargs=new_kwargs)
        new.applied_filters = filters
        return new
    except ValueError:
        # This can fail if the creation layer does not have
        # `creation_info` defined
        return operation

from __future__ import annotations

import operator
from dataclasses import dataclass, replace
from functools import cached_property, partial
from typing import Any, Callable, Tuple

import pandas as pd
from tlz import remove

from dask.base import tokenize
from dask.dataframe.core import _extract_meta
from dask.dataframe.utils import raise_on_meta_error, valid_divisions
from dask.operation.core import (
    LiteralInputs,
    _CollectionOperation,
    _FusableOperation,
    fuse_subgraph_callables,
    map_fusion,
)
from dask.optimization import SubgraphCallable
from dask.utils import apply, funcname, partial_by_order

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

        # Check if we have LiteralInputs dependencies to fuse
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
        _changes = {"source": replace_dependencies[self.source.name], **changes}
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
        npartitions = self._npartitions
        if npartitions <= -1:
            npartitions = dep.npartitions
        head = safe_head if npartitions != dep.npartitions else M.head
        if npartitions > 1:
            name_p = f"head-partial-{self._n}-{dep.name}"

            dsk: dict[tuple, Any] = {}
            dep_keys = []
            for i in range(npartitions):
                dep_keys.append((dep.name, i))
                dsk[(name_p, i)] = (M.head, dep_keys[-1], self._n)

            concat = (_concat, [(name_p, i) for i in range(npartitions)])
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
    _transform_divisions: bool = True
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
        from dask.operation.dataframe.collection import _Frame, new_dd_collection

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
        from dask.operation.dataframe.collection import Index, _Frame, new_dd_collection

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

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> _PartitionwiseOperation:
        args = [
            replace_dependencies[arg.name]
            if isinstance(arg, _CollectionOperation)
            else arg
            for arg in self.args
        ]
        _changes = {"_args": args, **changes}
        return replace(self, **_changes)

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

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> ColumnSelection:
        args = [
            replace_dependencies[arg.name]
            if isinstance(arg, _CollectionOperation)
            else arg
            for arg in self.args
        ]
        _changes = {"source": args[0], **changes}
        return replace(self, **_changes)

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

    def reinitialize(
        self, replace_dependencies: dict[str, _CollectionOperation], **changes
    ) -> SeriesSelection:
        args = [
            replace_dependencies[arg.name]
            if isinstance(arg, _CollectionOperation)
            else arg
            for arg in self.args
        ]
        _changes = {"source": args[0], "key": args[1], **changes}
        return replace(self, **_changes)

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

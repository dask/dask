from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from functools import cached_property
from typing import Any

from dask.base import tokenize
from dask.core import reverse_dict
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import SubgraphCallable


class CollectionOperation:
    """CollectionOperation class

    Encapsulates the state and graph-creation logic for
    a Dask collection.
    """

    _name: str
    _dask: HighLevelGraph | None = None  # Compatibility

    @property
    def name(self) -> str:
        """Return the unique name for this CollectionOperation"""
        return self._name

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        """Get CollectionOperation dependencies"""
        raise NotImplementedError

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        """Regenerate this CollectionOperation

        Parameters
        ----------
        new_dependencies : dict
            Dependencies for the new operation
        **new_kwargs : dict
            New kwargs to use when initializing the new operation
        """
        raise NotImplementedError

    def subgraph(self, keys: list[tuple]) -> tuple:
        """Return the subgraph and key dependencies for this operation

        Parameters
        ----------
        keys : list[tuple]
            List of required output keys needed from this collection

        Returns
        -------
        graph : dict
            The subgraph for the current operation
        dependencies : dict[CollectionOperation, list[tuple]]
            A dictionary mapping of ``CollectionOperation`` objects
            to required keys. This dictionary will be used by the
            global graph-generation algorithm to determine which
            operation-key combinations to materialize next.

            Note: Since this dictionary uses ``CollectionOperation``
            objects as keys (rather than str names), the logic in this
            ``subgraph`` method is allowed to recursively fuse with
            the operation's dependencies.
        """
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[tuple]:
        """Get the collection keys for this operation"""
        raise NotImplementedError

    @property
    def dask(self) -> HighLevelGraph:
        """Return a HighLevelGraph representation of this operation

        The primary purpose of this method is compatibility
        with the existing HighLevelGraph/Layer API.
        """
        from dask.layers import CollectionOperationLayer

        if self._dask is None:
            self._dask = HighLevelGraph.from_collections(
                self.name,
                CollectionOperationLayer(self, self.collection_keys),
                dependencies=[],
            )
        return self._dask

    def visualize(self, filename="dask-operation.svg", format=None, **kwargs):
        """Visualize the operation DAG"""
        from dask.dot import graphviz_to_file

        g = to_graphviz(self, **kwargs)
        graphviz_to_file(g, filename, format)
        return g

    def copy(self):
        """Return a shallow copy of this operation"""
        raise NotImplementedError

    def __hash__(self):
        """Hash a CollectionOperation using its unique name"""
        return hash(self.name)


class LiteralInputs(CollectionOperation):
    """LiteralInputs class

    Defines literal block/partition inputs to a CollectionOperation
    """

    def __init__(self, inputs: Mapping, label=None):
        self._inputs = inputs
        self._label = label or "literals"
        self._name = self._label + "-" + tokenize(inputs)

    @property
    def dependencies(self):
        # LiteralInputs can not have dependencies
        return {}

    @property
    def collection_keys(self) -> list[tuple]:
        keys = self._inputs.keys()  # This may not always work
        return [(self.name,) + tuple(key[1:]) for key in keys]

    def subgraph(self, keys):
        dsk: dict[tuple, Any] = {}
        for key in keys:
            index = key[1:]
            dsk[key] = self._inputs[index]
        return dsk, {}

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        # Nothing to "regenerate"
        return self

    def copy(self):
        return type(self)(self._inputs, label=self._label)


class MapOperation(CollectionOperation):
    """MapOperation class

    A block/partition-wise collection operation which can
    be fused with other ``MapOperation`` dependencies.
    """

    @property
    def subgraph_callable(self):
        """Return a SubgraphCallable representation of the
        function being mapped by this operation and its
        dependencies

        Returns
        -------
        func : SubgraphCallable
            The subgraph for the current operation
        dependencies : dict[str, CollectionOperation | Mapping]
            A dictionary mapping the elements of ``func.inkeys``
            to specific ``CollectionOperation`` or Mapping objects.
        """
        raise NotImplementedError


class FusedOperation(MapOperation):
    """FusedOperation class

    A specialized ``MapOperation`` class corresponding
    to the fusion of multiple ``MapOperation`` objects.
    """

    def __init__(
        self,
        func: SubgraphCallable,
        inkey_mapping: dict,
        dependencies: dict,
        label=None,
    ):
        if not isinstance(func, SubgraphCallable):
            raise ValueError(
                f"FusedOperation only supports SubgraphCallable func input "
                f"got {type(func)}: {func}"
            )
        self._subgraph_callable = func
        self._inkey_mapping = inkey_mapping
        self._dependencies = dependencies
        self._label = label or "fused-operation"
        self._name = self._label + "-" + tokenize(func, dependencies)

    @classmethod
    def from_operation(
        cls,
        operation: CollectionOperation,
        fusable: set | bool,
        label: str | None = None,
    ):
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[tuple]:
        raise NotImplementedError

    @cached_property
    def subgraph_callable(self):
        return self._subgraph_callable, self.dependencies

    @property
    def dependencies(self):
        return self._dependencies

    def subgraph(self, keys):
        func, deps = self.subgraph_callable

        # Check if we have MapInput dependencies to fuse
        dep_subgraphs = {}
        dep_keys = {}
        for dep_name, dep in deps.items():
            input_op_keys = [(dep_name,) + tuple(key[1:]) for key in keys]
            if isinstance(dep, LiteralInputs):
                dep_subgraphs.update(dep.subgraph(input_op_keys)[0])
            else:
                dep_keys[dep] = input_op_keys

        # Build subgraph with LiteralInputs dependencies fused
        dsk = {}
        for key in keys:
            task = [func]
            for inkey in func.inkeys:
                dep_name = self._inkey_mapping[inkey]
                real_dep = self.dependencies[dep_name]
                dep_key = (real_dep.name,) + tuple(key[1:])
                task.append(dep_subgraphs.get(dep_key, dep_key))
            dsk[key] = tuple(task)
        return dsk, dep_keys

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError


def to_graphviz(
    operation,
    data_attributes=None,
    function_attributes=None,
    rankdir="BT",
    graph_attr=None,
    node_attr=None,
    edge_attr=None,
    **kwargs,
):
    """Visualize DAG of CollectionOperation objects

    This code was mostly copied from ``dask.highlevelgraph``.
    """
    from dask.dot import graphviz, label, name

    data_attributes = data_attributes or {}
    function_attributes = function_attributes or {}
    graph_attr = graph_attr or {}
    node_attr = node_attr or {}
    edge_attr = edge_attr or {}

    graph_attr["rankdir"] = rankdir
    node_attr["shape"] = "box"
    node_attr["fontname"] = "helvetica"

    graph_attr.update(kwargs)
    g = graphviz.Digraph(
        graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr
    )

    all_ops = operations(operation)
    op_tree = operation_dag(operation)

    n_tasks = {}
    for op_name in op_tree:
        n_tasks[op_name] = len(all_ops[op_name].collection_keys)

    min_tasks = min(n_tasks.values())
    max_tasks = max(n_tasks.values())

    cache = {}

    for op in op_tree:
        op_name = name(op)
        attrs = data_attributes.get(op, {})

        node_label = label(op, cache=cache)
        node_size = (
            20
            if max_tasks == min_tasks
            else int(20 + ((n_tasks[op] - min_tasks) / (max_tasks - min_tasks)) * 20)
        )

        attrs.setdefault("label", str(node_label))
        attrs.setdefault("fontsize", str(node_size))

        g.node(op_name, **attrs)

    for op, deps in op_tree.items():
        op_name = name(op)
        for dep in deps:
            dep_name = name(dep)
            g.edge(dep_name, op_name)

    return g


class MemoizingVisitor:
    """Helper class for memoized graph traversal"""

    def __init__(self, func, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.cache = {}

    def __call__(self, operation, *args):
        key = tokenize(operation, *args)
        try:
            return self.cache[key]
        except KeyError:
            return self.cache.setdefault(
                key,
                self.func(
                    operation,
                    self,
                    *args,
                    **self.kwargs.get(operation.name, {}),
                ),
            )


def _generate_graph(operation, visitor, keys):
    dsk, dependency_keys = operation.subgraph(keys)
    for dep, dep_keys in dependency_keys.items():
        dsk.update(visitor(dep, dep_keys))
    return dsk


def generate_graph(operation, keys=None):
    keys = keys or operation.collection_keys
    return MemoizingVisitor(_generate_graph)(operation, keys)


def _regenerate(operation, visitor, **kwargs):
    transformed_dependencies = {}
    operation = kwargs.pop("replace_with", operation)
    for depname, dep in operation.dependencies.items():
        new_dep = visitor(dep)
        transformed_dependencies[depname] = new_dep
    return operation.regenerate(transformed_dependencies, **kwargs)


def regenerate(operation, operation_kwargs=None):
    visitor = MemoizingVisitor(_regenerate, **(operation_kwargs or {}))
    return visitor(operation)


def _operation_dag(operation, visitor):
    dag = {operation.name: set()}
    for dep_name, dep in operation.dependencies.items():
        dag[operation.name].add(dep_name)
        dag.update(visitor(dep))
    return dag


def operation_dag(operation):
    return MemoizingVisitor(_operation_dag)(operation)


def _operations(operation, visitor):
    ops = {operation.name: operation}
    for dep_name, dep in operation.dependencies.items():
        ops.update(visitor(dep))
    return ops


def operations(operation):
    return MemoizingVisitor(_operations)(operation)


def _fuse_subgraph_callables(operation, visitor, fusable):
    func, deps = operation.subgraph_callable
    all_deps = deps.copy()
    dep_funcs = {}
    for key, dep in deps.items():
        if (fusable is True or key in fusable) and isinstance(dep, MapOperation):
            _func, _deps = visitor(dep, fusable)
            dep_funcs[key] = _func
            all_deps.update(_deps)

    if dep_funcs:
        new_dsk = func.dsk.copy()
        new_inkeys = []
        for key in func.inkeys:
            if key in dep_funcs:
                dep_func = dep_funcs[key]
                new_dsk.update(dep_func.dsk)
                new_inkeys.extend([k for k in dep_func.inkeys if k not in new_inkeys])
            elif key not in new_inkeys:
                new_inkeys.append(key)
        func = SubgraphCallable(
            dsk=new_dsk,
            outkey=operation.name,
            inkeys=new_inkeys,
        )

    return func, all_deps


def fuse_subgraph_callables(operation, fusable=None):
    fusable = fusable or True
    visitor = MemoizingVisitor(_fuse_subgraph_callables)
    return visitor(operation, fusable)


def map_fusion(operation, fused_op_cls):

    all_ops = operations(operation)
    op_dag = operation_dag(operation)
    reverse_dag = reverse_dict(op_dag)

    fusable = defaultdict(set)
    fuse_operation = None

    work = [operation.name]
    while work:

        op_name = work.pop()
        op = all_ops[op_name]
        work.extend(op_dag[op_name])
        dependents = reverse_dag[op_name]

        if isinstance(op, MapOperation):
            map_dependents = {
                dep_name
                for dep_name in dependents
                if isinstance(all_ops[dep_name], MapOperation)
            }
            if not map_dependents:
                fuse_operation = op_name
            elif (
                len(
                    {
                        map_dep
                        for map_dep in map_dependents
                        if map_dep not in fusable[fuse_operation]
                    }
                )
                <= 1
            ):
                fusable[fuse_operation].add(op_name)

    replaced = {}
    for op_name, fusable_set in fusable.items():
        # Define new fused operation
        new_op = fused_op_cls.from_operation(all_ops[op_name], fusable_set)
        replaced[op_name] = {"replace_with": new_op}

    return regenerate(operation, operation_kwargs=replaced)

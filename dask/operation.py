from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from functools import cached_property

from dask.base import tokenize
from dask.core import reverse_dict
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import SubgraphCallable


class CollectionOperation:

    _name: str
    _dask: HighLevelGraph | None = None

    @property
    def name(self) -> str:
        """Return the unique name for this CollectionOperation"""
        return self._name

    @property
    def dask(self) -> HighLevelGraph:
        """Return a HighLevelGraph representation of this operation"""
        from dask.layers import CollectionOperationLayer

        if self._dask is None:
            self._dask = HighLevelGraph.from_collections(
                self.name,
                CollectionOperationLayer(self, self.collection_keys),
                dependencies=[],
            )
        return self._dask

    def subgraph(self, keys: list[tuple]) -> tuple[dict, dict]:
        """Return the subgraph and key dependencies for this operation"""
        raise NotImplementedError

    @property
    def collection_keys(self) -> list[tuple]:
        """Get the collection keys for this operation"""
        raise NotImplementedError

    @property
    def dependencies(self) -> Mapping[str, CollectionOperation]:
        """Get CollectionOperation dependencies"""
        raise NotImplementedError

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        """Regenerate this CollectionOperation with ``new_kwargs``"""
        raise NotImplementedError

    def visualize(self, filename="dask-operation.svg", format=None, **kwargs):
        """Visualize the operation DAG"""
        from dask.dot import graphviz_to_file

        g = to_graphviz(self, **kwargs)
        graphviz_to_file(g, filename, format)
        return g

    def copy(self):
        """Copy this operation"""
        raise NotImplementedError

    def __hash__(self):
        """Hash a CollectionOperation using its unique name"""
        return hash(self.name)


class MapOperation(CollectionOperation):
    @property
    def subgraph_callable(self):
        raise NotImplementedError


class FusedOperation(CollectionOperation):
    def __init__(self, operation, fusable, dependencies=None):
        if not isinstance(operation, MapOperation):
            raise ValueError("FusedOperation only supports MapOperation inputs")
        self.operation = operation
        self.fusable = fusable
        self._dependencies = dependencies
        self._name = "fused-operation" + tokenize(operation, fusable)

    @property
    def collection_keys(self) -> list[tuple]:
        return [(self.name,) + tuple(key[1:]) for key in self.operation.collection_keys]

    @cached_property
    def subgraph_callable(self):
        return fuse_subgraph_callables(self.operation, self.fusable)

    @property
    def dependencies(self):
        if self._dependencies is None:
            deps = {}
            sgc, all_deps = self.subgraph_callable
            for key in sgc.inkeys:
                if isinstance(all_deps[key], CollectionOperation):
                    deps[key] = all_deps[key]
            self._dependencies = deps
        return self._dependencies

    def subgraph(self, keys):
        func, deps = self.subgraph_callable

        # Populate dep_keys
        dep_keys = {}
        for func_key in func.inkeys:
            fused_dep = deps[func_key]
            if isinstance(fused_dep, CollectionOperation):
                dep_keys[fused_dep] = [(func_key,) + tuple(key[1:]) for key in keys]

        # Build the graph
        dsk: dict[tuple, tuple] = {}
        for key in keys:
            _, index = key

            task = [func]
            for arg in func.inkeys:
                fused_dep = deps[func_key]
                if isinstance(fused_dep, CollectionOperation):
                    dep_key = tuple((arg,) + key[1:])
                else:
                    # TODO: Does this only work for DataFrame?
                    # Perhaps we need BlockwiseDep concept here
                    dep_key = fused_dep[index]
                task.append(dep_key)
            dsk[key] = tuple(task)

        return dsk, dep_keys

    def regenerate(self, new_dependencies: dict, **new_kwargs):
        return type(self)(
            self.operation,
            self.fusable,
            new_dependencies,
        )

    def copy(self):
        return type(self)(self.operation, self.fusable, dependencies=self.dependencies)


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
        n_tasks[op_name] = all_ops[op_name].npartitions

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


def _regenerate(operation, visitor, replace_operation=None, **kwargs):
    transformed_dependencies = {}
    operation = replace_operation or operation
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


def map_fusion(operation):

    all_ops = operations(operation)
    op_dag = operation_dag(operation)
    reverse_dag = reverse_dict(op_dag)

    fusable = defaultdict(set)
    fuse_operation = None
    for op_name, dependents in reverse_dag.items():
        op = all_ops[op_name]
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
        new_op = FusedOperation(all_ops[op_name], fusable_set)
        replaced[op_name] = new_op

    new_operation = regenerate(
        replaced.get(operation.name, operation),
        operation_kwargs=replaced,
    )

    # TODO: Define the fused operators, and then regenerate the graph
    import pdb

    pdb.set_trace()
    pass

    return new_operation

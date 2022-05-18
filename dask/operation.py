from __future__ import annotations

from collections.abc import Mapping

from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph


class CollectionOperation:

    _dask: HighLevelGraph | None = None

    @property
    def dask(self) -> HighLevelGraph | None:
        """Return a HighLevelGraph representation of this operation"""
        # TODO: We can wrap the operator in a new Layer type
        # to avoid materialization here once the HLG/Layer
        # serialization moves to Pickle (otherwise it will
        # be too much of a headache to wrap a general
        # CollectionOperation in an HLG Layer)
        if self._dask is None:
            self._dask = HighLevelGraph.from_collections(
                self.name,
                generate_graph(self),
                dependencies=[],
            )
        return self._dask

    def subgraph(self, keys: list[tuple]) -> tuple[dict, dict]:
        """Return the subgraph and key dependencies for this operation"""
        raise NotImplementedError

    @property
    def name(self) -> str:
        """Operation name getter"""
        raise NotImplementedError

    @name.setter
    def name(self, value):
        """Operation name setter"""
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
        return hash(self.name)


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


def generate_graph(operation):
    return MemoizingVisitor(_generate_graph)(operation, operation.collection_keys)


def _regenerate(operation, visitor, **kwargs):
    transformed_dependencies = {}
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

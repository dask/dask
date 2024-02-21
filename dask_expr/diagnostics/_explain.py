from dask.utils import funcname

from dask_expr._core import OptimizerStage
from dask_expr._expr import Expr, optimize_until
from dask_expr._merge import Merge
from dask_expr.io.parquet import ReadParquet

STAGE_LABELS: dict[OptimizerStage, str] = {
    "logical": "Logical Plan",
    "simplified-logical": "Simplified Logical Plan",
    "tuned-logical": "Tuned Logical Plan",
    "physical": "Physical Plan",
    "simplified-physical": "Simplified Physical Plan",
    "fused": "Fused Physical Plan",
}


def explain(expr: Expr, stage: OptimizerStage = "fused", format: str | None = None):
    import graphviz

    if format is None:
        format = "png"

    g = graphviz.Digraph(
        STAGE_LABELS[stage], filename=f"explain-{stage}-{expr._name}", format=format
    )
    g.node_attr.update(shape="record")

    expr = optimize_until(expr, stage)

    seen = set(expr._name)
    stack = [expr]

    while stack:
        node = stack.pop()
        explain_info = _explain_info(node)
        _add_graphviz_node(explain_info, g)
        _add_graphviz_edges(explain_info, g)

        for dep in node.operands:
            if not isinstance(dep, Expr) or dep._name in seen:
                continue
            seen.add(dep._name)
            stack.append(dep)

    g.view()


def _add_graphviz_node(explain_info, graph):
    label = "".join(
        [
            "<{<b>",
            explain_info["label"],
            "</b> | ",
            "<br />".join(
                [f"{key}: {value}" for key, value in explain_info["details"].items()]
            ),
            "}>",
        ]
    )

    graph.node(explain_info["name"], label)


def _add_graphviz_edges(explain_info, graph):
    name = explain_info["name"]
    for _, dep in explain_info["dependencies"]:
        graph.edge(dep, name)


def _explain_info(expr: Expr):
    return {
        "name": expr._name,
        "label": funcname(type(expr)),
        "details": _explain_details(expr),
        "dependencies": _explain_dependencies(expr),
    }


def _explain_details(expr: Expr):
    details = {"npartitions": expr.npartitions}

    if isinstance(expr, Merge):
        details["how"] = expr.how
    elif isinstance(expr, ReadParquet):
        details["path"] = expr.path

    return details


def _explain_dependencies(expr: Expr) -> list[tuple[str, str]]:
    dependencies = []
    for i, operand in enumerate(expr.operands):
        if not isinstance(operand, Expr):
            continue
        param = expr._parameters[i] if i < len(expr._parameters) else ""
        dependencies.append((str(param), operand._name))
    return dependencies

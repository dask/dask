"""Rich-based visualization for DataFrame expressions."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from dask._rich_table import (
    REDUCER_COLOR,
    SOURCE_COLOR,
    ExprTable,
    compute_row_emphasis,
    get_op_display_name,
    get_op_style,
    is_expr,
    walk_expr_with_prefix,
)

if TYPE_CHECKING:
    from dask.dataframe.dask_expr._expr import Expr


def _get_op_color(node) -> str | None:
    """Determine operation color based on class hierarchy and data flow."""
    from dask.dataframe.dask_expr._expr import Expr

    # Sources: no Expr dependencies (data enters here)
    deps = [op for op in node.operands if isinstance(op, Expr)]
    if not deps:
        return SOURCE_COLOR

    # Reducers: aggregations and operations that reduce partitions
    class_name = type(node).__name__
    reducer_names = {
        "TreeReduce",
        "Reduction",
        "Sum",
        "Mean",
        "Count",
        "Min",
        "Max",
        "Std",
        "Var",
        "Size",
        "Len",
    }
    if class_name in reducer_names:
        return REDUCER_COLOR

    return None


# Operations where we prefer showing the _name prefix as the primary name
_USE_LABEL_AS_NAME = frozenset({"Blockwise", "Fused"})


def _is_df_expr(op):
    """Check if operand is a DataFrame Expr for tree walking."""
    return hasattr(op, "npartitions") and is_expr(op)


def _format_columns(node) -> str:
    """Format column names compactly."""
    try:
        cols = list(node.columns)
        if len(cols) == 0:
            return ""
        elif len(cols) <= 3:
            return ", ".join(str(c) for c in cols)
        else:
            return f"{cols[0]}, ... ({len(cols)} cols)"
    except Exception:
        return ""


def _format_dtypes(node) -> str:
    """Format dtypes compactly."""
    try:
        dtypes = node._meta.dtypes
        unique_dtypes = set(str(d) for d in dtypes)
        if len(unique_dtypes) == 1:
            return str(next(iter(unique_dtypes)))
        elif len(unique_dtypes) <= 3:
            return ", ".join(sorted(unique_dtypes))
        else:
            return f"{len(unique_dtypes)} types"
    except Exception:
        try:
            return str(node._meta.dtype)
        except Exception:
            return ""


def expr_table(expr: Expr, color: bool = True) -> ExprTable:
    """
    Display expression tree as a lightweight aligned table.

    Parameters
    ----------
    expr : Expr
        The expression to visualize
    color : bool
        Whether to color-code operations by type

    Returns
    -------
    ExprTable
        A displayable table object (works in Jupyter and terminal)
    """
    from rich.table import Table
    from rich.text import Text

    table = Table(
        show_header=True,
        header_style="dim",
        box=None,
        padding=(0, 2),
        collapse_padding=True,
    )

    # Add columns (no per-column dim style - we use row-level emphasis instead)
    table.add_column("Operation", no_wrap=True)
    table.add_column("Parts", justify="right", no_wrap=True)
    table.add_column("Columns", no_wrap=True)
    table.add_column("Dtypes", justify="right", no_wrap=True)

    # First pass: collect nodes and partition counts for emphasis
    nodes_and_prefixes = list(walk_expr_with_prefix(expr, is_expr_child=_is_df_expr))
    partition_counts = []
    for node, _ in nodes_and_prefixes:
        try:
            partition_counts.append(float(node.npartitions))
        except Exception:
            partition_counts.append(float("nan"))
    row_emphasis = compute_row_emphasis(partition_counts)

    # Walk tree and add rows
    for (node, prefix), nparts, emphasize in zip(
        nodes_and_prefixes, partition_counts, row_emphasis
    ):
        display_name = get_op_display_name(node, _USE_LABEL_AS_NAME)

        if color:
            op_text = Text()
            op_text.append(prefix, style="dim")
            op_text.append(display_name, style=get_op_style(_get_op_color(node)))
        else:
            op_text = f"{prefix}{display_name}"

        # Dim data columns for low-partition operations (operation column stays bright)
        data_style = None if color and emphasize else "dim"

        npartitions_str = "?" if math.isnan(nparts) else str(int(nparts))

        row = [
            op_text,
            Text(npartitions_str, style=data_style),
            Text(_format_columns(node), style=data_style),
            Text(_format_dtypes(node), style=data_style),
        ]
        table.add_row(*row)

    return ExprTable(table)
